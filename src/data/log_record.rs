use std::mem::size_of;

use anyhow::Ok;
use bytes::{Buf, BufMut};

use crate::{
    file::IO,
    key::{Key, Value},
};

#[derive(Clone, Copy)]
pub struct RecordPosition {
    pub file_id: u32,
    pub offset: u64,
    pub size: u32,
}

impl RecordPosition {
    pub fn new(file_id: u32, offset: u64, size: u32) -> Self {
        Self {
            file_id,
            offset,
            size,
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(size_of::<Self>());

        data.put_u32(self.file_id);
        data.put_u64(self.offset);
        data.put_u32(self.size);

        data
    }

    pub fn decode(data: &[u8]) -> Self {
        let mut data = &data[..];
        let file_id = data.get_u32();
        let offset = data.get_u64();
        let size = data.get_u32();

        Self {
            file_id,
            offset,
            size,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum RecordType {
    Deleted,
    Normal,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum BatchState {
    Enable(u64),
    Finish(u64),
    Disable,
}

impl TryFrom<u8> for RecordType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Deleted),
            1 => Ok(Self::Normal),
            _ => Err(anyhow::Error::msg("wrong record type!")),
        }
    }
}

pub struct Record {
    // identify which type
    pub record_type: RecordType,
    pub key: Key,
    pub value: Value,

    pub batch_state: BatchState,
}

impl Record {
    pub fn normal(key: Key, value: Value) -> Self {
        Self {
            key,
            value,
            record_type: RecordType::Normal,
            batch_state: BatchState::Disable,
        }
    }

    pub fn deleted(key: Key) -> Self {
        Self {
            key,
            value: Default::default(),
            record_type: RecordType::Deleted,
            batch_state: BatchState::Disable,
        }
    }

    pub fn batch_finished(seq: u64) -> Self {
        Self {
            key: "BF".into(),
            value: Default::default(),
            record_type: RecordType::Normal,
            batch_state: BatchState::Finish(seq),
        }
    }

    pub fn merge_finished(next_unmerged_file_id: u32) -> Self {
        Self::normal("MF".into(), next_unmerged_file_id.to_be_bytes().into())
    }
}

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        let size = self.get_encode_len();
        let mut buf = Vec::with_capacity(size);

        buf.put_u64(size as u64);

        let record_type = match self.record_type {
            RecordType::Deleted => 0 as u8,
            RecordType::Normal => 1 as u8,
        };
        buf.put_u8(record_type);

        match self.batch_state {
            BatchState::Enable(seq) => {
                buf.put_u8(0 as u8);
                buf.put_u64(seq);
            }
            BatchState::Finish(seq) => {
                buf.put_u8(1 as u8);
                buf.put_u64(seq)
            }
            BatchState::Disable => {
                buf.put_u8(2 as u8);
            }
        }

        buf.put_u32(self.key.len() as u32);
        buf.put_u32(self.value.len() as u32);

        buf.put_slice(&self.key);
        buf.put_slice(&self.value);

        let crc32 = get_crc_32(&buf);
        buf.put_u32(crc32);

        buf
    }

    pub fn get_encode_len(&self) -> usize {
        let key_len = self.key.len();
        let value_len = self.value.len();

        let mut res = std::mem::size_of::<u64>() // record_len
            + std::mem::size_of::<u8>() // record_type
            + std::mem::size_of::<u8>() // batch_state
            + std::mem::size_of::<u32>() * 2 // key + value 
            + std::mem::size_of::<u32>() // crc32
            + key_len
            + value_len;

        match self.batch_state {
            BatchState::Enable(_) => res += std::mem::size_of::<u64>(),
            BatchState::Finish(_) => res += std::mem::size_of::<u64>(),
            BatchState::Disable => {}
        }

        res
    }

    pub fn enable_batch(&mut self, seq: u64) -> Result<(), anyhow::Error> {
        self.batch_state = BatchState::Enable(seq);
        Ok(())
    }

    pub fn disable_batch(&mut self) -> Result<(), anyhow::Error> {
        self.batch_state = BatchState::Disable;
        Ok(())
    }
}

pub fn get_crc_32(buf: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();

    hasher.update(buf);
    hasher.finalize()
}

pub struct RecordReader {
    data: Box<[u8]>,
    key_value_start: u32,
    key_size: u32,
    value_size: u32,
    pub(crate) record_type: RecordType,
    pub(crate) batch_state: BatchState,
}

impl RecordReader {
    pub fn decode(io: &dyn IO, offset: u64) -> Result<Self, anyhow::Error> {
        let mut size = [0; 8];
        io.read(&mut size, offset)?;
        let size = u64::from_be_bytes(size);

        if size == 0 {
            return Err(anyhow::Error::msg("read record with size == 0"));
        }

        let mut data = vec![0u8; size as usize];

        io.read(&mut data, offset)?;

        Self::decode_from_vec(data)
    }

    pub fn decode_from_vec(buf: Vec<u8>) -> Result<Self, anyhow::Error> {
        let box_data = buf.clone().into_boxed_slice();
        let mut data = buf.as_slice();

        let mut index = 0;

        let checksum = u32::from_be_bytes(*data.last_chunk::<4>().unwrap());
        let crc32 = get_crc_32(&data[..data.len() - 4]);

        if checksum != crc32 {
            return Err(anyhow::Error::msg("check crc32 error!"));
        }

        // skip size field
        data.get_u64();
        index += 8;

        let record_type = RecordType::try_from(data.get_u8())?;
        index += 1;

        let batch_state = match data.get_u8() {
            0 => {
                let seq = data.get_u64();
                index += 9;
                BatchState::Enable(seq)
            }
            1 => {
                let seq = data.get_u64();
                index += 9;
                BatchState::Finish(seq)
            }
            2 => {
                index += 1;
                BatchState::Disable
            }
            _ => unreachable!(),
        };

        let key_len = data.get_u32() as usize;
        index += 4;
        let value_len = data.get_u32() as usize;
        index += 4;

        Ok(Self {
            data: box_data,
            key_value_start: index,
            key_size: key_len as u32,
            value_size: value_len as u32,
            record_type,
            batch_state,
        })
    }

    pub fn key(&self) -> &[u8] {
        &self.data[self.key_value_start as usize..(self.key_value_start + self.key_size) as usize]
    }

    pub fn value(&self) -> &[u8] {
        &self.data[(self.key_value_start + self.key_size) as usize
            ..(self.key_value_start + self.key_size + self.value_size) as usize]
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
    pub fn to_record(&self) -> Record {
        let key = self.key();
        let value = self.value();

        Record {
            record_type: self.record_type,
            key: key.to_vec(),
            value: value.to_vec(),
            batch_state: self.batch_state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_encode_and_decode() {
        let record = Record {
            record_type: RecordType::Normal,
            key: "cxk".into(),
            value: "kk".into(),
            batch_state: BatchState::Disable,
        };

        let encode_data = record.encode();

        let reader = RecordReader::decode_from_vec(encode_data).unwrap();

        assert_eq!(reader.key(), "cxk".as_bytes());
        assert_eq!(reader.value(), "kk".as_bytes());
    }
}
