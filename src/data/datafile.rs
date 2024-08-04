use std::path::Path;

use anyhow::{Ok, Result};

use crate::{
    consts::{HINT_FILE_NAME, MERGE_FILE_NAME},
    file::{new_io, IO},
    utils::get_data_file_path,
};

use super::log_record::{Record, RecordReader};

pub struct DataFile {
    pub(crate) id: u32,
    pub(crate) write_offset: u64,
    io: Box<dyn IO>,
}

impl DataFile {
    pub fn new(path: impl AsRef<Path>, file_id: u32) -> Result<Self> {
        let file_path = get_data_file_path(path, file_id);

        Ok(Self {
            id: file_id,
            write_offset: 0,
            io: new_io(file_path)?,
        })
    }

    pub fn hint_file(path: impl AsRef<Path>) -> Result<Self> {
        let file_path = path.as_ref().join(HINT_FILE_NAME);

        Ok(Self {
            id: 0,
            write_offset: 0,
            io: new_io(file_path)?,
        })
    }

    pub fn merge_file(path: impl AsRef<Path>) -> Result<Self> {
        let file_path = path.as_ref().join(MERGE_FILE_NAME);

        Ok(Self {
            id: 0,
            write_offset: 0,
            io: new_io(file_path)?,
        })
    }

    pub fn sync(&self) -> Result<()> {
        self.io.sync()
    }

    pub fn padding(&mut self, max: u64) -> Result<()> {
        let len = max - self.write_offset;
        let buf = vec![0; len as usize];
        self.io.write(&buf, self.write_offset)?;
        Ok(())
    }
}

impl DataFile {
    pub fn write_record(&mut self, record: &Record) -> Result<u32> {
        let encode_data = record.encode();
        let write_size = self.io.write(&encode_data, self.write_offset)?;

        self.write_offset += write_size as u64;

        Ok(write_size)
    }

    pub fn read_record(&self, offset: u64) -> Result<RecordReader> {
        RecordReader::decode(self.io.as_ref(), offset)
    }

    pub fn read_record_with_size(&self, offset: u64, size: u64) -> Result<RecordReader> {
        let mut buf = vec![0u8; size as usize];
        self.io.read(&mut buf, offset)?;

        RecordReader::decode_from_vec(buf)
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use super::*;

    #[test]
    fn new() -> Result<()> {
        let temp_dir = temp_dir();

        let data_file1 = DataFile::new(temp_dir.clone(), 0)?;
        assert_eq!(data_file1.id, 0);
        assert_eq!(data_file1.write_offset, 0);

        let data_file2 = DataFile::new(temp_dir.clone(), 0)?;
        assert_eq!(data_file2.id, 0);
        assert_eq!(data_file2.write_offset, 0);

        let data_file3 = DataFile::new(temp_dir.clone(), 666)?;
        assert_eq!(data_file3.id, 666);
        assert_eq!(data_file3.write_offset, 0);

        // test new hint file
        let hint_file = DataFile::hint_file(temp_dir.clone())?;
        assert_eq!(hint_file.id, 0);

        // test new merge finish file
        let merge_finish_file = DataFile::merge_file(temp_dir.clone())?;
        assert_eq!(merge_finish_file.id, 0);

        Ok(())
    }

    #[test]
    fn write() -> Result<()> {
        let temp_dir = temp_dir();
        let mut data_file = DataFile::new(temp_dir.clone(), 0)?;
        assert_eq!(data_file.id, 0);

        let record = Record::normal("foo".into(), "bar".into());

        let encoded_record = record.encode();
        let write_size = data_file.write_record(&record)?;
        assert_eq!(encoded_record.len() as u32, write_size);

        let record = Record::normal("foo".into(), "".into());

        let encoded_record = record.encode();
        let write_size = data_file.write_record(&record)?;
        assert_eq!(encoded_record.len() as u32, write_size);

        let record = Record::deleted("foo".into());

        let encoded_record = record.encode();
        let write_size = data_file.write_record(&record)?;
        assert_eq!(encoded_record.len() as u32, write_size);

        Ok(())
    }

    #[test]
    fn read() -> Result<()> {
        let temp_dir = temp_dir();

        let mut data_file = DataFile::new(temp_dir.clone(), 0)?;
        assert_eq!(data_file.id, 0);

        // type is normal
        let record = Record::normal("foo".into(), "baraaa".into());

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(0)?;

        let mut size = read_record.size();

        assert_eq!(record.key, read_record.key());
        assert_eq!(record.value, read_record.value());
        assert_eq!(record.record_type, read_record.record_type);

        let record = Record::normal("foo".into(), Default::default());

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(size as u64)?;
        size += read_record.size();

        assert_eq!(record.key, read_record.key());
        assert_eq!(record.value, read_record.value());
        assert_eq!(record.record_type, read_record.record_type);

        // type is deleted
        let record = Record::deleted("foo".into());

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(size as u64)?;

        size += read_record.size();
        assert_eq!(record.key, read_record.key());
        assert_eq!(record.value, read_record.value());
        assert_eq!(record.record_type, read_record.record_type);

        // type is Batch
        let mut record = Record::normal("foo".into(), "f".into());
        record.enable_batch(1)?;

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(size as u64)?;
        size += read_record.size();

        assert_eq!(record.key, read_record.key());
        assert_eq!(record.value, read_record.value());
        assert_eq!(record.record_type, read_record.record_type);

        // type is BatchFinish
        let record = Record::batch_finished(2);

        data_file.write_record(&record)?;

        let read_record = data_file.read_record(size as u64)?;
        // size += read_record.size();

        assert_eq!(record.key, read_record.key());
        assert_eq!(record.value, read_record.value());
        assert_eq!(record.record_type, read_record.record_type);
        Ok(())
    }
}
