use std::{collections::HashMap, sync::atomic::Ordering};

use anyhow::Result;
use parking_lot::RwLock;

use crate::{
    data::log_record::Record,
    key::{check_key_valid, Key},
    options::WriteBatchOptions,
    storage::Bitcask,
};

pub struct BatchWrite<'a> {
    pub pending: RwLock<HashMap<Key, Record>>,
    pub storage: &'a Bitcask,
    pub opts: WriteBatchOptions,
}

impl Bitcask {
    pub fn new_batch_write(&self, opts: WriteBatchOptions) -> Result<BatchWrite> {
        Ok(BatchWrite {
            pending: RwLock::new(HashMap::new()),
            storage: self,
            opts,
        })
    }
}

impl<'a> BatchWrite<'a> {
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        check_key_valid(&key)?;

        self.pending
            .write()
            .insert(key.clone(), Record::normal(key, value));

        Ok(())
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref().to_vec();
        check_key_valid(&key)?;

        let mut pending = self.pending.write();
        let index = self.storage.get_index(&key);

        if index.get(&key).is_none() && pending.contains_key(&key) {
            pending.remove(&key);
            return Ok(());
        }

        pending.insert(key.clone(), Record::deleted(key));

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        let pending = std::mem::take(self.pending.get_mut());

        if pending.is_empty() {
            return Ok(());
        }

        if pending.len() > self.opts.max_batch_size {
            return Err(anyhow::Error::msg("batch write: exceed max size!"));
        }

        let _ = self.storage.batch_lock.lock();

        let seq = self.storage.batch_seq.fetch_add(1, Ordering::SeqCst);

        let mut index = Vec::with_capacity(pending.len());

        for (_key, mut record) in pending {
            record.enable_batch(seq)?;

            let pos = self.storage.append_record(&record)?;

            index.push((record, pos));
        }

        self.storage.append_record(&Record::batch_finished(seq))?;

        if self.opts.write_sync {
            self.storage.sync()?;
        }

        index.into_iter().try_for_each(|(record, pos)| {
            let index = self.storage.get_index(&record.key);

            let pos = match record.record_type {
                crate::data::log_record::RecordType::Deleted => Some(index.delete(&record.key)?),
                crate::data::log_record::RecordType::Normal => match index.put(record.key, pos) {
                    Ok(pos) => pos,
                    Err(e) => return Err(e),
                },
            };

            if let Some(pos) = pos {
                self.storage
                    .reclaimable
                    .fetch_add(pos.size as usize, Ordering::SeqCst);
            }

            Ok(())
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        options::{BitcaskOptions, WriteBatchOptions},
        storage::Bitcask,
    };

    #[test]
    fn test_bitcask_write_batch() -> Result<()> {
        {
            let ops = BitcaskOptions::default();

            let bitcask = Bitcask::open(ops)?;

            let batch_ops = WriteBatchOptions::default();

            let mut batch = bitcask.new_batch_write(batch_ops)?;

            for i in 0..1000 {
                for _ in 0..100 {
                    batch.put(format!("{:09}", i), format!("{:09}", i)).unwrap();
                }
            }

            batch.commit()?;

            bitcask.merge()?;
            bitcask.close()?;
        }

        let ops = BitcaskOptions::default();
        let bitcask = Bitcask::open(ops)?;
        for i in 0..1000 {
            let value = bitcask.get(format!("{:09}", i))?;
            assert_eq!(format!("{:09}", i).as_bytes(), value.as_slice());
        }

        Ok(())
    }
}
