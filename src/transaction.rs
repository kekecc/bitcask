pub(crate) mod engine;
pub(crate) mod manager;

use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use manager::TxnManager;

use crate::{
    data::log_record::Record,
    key::{check_key_valid, Key},
    storage::Bitcask,
};

pub(crate) struct KeySlice(Key, u64);

impl KeySlice {
    fn new(key: Key, ts: u64) -> Self {
        Self(key, ts)
    }

    fn encode(mut self) -> Vec<u8> {
        self.0.extend_from_slice(&self.1.to_be_bytes());
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TxnSearchType {
    Read,
    Write,
}

pub struct Transaction {
    storage: Arc<Bitcask>,
    manager: Arc<TxnManager>,

    ts: u64,
    active_txn_ids: HashSet<u64>,
}

impl Transaction {
    pub(crate) fn begin(storage: Arc<Bitcask>, manager: Arc<TxnManager>) -> Self {
        let ts = manager.acquire_next_ts();
        let active_txn_ids = manager.add_txn(ts);

        Self {
            storage,
            manager,
            ts,
            active_txn_ids,
        }
    }

    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();

        check_key_valid(&key)?;
        self.write(Record::normal(key, value))
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref().to_vec();
        check_key_valid(&key)?;

        self.write(Record::deleted(key))
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        let key = key.as_ref().to_vec();
        check_key_valid(&key)?;

        let (pos, _) = self.storage.txn_search(&key, TxnSearchType::Read, self)?;

        let record = self.storage.get_record_with_pos(pos)?;

        match record.record_type {
            crate::data::log_record::RecordType::Deleted => {
                Err(anyhow::Error::msg("key not found!"))
            }
            crate::data::log_record::RecordType::Normal => Ok(record.value().to_vec()),
        }
    }

    pub fn is_visible(&self, ts: u64) -> bool {
        if self.active_txn_ids.contains(&ts) {
            return false;
        }

        ts <= self.ts
    }

    pub fn commit(&self) -> Result<()> {
        self.manager.remove_txn(self.ts);
        self.manager.sync_to_file()?;

        self.storage.sync()
    }

    pub fn rollback(&self) -> Result<()> {
        if let Some(keys) = self.manager.remove_txn(self.ts) {
            for key in keys {
                self.storage.delete(KeySlice::new(key, self.ts).encode())?;
            }
        }

        self.manager.sync_to_file()?;
        self.storage.sync()
    }

    fn write(&self, record: Record) -> Result<()> {
        let key = record.key;
        let value = record.value;

        match self.storage.txn_search(&key, TxnSearchType::Write, self) {
            Ok((_, ts)) => {
                if ts != self.ts {
                    self.manager.mark_to_clean(ts, key.clone())
                }
            }
            Err(e) => return Err(e),
        }

        self.manager.update_txn(self.ts, &key);

        let key_slice = KeySlice::new(key, self.ts);

        let mut write_record = Record::normal(key_slice.encode(), value);
        write_record.record_type = record.record_type;

        self.storage.txn_write(write_record)
    }
}
