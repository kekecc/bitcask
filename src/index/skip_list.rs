use anyhow::{Error, Ok};
use crossbeam_skiplist::SkipMap;

use crate::{data::log_record::RecordPosition, key::Key};

use super::Indexer;

#[derive(Default)]
pub struct SkipList {
    pub map: SkipMap<Key, RecordPosition>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
        }
    }
}

impl Indexer for SkipList {
    fn put(&self, key: Key, pos: RecordPosition) -> anyhow::Result<Option<RecordPosition>> {
        if let Some(entry) = self.map.remove(&key) {
            self.map.insert(key, pos);
            Ok(Some(*entry.value()))
        } else {
            self.map.insert(key, pos);
            Ok(None)
        }
    }

    fn get(&self, key: &[u8]) -> Option<RecordPosition> {
        self.map.get(key).map(|entry| *entry.value())
    }

    fn delete(&self, key: &[u8]) -> anyhow::Result<RecordPosition> {
        if let Some(entry) = self.map.remove(key) {
            return Ok(*entry.value());
        }

        Err(Error::msg("key not found!"))
    }

    fn exits(&self, key: &[u8]) -> bool {
        self.map.contains_key(key)
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn txn_prefix_search(
        &self,
        key_prefix: &[u8],
        search_type: crate::transaction::TxnSearchType,
        txn: &crate::transaction::Transaction,
    ) -> anyhow::Result<(RecordPosition, u64)> {
        // todo! use bound to narrow search range
        for entry in self.map.iter().rev() {
            let key = entry.key();

            if key.len() - key_prefix.len() == 8 && key.starts_with(key_prefix) {
                let ts = u64::from_be_bytes(*key.last_chunk::<8>().unwrap());

                if !txn.is_visible(ts) {
                    match search_type {
                        crate::transaction::TxnSearchType::Read => {
                            continue;
                        }
                        crate::transaction::TxnSearchType::Write => {
                            return Err(anyhow::Error::msg("txn conflict!"));
                        }
                    }
                }

                return Ok((*entry.value(), ts));
            }
        }

        Err(anyhow::Error::msg("txn search: key not found!"))
    }
}
