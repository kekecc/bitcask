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
}
