use std::sync::Arc;

use anyhow::Result;
use skip_list::SkipList;

use crate::{
    data::log_record::RecordPosition,
    key::Key,
    transaction::{Transaction, TxnSearchType},
};

pub mod skip_list;

// pub interface for index like skiplist..
pub trait Indexer: Sync + Send {
    fn put(&self, key: Key, pos: RecordPosition) -> Result<Option<RecordPosition>>;

    fn get(&self, key: &[u8]) -> Option<RecordPosition>;

    fn delete(&self, key: &[u8]) -> Result<RecordPosition>;

    fn exits(&self, key: &[u8]) -> bool;

    fn is_empty(&self) -> bool;

    fn txn_prefix_search(
        &self,
        key_prefix: &[u8],
        search_type: TxnSearchType,
        txn: &Transaction,
    ) -> Result<(RecordPosition, u64)>;
}

pub fn new_indexer(num: u8) -> Vec<Arc<dyn Indexer>> {
    let mut indexs: Vec<Arc<dyn Indexer>> = Vec::with_capacity(num as usize);

    for _ in 0..num {
        indexs.push(Arc::new(SkipList::new()));
    }

    indexs
}
