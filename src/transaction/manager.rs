use std::{
    collections::{HashMap, HashSet},
    fs,
    sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering},
};

use anyhow::Result;
use crossbeam_channel::Sender;
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};

use crate::{consts::TXN_FILE, key::Key, options::BitcaskOptions};

pub(crate) struct TxnManager {
    ts: AtomicU64,
    active_txn: Mutex<HashMap<u64, Vec<Key>>>,
    storage_ops: BitcaskOptions,

    pub(crate) pending_clean: Mutex<Vec<(u64, Vec<u8>)>>,
    pub(crate) cleanup_signal: Sender<()>,
}

impl TxnManager {
    pub(crate) fn new(ops: BitcaskOptions, signal: Sender<()>) -> Result<Self> {
        let txn_file_path = &ops.db_path.join(TXN_FILE);

        let manager = match fs::read(&txn_file_path) {
            Ok(buf) => {
                let (active_txn, ts): (HashMap<u64, Vec<Key>>, u64) = bincode::deserialize(&buf)
                    .map_err(|_| anyhow::Error::msg("new txn manager from txn file error!"))?;

                TxnManager {
                    ts: AtomicU64::new(ts),
                    active_txn: Mutex::new(active_txn),
                    storage_ops: ops,
                    pending_clean: Mutex::new(Vec::new()),
                    cleanup_signal: signal,
                }
            }
            Err(_) => {
                fs::File::create(txn_file_path)
                    .map_err(|_| anyhow::Error::msg("new txn file error!"))?;

                TxnManager {
                    ts: AtomicU64::new(0),
                    active_txn: Default::default(),
                    storage_ops: ops,
                    pending_clean: Default::default(),
                    cleanup_signal: signal,
                }
            }
        };

        Ok(manager)
    }

    pub(crate) fn get_uncommitted_txn(&self) -> MappedMutexGuard<HashMap<u64, Vec<Vec<u8>>>> {
        MutexGuard::map(self.active_txn.lock(), |txn| txn)
    }

    pub(crate) fn acquire_next_ts(&self) -> u64 {
        self.ts.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn add_txn(&self, version: u64) -> HashSet<u64> {
        let mut active = self.active_txn.lock();
        let active_txn_id = active.keys().copied().collect();
        active.insert(version, vec![]);

        active_txn_id
    }

    pub(crate) fn remove_txn(&self, version: u64) -> Option<Vec<Vec<u8>>> {
        let mut active = self.active_txn.lock();

        let res = active.remove(&version);

        if active.is_empty() {
            self.cleanup_signal.send(()).unwrap();
        }

        res
    }

    pub(crate) fn update_txn(&self, version: u64, key: &[u8]) {
        self.active_txn
            .lock()
            .entry(version)
            .and_modify(|keys| keys.push(key.to_vec()))
            .or_insert_with(|| vec![key.to_vec()]);
    }

    pub(crate) fn sync_to_file(&self) -> Result<()> {
        let bytes = bincode::serialize(&(&*self.active_txn.lock(), self.ts.load(Ordering::SeqCst)))
            .unwrap();

        let path = self.storage_ops.db_path.join(TXN_FILE);

        std::fs::write(path, bytes)
            .map_err(|_| anyhow::Error::msg("txn manager sync to file error!"))
    }

    pub(crate) fn mark_to_clean(&self, version: u64, key: Vec<u8>) {
        self.pending_clean.lock().push((version, key));
    }
}
