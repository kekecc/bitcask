use std::{sync::Arc, thread};

use anyhow::Result;
use crossbeam_channel::unbounded;

use crate::{storage::Bitcask, transaction::KeySlice};

use super::{manager::TxnManager, Transaction};

pub struct TxnEngine {
    storage: Arc<Bitcask>,
    manager: Arc<TxnManager>,
}

impl TxnEngine {
    pub fn new(storage: Bitcask) -> Result<Self> {
        let (tx, rx) = unbounded();

        let manager = TxnManager::new(storage.opts.clone(), tx)?;

        {
            // delete all former txn
            for (ts, keys) in manager.get_uncommitted_txn().drain() {
                for key in keys {
                    storage.delete(KeySlice::new(key, ts).encode())?;
                }
            }
        }

        let storage = Arc::new(storage);
        let manager = Arc::new(manager);

        let storage_ = storage.clone();
        let manager_ = manager.clone();

        thread::spawn(move || {
            while rx.recv().is_ok() {
                for (ts, key) in manager_.pending_clean.lock().drain(..) {
                    if let Err(e) = storage_.delete(KeySlice::new(key, ts).encode()) {
                        log::error!("transaction clean up error!");
                    }
                }
            }
        });

        Ok(Self { storage, manager })
    }

    pub fn close(&self) -> Result<()> {
        self.manager.sync_to_file()?;
        self.storage.close()
    }

    pub fn sync(&self) -> Result<()> {
        self.manager.sync_to_file()?;
        self.storage.sync()
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction::begin(self.storage.clone(), self.manager.clone())
    }
}
