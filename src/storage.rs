use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    path::Path,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::Result;
use fs4::FileExt;
use parking_lot::{Mutex, RwLock};

use crate::{
    consts::{DATA_FILE_SUFFIX, FILE_LOCK},
    data::{
        datafile::DataFile,
        log_record::{Record, RecordPosition, RecordReader, RecordType},
    },
    index::{new_indexer, Indexer},
    key::check_key_valid,
    options::{check_options, BitcaskOptions},
    transaction::{Transaction, TxnSearchType},
    utils::get_merge_path,
};

#[derive(Clone, Copy, Debug)]
pub struct BitcaskState {
    pub data_file_num: u32,
    pub key_num: u32,

    // 可回收空间
    pub reclaimable_size: usize,
    // 已使用磁盘大小
    pub disk_used: usize,
}

pub struct Bitcask {
    pub(crate) opts: BitcaskOptions,

    pub(crate) indexs: Vec<Arc<dyn Indexer>>,
    pub(crate) file_ids: Vec<u32>,

    pub(crate) active_file: RwLock<DataFile>,
    pub(crate) old_files: RwLock<HashMap<u32, DataFile>>,

    pub(crate) batch_lock: Mutex<()>,
    pub(crate) batch_seq: AtomicU64,

    pub(crate) merge_lock: Mutex<()>,

    pub(crate) lock_file: File,
    pub(crate) bytes_written: AtomicUsize,
    pub(crate) reclaimable: AtomicUsize,
}

impl Bitcask {
    pub fn open(opts: BitcaskOptions) -> Result<Self> {
        check_options(&opts)?;

        fs::create_dir_all(&opts.db_path)
            .map_err(|_| anyhow::Error::msg("create db path error!"))?;

        // lock file
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(opts.db_path.join(FILE_LOCK))
            .map_err(|_| anyhow::Error::msg("try to open lock file error!"))?;

        lock_file
            .lock_exclusive()
            .map_err(|_| anyhow::Error::msg("try to lock lock file error!"))?;

        // handle merge path
        Self::load_merge_file(&opts.db_path)?;

        let mut datafile_ids = Self::load_data_file_ids(&opts.db_path)?;

        let active_file = datafile_ids
            .pop()
            .map(|id| DataFile::new(&opts.db_path, id))
            .unwrap_or(DataFile::new(&opts.db_path, 0))
            .unwrap();
        let active_file_id = active_file.id;

        let old_files: Vec<DataFile> = datafile_ids
            .iter()
            .map(|id| DataFile::new(&opts.db_path, *id).unwrap())
            .collect();

        let old_files = datafile_ids.iter().copied().zip(old_files).collect();

        let mut bitcask = Self {
            indexs: new_indexer(opts.index_num),
            file_ids: datafile_ids,
            opts,
            active_file: RwLock::new(active_file),
            old_files: RwLock::new(old_files),
            batch_lock: Mutex::new(()),
            batch_seq: AtomicU64::new(1),
            merge_lock: Mutex::new(()),
            lock_file,
            bytes_written: AtomicUsize::new(0),
            reclaimable: AtomicUsize::new(0),
        };

        bitcask.file_ids.push(active_file_id);

        bitcask.load_index()?;

        Ok(bitcask)
    }

    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();

        check_key_valid(&key.to_vec())?;

        // construct record
        let record = Record::normal(key, value);
        // write into wal
        let pos = self.append_record(&record)?;

        // update mem-index
        let pre_pos = self
            .get_index(&record.key)
            .put(record.key, pos)
            .map_err(|_| anyhow::Error::msg("put: update mem-index error!"))?;

        if let Some(pre_pos) = pre_pos {
            self.reclaimable
                .fetch_add(pre_pos.size as usize, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        let key = key.as_ref().to_vec();
        check_key_valid(&key)?;

        // acquire record
        let pos = self
            .get_index(&key)
            .get(&key)
            .ok_or(anyhow::Error::msg("get: key not found!"))?;
        let record = self.get_record_with_pos(pos)?;

        if let RecordType::Deleted = record.record_type {
            return Err(anyhow::Error::msg("get: key has been deleted!"));
        }

        Ok(record.value().to_vec())
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref().to_vec();
        check_key_valid(&key)?;

        let index = self.get_index(&key);
        if !index.exits(&key) {
            return Ok(());
        }

        let record = Record::deleted(key);
        self.append_record(&record)?;

        let pre_pos = index
            .delete(&record.key)
            .map_err(|_| anyhow::Error::msg("delete: remove key from mem-index error!"))?;

        self.reclaimable
            .fetch_add(pre_pos.size as usize, Ordering::SeqCst);

        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.sync()?;
        self.lock_file.unlock()?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.active_file.read().sync()
    }

    pub fn is_empty(&self) -> bool {
        self.indexs.iter().all(|index| index.is_empty())
    }

    pub(crate) fn get_index(&self, key: &[u8]) -> Arc<dyn Indexer> {
        self.indexs[key[0] as usize % self.opts.index_num as usize].clone()
    }

    #[cfg(test)]
    pub(crate) fn get_size(&self) -> usize {
        self.bytes_written.load(Ordering::SeqCst)
    }
}

impl Bitcask {
    fn load_data_file_ids(path: impl AsRef<Path>) -> Result<Vec<u32>> {
        let dir = fs::read_dir(&path)
            .map_err(|_| anyhow::Error::msg("load datafile ids, open dir error!"))?;

        let datafile_names: Vec<String> = dir
            .filter_map(|f| f.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|filename| filename.ends_with(DATA_FILE_SUFFIX))
            .filter(|filename| {
                filename.starts_with(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'])
            })
            .collect();

        let mut ids = Vec::with_capacity(datafile_names.len());

        for filename in datafile_names {
            let id = filename
                .split_once('.')
                .unwrap()
                .0
                .parse::<u32>()
                .map_err(|_| anyhow::Error::msg("parse filename error!"))?;

            ids.push(id);
        }

        ids.sort_unstable();

        Ok(ids)
    }

    fn load_index(&self) -> Result<()> {
        self.load_index_from_hint_file()?;
        self.load_index_from_datafile()
    }

    fn load_index_from_hint_file(&self) -> Result<()> {
        let hint_file = DataFile::hint_file(&self.opts.db_path)?;

        let mut offset = 0;
        loop {
            match hint_file.read_record(offset) {
                Ok(record) => {
                    self.get_index(record.key()).put(
                        record.key().to_vec(),
                        RecordPosition::decode(record.value()),
                    )?;

                    offset += record.size() as u64;
                }
                Err(_) => break,
            }
        }

        Ok(())
    }

    fn load_index_from_datafile(&self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(());
        }

        let mut merged = false;
        let mut next_file_id = 0;
        let merge_file_path = get_merge_path(&self.opts.db_path);

        if merge_file_path.exists() && merge_file_path.is_file() {
            let merge_file = DataFile::merge_file(&merge_file_path)?;
            let record = merge_file.read_record(0)?;
            let id_bytes = record.value().first_chunk::<4>().unwrap();
            next_file_id = u32::from_be_bytes(*id_bytes);
            merged = true;
        }

        let (active_file_id, old_file_ids) = self.file_ids.split_last().unwrap();
        for id in old_file_ids.iter() {
            if merged && *id < next_file_id {
                continue;
            }

            self.update_index_from_datafile(*id)?;
        }

        self.active_file.write().write_offset =
            self.update_index_from_datafile(*active_file_id).unwrap();

        Ok(())
    }

    fn update_index_from_datafile(&self, file_id: u32) -> Result<u64> {
        let mut offset = 0;
        let mut batch_map = HashMap::<u64, Vec<Record>>::new();
        let mut current_seq = self.batch_seq.load(Ordering::SeqCst);

        loop {
            let (record_len, record) = match self.get_record_with_offset(file_id, offset) {
                Ok(reader) => (reader.size(), reader.to_record()),
                Err(_) => break,
            };

            let pos = RecordPosition::new(file_id, offset, record_len as u32);

            match record.batch_state {
                crate::data::log_record::BatchState::Enable(seq) => {
                    batch_map.entry(seq).or_default().push(record);
                }
                crate::data::log_record::BatchState::Finish(seq) => {
                    batch_map
                        .remove(&seq)
                        .expect("update index, batch write is none!")
                        .into_iter()
                        .try_for_each(|record| self.update_index(&record, pos))?;

                    if current_seq < seq {
                        current_seq = seq;
                    }
                }
                crate::data::log_record::BatchState::Disable => {
                    self.update_index(&record, pos)?;
                }
            }

            offset += record_len as u64;
        }

        self.batch_seq.store(current_seq, Ordering::SeqCst);

        Ok(offset)
    }

    fn update_index(&self, record: &Record, pos: RecordPosition) -> Result<()> {
        let index = self.get_index(&record.key);

        let position = match record.record_type {
            crate::data::log_record::RecordType::Deleted => Some(index.delete(&record.key)?),
            crate::data::log_record::RecordType::Normal => index.put(record.key.clone(), pos)?,
        };

        if let Some(pos) = position {
            self.reclaimable
                .fetch_add(pos.size as usize, Ordering::SeqCst);
        }

        Ok(())
    }

    fn get_record_with_offset(&self, file_id: u32, offset: u64) -> Result<RecordReader> {
        let active_file_id = {
            let active_file = self.active_file.read();
            active_file.id
        };

        if active_file_id == file_id {
            let active_file = self.active_file.read();

            active_file.read_record(offset)
        } else {
            let old_files = self.old_files.read();

            old_files.get(&file_id).unwrap().read_record(offset)
        }
    }

    pub(crate) fn get_record_with_pos(&self, record_pos: RecordPosition) -> Result<RecordReader> {
        let active_file_id = {
            let active_file = self.active_file.read();
            active_file.id
        };

        if active_file_id == record_pos.file_id {
            let active_file = self.active_file.read();
            active_file.read_record(record_pos.offset)
        } else {
            let old_files = self.old_files.read();
            old_files
                .get(&record_pos.file_id)
                .unwrap()
                .read_record(record_pos.offset)
        }
    }

    pub(crate) fn append_record(&self, record: &Record) -> Result<RecordPosition> {
        let record_size = record.get_encode_len();

        let mut active_file = self.active_file.write();
        if active_file.write_offset as usize + record_size > self.opts.max_file_size {
            // sync file
            active_file.sync()?;

            let prev_file_id = active_file.id;
            let pre_active_file = std::mem::replace(
                &mut *active_file,
                DataFile::new(&self.opts.db_path, prev_file_id + 1)?,
            );

            self.old_files.write().insert(prev_file_id, pre_active_file);
        }

        let write_offset = active_file.write_offset;
        let write_size = active_file.write_record(record)?;

        assert_eq!(write_offset + write_size as u64, active_file.write_offset);

        self.bytes_written
            .fetch_add(write_size as usize, Ordering::SeqCst);

        // self.file_ids.push(active_file.id);

        if self.opts.write_sync {
            active_file.sync()?;
        }

        Ok(RecordPosition {
            file_id: active_file.id,
            offset: write_offset,
            size: write_size,
        })
    }
}

impl Bitcask {
    pub(crate) fn txn_search(
        &self,
        key_prefix: impl AsRef<[u8]>,
        search_type: TxnSearchType,
        txn: &Transaction,
    ) -> Result<(RecordPosition, u64)> {
        let key_prefix = key_prefix.as_ref();
        let index = self.get_index(key_prefix);

        index.txn_prefix_search(key_prefix, search_type, txn)
    }

    pub(crate) fn txn_write(&self, record: Record) -> Result<()> {
        let pos = self.append_record(&record)?;

        self.get_index(&record.key)
            .put(record.key, pos)
            .map_err(|_| anyhow::Error::msg("txn write: update index error!"))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, path::Path};

    use anyhow::Result;

    use crate::{options::BitcaskOptions, storage::Bitcask};

    fn clear_directory(path: impl AsRef<Path>) -> Result<()> {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                // 如果是文件，直接删除
                std::fs::remove_file(path)?;
            } else if path.is_dir() {
                // 如果是子目录，递归删除其内部所有文件，但保留子目录
                clear_directory(&path)?;
                // 删除空的子目录
                std::fs::remove_dir(&path)?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_bitcask_put_get_delete() -> Result<()> {
        let ops = BitcaskOptions::default();
        let bitcask = Bitcask::open(ops)?;

        bitcask.put("foo", "ddd").unwrap();
        bitcask.put("ddd", "foo").unwrap();

        let value = bitcask.get("foo").unwrap();
        assert_eq!("ddd".as_bytes(), value.as_slice());

        let value = bitcask.get("ddd").unwrap();
        assert_eq!("foo".as_bytes(), value.as_slice());

        // bitcask.put("", "foo").unwrap();

        bitcask.delete("foo").unwrap();
        println!("{}", bitcask.get_size());

        // bitcask.get("foo").unwrap();

        clear_directory("/tmp/bitcask_tmp")?;
        Ok(())
    }

    #[test]
    fn test_bitcask_merge() -> Result<()> {
        let ops = BitcaskOptions::default();

        let bitcask = Bitcask::open(ops)?;

        for i in 0..1000 {
            for _ in 0..100 {
                bitcask
                    .put(format!("{:09}", i), format!("{:09}", i))
                    .unwrap();
            }
        }

        bitcask.merge()?;
        bitcask.close()?;

        let ops = BitcaskOptions::default();
        let bitcask = Bitcask::open(ops)?;
        for i in 0..1000 {
            let value = bitcask.get(format!("{:09}", i))?;
            assert_eq!(format!("{:09}", i).as_bytes(), value.as_slice());
        }

        Ok(())
    }

    #[test]
    fn t() {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open("/tmp/bitcask_tmp/index.HINT")
            .unwrap();
    }
}
