use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use parking_lot::RwLock;

use crate::{
    consts::{DATA_FILE_SUFFIX, HINT_FILE_NAME, MERGE_FILE_NAME},
    data::{
        datafile::DataFile,
        log_record::{BatchState, Record, RecordPosition},
    },
    storage::Bitcask,
    utils::{get_data_file_path, get_merge_path},
};

pub struct MergeEngine {
    pub(crate) merge_path: PathBuf,
    pub(crate) max_file_size: usize,
    pub(crate) active_file: RwLock<DataFile>,
    pub(crate) old_files: RwLock<HashMap<u32, DataFile>>,
}

impl MergeEngine {
    pub fn new(merge_path: impl AsRef<Path>, max_file_size: usize) -> Result<Self> {
        Ok(Self {
            merge_path: merge_path.as_ref().to_path_buf(),
            max_file_size,
            active_file: RwLock::new(DataFile::new(merge_path, 0)?),
            old_files: Default::default(),
        })
    }

    pub(crate) fn append_record(&self, record: &Record) -> Result<RecordPosition> {
        let path = self.merge_path.as_path();

        let encode_data_len = record.get_encode_len();

        let mut active_file = self.active_file.write();

        if active_file.write_offset as usize + encode_data_len > self.max_file_size {
            active_file.sync()?;

            let current_file_id = active_file.id;
            let current_active_file =
                std::mem::replace(&mut *active_file, DataFile::new(path, current_file_id + 1)?);

            self.old_files
                .write()
                .insert(current_file_id, current_active_file);
        }

        let prev_offset = active_file.write_offset;
        let write_size = active_file.write_record(record)?;

        assert_eq!(prev_offset + write_size as u64, active_file.write_offset);

        return Ok(RecordPosition {
            file_id: active_file.id,
            offset: prev_offset,
            size: write_size,
        });
    }

    pub fn sync(&self) -> Result<()> {
        self.active_file.read().sync()
    }
}

impl Bitcask {
    pub fn merge(&self) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        let _ = self
            .merge_lock
            .try_lock()
            .ok_or(anyhow::Error::msg("bitcask engine is merging!"))?;

        let merge_path = get_merge_path(&self.opts.db_path);
        if merge_path.is_dir() {
            std::fs::remove_dir_all(&merge_path).unwrap();
        }

        std::fs::create_dir_all(&merge_path)
            .map_err(|_| anyhow::Error::msg("create merge dir error!"))?;

        let merge_files = self.get_merge_files()?;
        let merge_engine = MergeEngine::new(&merge_path, self.opts.max_file_size)?;
        let mut hint_file = DataFile::hint_file(&merge_path)?;

        let mut max_file_id = 0;
        for file in merge_files.iter() {
            let mut offset = 0;
            loop {
                let (size, mut record) = match file.read_record(offset) {
                    Ok(reader) => (reader.size(), reader.to_record()),
                    Err(_) => break,
                };

                if let BatchState::Finish(_) = record.batch_state {
                    offset += size as u64;
                    continue;
                }

                if let Some(pos) = self.get_index(&record.key).get(&record.key) {
                    if pos.file_id == file.id && pos.offset == offset {
                        record.disable_batch()?;
                        let merge_pos = merge_engine.append_record(&record)?;
                        hint_file.write_record(&Record::normal(record.key, merge_pos.encode()))?;
                    }
                }

                offset += size as u64;
                max_file_id = max_file_id.max(file.id);
            }
        }

        hint_file.sync()?;
        merge_engine.sync()?;

        let mut merge_file = DataFile::merge_file(&merge_path)?;
        let next_file_id = max_file_id + 1;

        let merge_record = Record::merge_finished(next_file_id);
        merge_file.write_record(&merge_record)?;
        merge_file.sync()?;

        Ok(())
    }

    fn get_merge_files(&self) -> Result<Vec<DataFile>> {
        let mut old_files = self.old_files.write();

        let mut active_file = self.active_file.write();
        active_file.sync()?;
        let prev_file_id = active_file.id;

        let prev_active_file = std::mem::replace(
            &mut *active_file,
            DataFile::new(&self.opts.db_path, prev_file_id + 1)?,
        );

        old_files.insert(prev_file_id, prev_active_file);

        let res = old_files
            .keys()
            .map(|key| DataFile::new(&self.opts.db_path, *key).unwrap())
            .collect();

        Ok(res)
    }

    pub fn load_merge_file(path: impl AsRef<Path>) -> Result<()> {
        let merge_path = get_merge_path(&path);
        if !merge_path.is_dir() {
            return Ok(());
        }

        let files = std::fs::read_dir(&merge_path)
            .map_err(|_| anyhow::Error::msg("read merge dir error!"))?;

        let merge_files: Vec<String> = files
            .filter_map(|f| f.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|filename| {
                filename.ends_with(DATA_FILE_SUFFIX)
                    || filename == MERGE_FILE_NAME
                    || filename == HINT_FILE_NAME
            })
            .collect();

        let mut merge_finished_flag = false;
        merge_files.iter().for_each(|filename| {
            if filename == MERGE_FILE_NAME {
                merge_finished_flag = true;
            }
        });

        if !merge_finished_flag {
            fs::remove_dir_all(&merge_path).unwrap();
            return Ok(());
        }

        let merge_file = DataFile::merge_file(&merge_path)?;
        let merge_record = merge_file.read_record(0)?;
        let bytes = merge_record.value().first_chunk::<4>().unwrap();
        let next_file_id = u32::from_be_bytes(*bytes);

        for id in 0..next_file_id {
            let filename = get_data_file_path(&path, id);
            if filename.is_file() {
                std::fs::remove_file(filename).unwrap();
            }
        }

        for filename in merge_files {
            let src = merge_path.join(&filename);
            let dst = path.as_ref().join(&filename);

            std::fs::rename(src, dst).unwrap();
        }

        std::fs::remove_dir(merge_path).unwrap();

        Ok(())
    }
}
