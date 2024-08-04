use std::path::PathBuf;

use anyhow::Result;

pub struct BitcaskOptions {
    pub db_path: PathBuf,
    pub max_file_size: usize,
    pub write_sync: bool,
    pub index_num: u8,
}

pub fn check_options(opts: &BitcaskOptions) -> Result<()> {
    let path = opts.db_path.to_str();
    if path.is_none() || path.unwrap().is_empty() {
        return Err(anyhow::Error::msg(
            "db path should not be empty in options!",
        ));
    }

    if opts.max_file_size == 0 {
        return Err(anyhow::Error::msg(
            "max file size should not be 0 in options!",
        ));
    }

    Ok(())
}

impl Default for BitcaskOptions {
    fn default() -> Self {
        Self {
            db_path: "/tmp/bitcask_tmp".into(),
            max_file_size: 256 << 10,
            write_sync: false,
            index_num: 8,
        }
    }
}

pub struct WriteBatchOptions {
    pub max_batch_size: usize,
    pub write_sync: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_size: 1 << 12,
            write_sync: true,
        }
    }
}
