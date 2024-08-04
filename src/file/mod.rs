use std::path::Path;

use anyhow::{Ok, Result};
use system_file::SystemFile;

pub mod system_file;
// abstract IO interface
pub trait IO: Send + Sync {
    fn write(&mut self, buf: &[u8], offset: u64) -> Result<u32>;

    fn read(&self, buf: &mut [u8], offset: u64) -> Result<u32>;

    fn sync(&self) -> Result<()>;
}

pub fn new_io(path: impl AsRef<Path>) -> Result<Box<dyn IO>> {
    Ok(Box::new(SystemFile::new(path)?))
}


