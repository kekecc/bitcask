use std::{
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::Path,
};

use super::IO;

pub struct SystemFile {
    fd: File,
}

impl SystemFile {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path)
            .map(|fd| Self { fd })
            .map_err(|_| anyhow::Error::msg("open system file error!"))
    }
}

impl IO for SystemFile {
    fn write(&mut self, buf: &[u8], offset: u64) -> anyhow::Result<u32> {
        self.fd
            .write_at(buf, offset)
            .map(|size| size as u32)
            .map_err(|_| anyhow::Error::msg("system file write buf error!"))
    }

    fn read(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<u32> {
        self.fd
            .read_at(buf, offset)
            .map(|size| size as u32)
            .map_err(|_| anyhow::Error::msg("system file read buf error!"))
    }

    fn sync(&self) -> anyhow::Result<()> {
        self.fd
            .sync_all()
            .map_err(|_| anyhow::Error::msg("system file sync error1"))
    }
}
