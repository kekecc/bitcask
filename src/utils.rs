use std::path::{Path, PathBuf};

use crate::consts::DATA_FILE_SUFFIX;

pub fn get_merge_path(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().join(".merge")
}

pub fn get_data_file_path(path: impl AsRef<Path>, file_id: u32) -> PathBuf {
    path.as_ref()
        .join(format!("{:09}{}", file_id, DATA_FILE_SUFFIX))
}
