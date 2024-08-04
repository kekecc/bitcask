pub mod batch_write;
pub mod data;
pub mod file;
pub mod index;
pub mod key;
pub mod merge;
pub mod options;
pub mod storage;
pub mod utils;

pub mod consts {
    pub const DATA_FILE_SUFFIX: &str = ".data";
    pub const HINT_FILE_NAME: &str = "index.HINT";
    pub const MERGE_FILE_NAME: &str = "db.MERGE";
    pub const FILE_LOCK: &str = "FILE_LOCK";
}
