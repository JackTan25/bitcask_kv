use std::path::PathBuf;

use crate::errors::Errors;

#[derive(Clone)]
pub struct Options {
    pub dir_path: PathBuf,
    pub file_size_threshlod: u64,
    pub sync: bool,
    pub index_type: IndexType,
}

impl Options {
    pub fn check_options(&self) -> Option<Errors> {
        let dirpath = self.dir_path.to_str();
        // 1.检测文件目录是否是空
        if dirpath.is_none() || dirpath.unwrap().len() == 0 {
            return Some(Errors::DirPathEmptyError);
        }
        // 2.检测文件大小配置是否合理
        if self.file_size_threshlod <= 0 {
            return Some(Errors::InvalidDataFileSizeOption);
        }
        None
    }
}

#[derive(Clone, Copy)]
pub enum IndexType {
    Btree,
    SkipList,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            dir_path: std::env::temp_dir(),
            file_size_threshlod: 256 * 1024 * 1024,
            sync: false,
            index_type: IndexType::Btree,
        }
    }
}

pub struct WriteBatchOptions {
    pub batch_max_rows: u32,
    pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            batch_max_rows: 10000,
            sync_writes: true,
        }
    }
}
