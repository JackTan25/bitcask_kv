use thiserror::Error;

#[allow(dead_code)]
// 这个文件用来自定义我们自己的error
#[derive(Error, Debug)]
pub enum Errors {
    // 利用thiserror来实现display
    #[error("Fail to read data from file")]
    FailReadFromFile,
    #[error("Fail to write data to file")]
    FailWriteDataToFile,
    #[error("Fail to sync data to file")]
    FailSyncDataToFile,
    #[error("Fail to new a data file")]
    FailNewDataFile,
    #[error("Key can't be empty")]
    KeyEmptyErr,
    #[error("Fail to Update Memory Indexer")]
    FailUpdateIndexer,
    #[error("Key not Found in Index")]
    KeyNotFoundInIndex,
    #[error("Key not Found in DataFile")]
    KeyNotFoundInDataFile,
    #[error("Key Not Found")]
    KeyNotFound,
}

pub type Result<T> = std::result::Result<T, Errors>;
