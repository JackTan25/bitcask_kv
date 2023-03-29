use thiserror::Error;

#[allow(dead_code)]
// 这个文件用来自定义我们自己的error
#[derive(Error, Debug, PartialEq)]
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
    #[error("DirPath Is Invalid, can't be empty")]
    DirPathEmptyError,
    #[error("FileSize option must greater than 0")]
    InvalidDataFileSizeOption,
    #[error("Create DirPath Failed")]
    DirPathCreateFailed,
    #[error("Read DirPath Error")]
    DirPathReadFailed,
    #[error("DataFile Maybe Corrupted")]
    DataFileCorrupted,
    #[error("DataFile Read EOF")]
    DataFileReadEOF,
    #[error("CheckSum Failed, the LogRecord maybe broken")]
    CheckSumFailed,
    #[error("Over MaxBatchRows")]
    ExceedBatchMaxRows,
}

pub type Result<T> = std::result::Result<T, Errors>;
