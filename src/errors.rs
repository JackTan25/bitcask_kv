use thiserror::Error;

// 这个文件用来自定义我们自己的error
#[derive(Error,Debug)]
pub enum  Errors {
    // 利用thiserror来实现display
    #[error("Fail to read data from file")]
    FailReadFromFile,
    #[error("Fail to write data to file")]
    FailWriteDataToFile,
    #[error("Fail to sync data to file")]
    FailSyncDataToFile
}

pub type Result<T> = std::result::Result<T,Errors>;