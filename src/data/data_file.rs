use std::path::PathBuf;

use bytes::Bytes;

use crate::fio;
use crate::errors::Result;

use super::log_record::LogRecord;
pub struct DataFile{
    file_id:u32,
    write_offset:u64,
    // 使用特征对象
    fio:Box<dyn fio::IOManager>,
}

impl DataFile{
    // 获取新的DataFile放到old_files这一map当中来
    pub fn new(dirpath:PathBuf,file_id:u32) -> DataFile{
        // DataFile { file_id: file_id, write_offset: 0, fio::File }
        todo!()
    }
    // 获取当前文件写入大小
    pub fn get_wtite_offset(&self) -> u64{
        self.write_offset
    }
    // 持久化当前文件
    pub fn sync(&self) -> Result<()>{
        self.fio.sync()
    }

    pub fn read_log_record(&self,offset:u64) -> Result<LogRecord>{
        todo!()
    }

    // 写数据到文件当中
    pub fn write(&self,buf:&[u8],offset:u64) -> Result<usize>{
        todo!()
    }

    pub fn get_file_id(&self) -> u32{
        self.file_id
    }
}