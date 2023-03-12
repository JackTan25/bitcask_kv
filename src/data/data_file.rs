use std::fs;
use std::path::PathBuf;

use crate::errors::Errors;
use crate::errors::Result;
use crate::fio;

use super::log_record::*;
pub struct DataFile {
    file_id: u32,
    write_offset: u64,
    // 使用特征对象
    fio: Box<dyn fio::IOManager>,
}

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

impl DataFile {
    // 获取新的DataFile放到old_files这一map当中来
    pub fn new(dirpath: PathBuf, file_id: u32) -> Result<DataFile> {
        // DataFile { file_id: file_id, write_offset: 0, fio::File }
        todo!()
    }
    // 获取当前文件写入大小
    pub fn get_wtite_offset(&self) -> u64 {
        self.write_offset
    }
    // 持久化当前文件
    pub fn sync(&self) -> Result<()> {
        self.fio.sync()
    }

    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        todo!()
    }

    // 写数据到文件当中
    pub fn write(&self, buf: &[u8], offset: u64) -> Result<usize> {
        todo!()
    }

    pub fn get_file_id(&self) -> u32 {
        self.file_id
    }
    /// 加载数据文件
    pub fn load_data_files(dirpath: PathBuf) -> Result<Vec<DataFile>> {
        // 1.读取数据目录
        let dir_files = fs::read_dir(dirpath.clone());
        if dir_files.is_err() {
            return Err(Errors::DirPathReadFailed);
        }
        let mut file_ids: Vec<u32> = Vec::new();
        let mut datafiles: Vec<DataFile> = Vec::new();
        for file in dir_files.unwrap() {
            let entry = file.unwrap().file_name();
            let file_name = entry.to_str().unwrap();
            // 我们只需要拿到数据文件,所以我们需要看后缀名
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let splits: Vec<&str> = file_name.split(".").collect();
                let file_id = match splits[0].parse::<u32>() {
                    Ok(file_id) => file_id,
                    Err(_) => return Err(Errors::DataFileCorrupted),
                };
                file_ids.push(file_id)
            }
        }
        // 对file_id进行排序
        file_ids.sort();
        for file_id in file_ids {
            // 这里出现错误我们不用unwarp将其panic掉
            // 而是使用?范围Err
            datafiles.push(DataFile::new(dirpath.clone(), file_id)?)
        }
        return Ok(datafiles);
    }
}
