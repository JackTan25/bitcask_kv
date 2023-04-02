use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use bytes::Buf;
use bytes::BytesMut;
use parking_lot::RwLock;
use prost::decode_length_delimiter;
use prost::length_delimiter_len;

use crate::errors::Errors;
use crate::errors::Result;
use crate::fio;
use crate::fio::new_io_manager;

use super::log_record::*;
pub struct DataFile {
    file_id: u32,
    write_offset: RwLock<u64>,
    // 使用特征对象
    fio: Box<dyn fio::IOManager>,
}

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub const HIT_FILE_NAME: &str = "hint-index";
pub const MERGE_FINISHED_FILE_NAME: &str = "merge-finished";
impl DataFile {
    pub fn new_hint_file(dir_path: PathBuf) -> Result<DataFile> {
        let file_name = dir_path.join(HIT_FILE_NAME);
        let io_manager = new_io_manager(&file_name)?;
        Ok(DataFile {
            file_id: 0,
            write_offset: RwLock::new(0),
            fio: Box::new(io_manager),
        })
    }

    pub fn new_finished_file(dir_path: PathBuf) -> Result<DataFile> {
        let file_name = dir_path.join(MERGE_FINISHED_FILE_NAME);
        let io_manager = new_io_manager(&file_name)?;
        Ok(DataFile {
            file_id: 0,
            write_offset: RwLock::new(0),
            fio: Box::new(io_manager),
        })
    }

    fn set_write_offset(&self, size: u64) {
        let mut write_guard = self.write_offset.write();
        *write_guard += size as u64;
    }

    pub fn get_file_name(dirpath: PathBuf, file_id: u32) -> PathBuf {
        let file_id_str = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
        dirpath.join(file_id_str)
    }

    // 获取新的DataFile放到old_files这一map当中来
    pub fn new(dirpath: PathBuf, file_id: u32) -> Result<DataFile> {
        let file_name = DataFile::get_file_name(dirpath, file_id);
        let io_manager = new_io_manager(&file_name)?;
        Ok(DataFile {
            file_id: file_id,
            write_offset: RwLock::new(0),
            fio: Box::new(io_manager),
        })
    }

    // 获取当前文件写入大小
    pub fn get_wtite_offset(&self) -> u64 {
        *self.write_offset.read()
    }

    // 持久化当前文件
    pub fn sync(&self) -> Result<()> {
        self.fio.sync()
    }

    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // 预取内存
        let mut header_bytes = BytesMut::zeroed(LogRecord::max_logrecord_header());
        // fio的read方法如果读不到数据并没有返回ReadDataFileEOF
        self.fio.read(&mut header_bytes, offset)?;

        // 读取当前record的类型
        let rec_typ = header_bytes.get_u8();
        let key_size = decode_length_delimiter(&mut header_bytes).unwrap();
        let value_size = decode_length_delimiter(&mut header_bytes).unwrap();

        // 如果key_size 为0说明没有数据了
        if key_size == 0 {
            return Err(Errors::DataFileReadEOF);
        }

        let actual_header_size =
            length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;
        let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.fio
            .read(&mut kv_buf, offset + actual_header_size as u64)?;

        let read_log_record = ReadLogRecord {
            size: (actual_header_size + key_size + value_size + 4) as i64,
            logrecord: LogRecord {
                key: kv_buf.get(..key_size).unwrap().to_vec(),
                value: kv_buf.get(key_size..(kv_buf.len() - 4)).unwrap().to_vec(),
                log_type: LogRecordType::from_byte(rec_typ),
            },
        };

        // 做checksum检测
        kv_buf.advance(key_size + value_size);
        let checksum = kv_buf.get_u32();
        if checksum != read_log_record.logrecord.crc32() {
            return Err(Errors::CheckSumFailed);
        }
        Ok(read_log_record)
    }

    // 写数据到文件当中
    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let size = self.fio.write(buf)?;
        let mut write_guard = self.write_offset.write();
        *write_guard += size as u64;
        Ok(size)
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
        let mut file_size: HashMap<u32, u64> = HashMap::new();
        let mut datafiles: Vec<DataFile> = Vec::new();
        for file in dir_files.unwrap() {
            let entry = file.unwrap().file_name();
            let file_name = entry.to_str().unwrap();
            let metadata = std::fs::metadata(dirpath.join(file_name)).unwrap();
            // 我们只需要拿到数据文件,所以我们需要看后缀名
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let splits: Vec<&str> = file_name.split(".").collect();
                let file_id = match splits[0].parse::<u32>() {
                    Ok(file_id) => file_id,
                    Err(_) => return Err(Errors::DataFileCorrupted),
                };
                file_ids.push(file_id);
                file_size.insert(file_id, metadata.len() as u64);
            }
        }
        // 对file_id进行排序
        file_ids.sort();
        for file_id in file_ids {
            // 这里出现错误我们不用unwarp将其panic掉
            // 而是使用?范围Err
            let datafile = DataFile::new(dirpath.clone(), file_id)?;
            datafile.set_write_offset(*file_size.get(&file_id).unwrap() as u64);
            datafiles.push(datafile);
        }
        return Ok(datafiles);
    }

    pub fn write_hint_file_record(&self, key: Vec<u8>, pos: LogRecordPos) -> Result<()> {
        let log_record = LogRecord {
            key: key,
            value: pos.encode(),
            log_type: LogRecordType::NORMAL,
        };
        self.write(&log_record.encode())?;
        Ok(())
    }
}

#[cfg(test)]
mod data_file_test {
    use std::fs;

    use crate::data::log_record::{LogRecord, LogRecordType::*};

    use super::DataFile;

    #[test]
    fn test_new_datafile() {
        let temp_dir = std::env::temp_dir();
        // 构造一个新的文件
        let datafile_res = DataFile::new(temp_dir.clone(), 0);
        assert!(datafile_res.is_ok());
        let datafile = datafile_res.unwrap();
        assert_eq!(datafile.get_file_id(), 0);
        assert_eq!(datafile.get_wtite_offset(), 0);

        // 重新打开老的文件(会清空)
        let datafile_res = DataFile::new(temp_dir.clone(), 0);
        assert!(datafile_res.is_ok());
        let datafile = datafile_res.unwrap();
        assert_eq!(datafile.get_file_id(), 0);
        assert_eq!(datafile.get_wtite_offset(), 0);

        // 再打开一个新文件
        let datafile_res = DataFile::new(temp_dir.clone(), 10);
        assert!(datafile_res.is_ok());
        let datafile = datafile_res.unwrap();
        assert_eq!(datafile.get_file_id(), 10);
        assert_eq!(datafile.get_wtite_offset(), 0);
    }
    #[test]
    fn test_write_datafile() {
        let temp_dir = std::env::temp_dir();
        // 构造一个新的文件
        let datafile_res = DataFile::new(temp_dir.clone(), 0);
        assert!(datafile_res.is_ok());
        let datafile = datafile_res.unwrap();
        assert_eq!(datafile.get_file_id(), 0);
        assert_eq!(datafile.get_wtite_offset(), 0);
        // write
        let write_res1 = datafile.write("abc".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 3 as usize);

        let write_res2 = datafile.write("def".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 3 as usize);

        let write_res3 = datafile.write("ghi".as_bytes());
        assert!(write_res3.is_ok());
        assert_eq!(write_res3.unwrap(), 3 as usize);
    }

    #[test]
    fn test_datafile_sync() {
        let temp_dir = std::env::temp_dir();
        // 构造一个新的文件
        let datafile_res = DataFile::new(temp_dir.clone(), 0);
        assert!(datafile_res.is_ok());
        let datafile = datafile_res.unwrap();
        assert_eq!(datafile.get_file_id(), 0);
        assert_eq!(datafile.get_wtite_offset(), 0);
        // write
        let write_res1 = datafile.write("abc".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 3 as usize);
        // sync
        let sync_res = datafile.sync();
        assert!(sync_res.is_ok())
    }

    #[test]
    fn test_read_log_record() {
        let temp_dir = std::env::temp_dir();
        // 构造一个新的文件
        let datafile_res = DataFile::new(temp_dir.clone(), 300);
        assert!(datafile_res.is_ok());
        let datafile = datafile_res.unwrap();
        assert_eq!(datafile.get_file_id(), 300);
        assert_eq!(datafile.get_wtite_offset(), 0);
        // 写入一个LogRecord
        let logrecord1 = LogRecord {
            key: "key1".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
            log_type: NORMAL,
        };
        let write_size = datafile.write(&logrecord1.encode()).unwrap();
        // 读第一个LogRecord
        let read_logrecord1 = datafile.read_log_record(0).unwrap();
        let log_record = read_logrecord1.logrecord;
        assert_eq!(log_record.key, logrecord1.key);
        assert_eq!(log_record.value, logrecord1.value);
        assert_eq!(log_record.log_type, logrecord1.log_type);
        // 再写入一个LogRecord
        let logrecord2 = LogRecord {
            key: "key2".as_bytes().to_vec(),
            value: "value2".as_bytes().to_vec(),
            log_type: NORMAL,
        };
        let write_size2 = datafile.write(&logrecord2.encode()).unwrap();
        let read_logrecord2 = datafile.read_log_record(write_size as u64).unwrap();
        let log_record = read_logrecord2.logrecord;
        assert_eq!(log_record.key, logrecord2.key);
        assert_eq!(log_record.value, logrecord2.value);
        assert_eq!(log_record.log_type, logrecord2.log_type);
        // 测试一个删除的LogRecord
        let logrecord3 = LogRecord {
            key: "key3".as_bytes().to_vec(),
            value: Default::default(),
            log_type: DELETED,
        };
        datafile.write(&logrecord3.encode()).unwrap();
        let read_logrecord3 = datafile
            .read_log_record((write_size + write_size2) as u64)
            .unwrap();
        let log_record = read_logrecord3.logrecord;
        assert_eq!(log_record.key, logrecord3.key);
        assert_eq!(log_record.value, logrecord3.value);
        assert_eq!(log_record.log_type, logrecord3.log_type);
    }
}
