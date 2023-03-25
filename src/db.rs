use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use bytes::Bytes;
use log::error;
use parking_lot::RwLock;

use crate::data::log_record::{LogRecordType, ReadLogRecord};
use crate::data::{
    data_file::DataFile,
    log_record::{LogRecord, LogRecordPos},
};
use crate::errors::{Errors, Result};
use crate::index::{Indexer, NewIndexer};
use crate::options::Options;
use crate::{data, options};
pub struct Engine {
    options: Options,
    // active file
    data_file: Arc<RwLock<DataFile>>,
    // old files
    old_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    // memory Index: key -> LogRecordPos
    pub(crate) indexer: Box<dyn Indexer>,
    // max file id is just used in engine init step
    max_file_id: u32,
}

const INIT_FILE_ID: u32 = 0;

impl Engine {
    // sync
    // 防止数据丢失
    pub fn sync(&self) -> Result<()> {
        let write_guard = self.data_file.write();
        write_guard.sync()
    }
    // close
    // 资源清理
    pub fn close(&self) -> Result<()> {
        let write_guard = self.data_file.write();
        write_guard.sync()
    }

    // 根据配置打开一个DB实例
    pub fn open(options: Options) -> Result<Self> {
        println!("文件夹:{:?}", options.dir_path);
        // 首先需要检测options的合法性
        if let Some(e) = options.check_options() {
            return Err(e);
        }
        // 合法性检测通过(当然不是完整的合法检测，比如目录是否是合法的没做)
        // 后,我们就开始看目录是否存在，不存在，就自己创建一个新的目录
        if let Err(e) = fs::create_dir_all(options.dir_path.as_path()) {
            // 这里创建文件失败
            error!("create database dirpath failed: {}", e);
            return Err(Errors::DirPathCreateFailed);
        }
        // 开始加载文件
        let mut data_files = DataFile::load_data_files(options.dir_path.clone())?;
        // 切分active_files 和 old_files
        let active_file: DataFile;
        let max_file_id = data_files.len();
        // 拿到active_file
        if data_files.len() > 0 {
            active_file = data_files.pop().unwrap();
        } else {
            active_file = DataFile::new(options.dir_path.clone(), INIT_FILE_ID).unwrap();
        }

        // old files
        let mut old_files_hashmap = HashMap::new();
        if data_files.len() >= 1 {
            for id in 0..=data_files.len() - 1 {
                let old_file = data_files.pop().unwrap();
                old_files_hashmap.insert(id as u32, old_file);
            }
        }
        // 构建DB实例
        let engine = Engine {
            max_file_id: max_file_id as u32,
            indexer: NewIndexer(options.index_type),
            options: options,
            data_file: Arc::new(RwLock::new(active_file)),
            old_files: Arc::new(RwLock::new(old_files_hashmap)),
        };

        // 加载索引
        match engine.load_index_from_datafiles() {
            Ok(_) => return Ok(engine),
            Err(e) => return Err(e),
        }
    }

    fn load_index_from_datafiles(&self) -> Result<()> {
        // 没有文件存在，不需要加载索引
        if self.max_file_id == 0 {
            return Ok(());
        }
        let read_guard = self.old_files.read();
        for id in 0..=self.max_file_id - 1 {
            let mut file: Option<&DataFile> = None;
            // 1.拿到读锁
            if id != self.max_file_id - 1 {
                file = Some(read_guard.get(&id).unwrap());
            }
            let mut offset = 0;
            loop {
                let logrecord_res: Result<ReadLogRecord>;
                if id == self.max_file_id - 1 {
                    logrecord_res = self.data_file.read().read_log_record(offset);
                } else {
                    logrecord_res = file.unwrap().read_log_record(offset);
                }
                let (logrecord, size) = match logrecord_res {
                    Ok(res) => (res.logrecord, res.size),
                    Err(e) => {
                        if e == Errors::DataFileReadEOF {
                            break;
                        }
                        return Err(e);
                    }
                };
                // 读取到logrecord 后就可以构建索引了
                if logrecord.log_type == LogRecordType::NORMAL {
                    self.indexer.put(
                        logrecord.key.to_vec(),
                        LogRecordPos {
                            file_id: id,
                            offset: offset,
                        },
                    );
                } else {
                    self.indexer.delete(logrecord.key.to_vec());
                }
                // 更新offset
                offset += size as u64;
            }
        }
        Ok(())
    }

    // 存储的kv对采用的是Bytes
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // println!("put: {:?},{:?}",key,value);
        // 我们不允许key是empty的
        if key.is_empty() {
            return Err(Errors::KeyEmptyErr);
        }
        let mut log_recored = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            log_type: crate::data::log_record::LogRecordType::NORMAL,
        };
        // 追加日志信息
        let logrecord_pos = self.append_log(&mut log_recored)?;
        // 更新内存索引信息
        let ok = self.indexer.put(key.to_vec(), logrecord_pos);
        // 当然,btreeIndex一直返回的都是true，为了逻辑完整性
        if !ok {
            return Err(Errors::FailUpdateIndexer);
        }
        Ok(())
    }

    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        // 1.非空key检测
        if key.is_empty() {
            return Err(Errors::KeyEmptyErr);
        }
        // 2. 查询索引信息获取LogRecordPos
        let log_record_pos_option = self.indexer.get(key.to_vec());
        if log_record_pos_option.is_none() {
            return Err(Errors::KeyNotFound);
        }
        let log_record_pos = log_record_pos_option.unwrap();
        self.get_value_by_pos(&log_record_pos)
    }

    pub(crate) fn get_value_by_pos(&self, log_record_pos: &LogRecordPos) -> Result<Bytes> {
        let active_file_read_guard = self.data_file.read();
        let old_files_read_guard = self.old_files.read();
        // 3. 根据LogRecordPos去查询
        let readlog_record = match active_file_read_guard.get_file_id() == log_record_pos.file_id {
            true => active_file_read_guard.read_log_record(log_record_pos.offset)?,
            false => {
                let data_file = old_files_read_guard.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    return Err(Errors::KeyNotFoundInDataFile);
                }
                data_file.unwrap().read_log_record(log_record_pos.offset)?
            }
        };
        // println!("logrecord_pos: id -> {:?},offset -> {:?}",log_record_pos.file_id,log_record_pos.offset);
        // println!("get: {:?},{:?},{:?}",  Bytes::from(readlog_record.logrecord.key.clone()),Bytes::from(readlog_record.logrecord.value.clone()),readlog_record.logrecord.log_type);
        if readlog_record.logrecord.log_type == LogRecordType::DELETED {
            return Err(Errors::KeyNotFound);
        }
        return Ok(readlog_record.logrecord.value.into());
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        // 1.判断空key
        if key.is_empty() {
            return Err(Errors::KeyEmptyErr);
        }
        // 2.从内存索引获取
        let logrecord_pos = self.indexer.get(key.to_vec());
        if logrecord_pos.is_none() {
            return Ok(());
        }
        let mut log_record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            log_type: LogRecordType::DELETED,
        };
        match self.append_log(&mut log_record) {
            Ok(_) => {
                self.indexer.delete(key.to_vec());
                return Ok(());
            }
            Err(e) => return Err(e),
        }
    }

    pub fn append_log(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        // 1.编码logRecord
        let enc_log_record = log_record.encode();
        let record_len = enc_log_record.len() as u64;
        // 2.获取活跃文件写锁锁
        let mut active_file_write_guard = self.data_file.write();
        // 3.超过阈值就持久化,并开启新的文件
        if active_file_write_guard.get_wtite_offset() + record_len
            > self.options.file_size_threshlod
        {
            active_file_write_guard.sync()?;
            let old_file_id = active_file_write_guard.get_file_id();
            let mut old_files_write_guard = self.old_files.write();
            old_files_write_guard.insert(
                old_file_id,
                DataFile::new(self.options.dir_path.clone(), old_file_id)?,
            );
            // 更新活跃文件
            let new_data_file = DataFile::new(self.options.dir_path.clone(), old_file_id + 1);
            *active_file_write_guard = new_data_file.unwrap();
        }
        // 4.append log
        active_file_write_guard.write(&enc_log_record)?;
        // 5.根据配置看是否每次都要进行持久化
        if self.options.sync {
            active_file_write_guard.sync()?;
        }

        // 写完数据后，构造内存索引信息并返回
        Ok(LogRecordPos {
            file_id: active_file_write_guard.get_file_id(),
            offset: active_file_write_guard.get_wtite_offset() - record_len,
        })
    }
}
