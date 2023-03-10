use std::collections::HashMap;
use std::env::consts::FAMILY;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::data;
use crate::data::log_record::LogRecordType;
use crate::index::Indexer;
use crate::options::Options;
use crate::errors::{Result, Errors};
use crate::data::{log_record::{LogRecord,LogRecordPos},data_file::DataFile};
pub struct Engine{
    options:Options,
    // active file
    data_file:Arc<RwLock<DataFile>>,
    // old files
    old_files:Arc<RwLock<HashMap<u32,DataFile>>>,
    // memory Index: key -> LogRecordPos
    indexer:Box<dyn Indexer>,
}

impl Engine{
    // 存储的kv对采用的是Bytes
    pub fn put(&self,key:Bytes,value:Bytes) -> Result<()>{
        // 我们不允许key是empty的
        if key.is_empty(){
            return Err(Errors::KeyEmptyErr);
        }
        let mut log_recored = LogRecord{
            key:key.to_vec(),
            value:value.to_vec(),
            log_type:crate::data::log_record::LogRecordType::NORMAL,
        };
        // 追加日志信息
        let logrecord_pos =self.appendLog(&mut log_recored)?;
        // 更新内存索引信息
        let ok = self.indexer.put(key.to_vec(),logrecord_pos);
        // 当然,btreeIndex一直返回的都是true，为了逻辑完整性
        if !ok{
            return Err(Errors::FailUpdateIndexer);
        }
        Ok(())
    }

    pub fn get(&self,key:Bytes) -> Result<Bytes>{
        // 1.非空key检测
        if key.is_empty(){
            return Err(Errors::KeyEmptyErr);
        }
        // 2. 查询索引信息获取LogRecordPos
        let log_record_pos_option = self.indexer.get(key.to_vec());
        if log_record_pos_option.is_none(){
            return Err(Errors::KeyNotFoundInIndex);
        }
        let log_record_pos = log_record_pos_option.unwrap();
        let active_file_read_guard = self.data_file.read();
        let old_files_read_guard = self.old_files.read();
        // 3. 根据LogRecordPos去查询
        let log_record = match active_file_read_guard.get_file_id() == log_record_pos.file_id{
            true => {
                active_file_read_guard.read_log_record(log_record_pos.offset)?
            },
            false => {
                let data_file = old_files_read_guard.get(&log_record_pos.file_id);
                if data_file.is_none(){
                    return  Err(Errors::KeyNotFoundInDataFile);
                }
                data_file.unwrap().read_log_record(log_record_pos.offset)?
            }
        };
        if log_record.log_type == LogRecordType::DELETED{
            return Err(Errors::KeyNotFound);
        }
        return Ok(log_record.value.into());
    }

    pub fn appendLog(&self,log_record:&mut LogRecord) -> Result<LogRecordPos>{
        // 1.编码logRecord
        let enc_log_record = log_record.encode();
        let record_len = enc_log_record.len() as u64;
        // 2.获取活跃文件写锁锁
        let mut active_file_write_guard = self.data_file.write();
        // 3.超过阈值就持久化,并开启新的文件
        if active_file_write_guard.get_wtite_offset() +  record_len > self.options.file_size_threshlod{
            active_file_write_guard.sync()?;
            let old_file_id = active_file_write_guard.get_file_id();
            let mut old_files_write_guard = self.old_files.write();
            old_files_write_guard.insert(old_file_id,DataFile::new(self.options.dir_path.clone(),old_file_id));
            // 更新活跃文件
            let new_data_file = DataFile::new(self.options.dir_path.clone(), old_file_id+1);
            *active_file_write_guard = new_data_file;
        }
        // 4.append log
        active_file_write_guard.write(&enc_log_record,active_file_write_guard.get_wtite_offset())?;
        // 5.根据配置看是否每次都要进行持久化
        if self.options.sync{
            active_file_write_guard.sync()?;
        }

        // 写完数据后，构造内存索引信息并返回
        Ok(LogRecordPos { file_id: active_file_write_guard.get_file_id(), offset: active_file_write_guard.get_wtite_offset()-record_len})
    }
}