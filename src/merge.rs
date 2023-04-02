use crate::{
    data::{
        data_file::MERGE_FINISHED_FILE_NAME,
        log_record::{LogRecord, LogRecordPos},
    },
    db::NO_TXN_SEQ_NO,
    write_batch::WriteBatch,
};
use bytes::Bytes;
use log::error;
use prost::encoding::decode_varint;
use std::{cell::RefCell, path::PathBuf};

use crate::{
    data::{data_file::DataFile, log_record::ReadLogRecord},
    db::Engine,
    errors::{Errors, Result},
    options::Options,
};

const MERGE_NAME: &str = "merge";
const MERGE_FNISHED_KEY: &[u8] = "merge-finished".as_bytes();
impl Engine {
    pub fn merge(&mut self) -> Result<()> {
        // 使用RefCell实现可变与不可变的同时使用
        let db = RefCell::new(self);
        let res = db.borrow();
        let lock = res.merge_lock.try_lock();
        if lock.is_none() {
            return Err(Errors::MergeInProcess);
        }

        let merge_dir_path = get_merge_dirpath(db.borrow().options.dir_path.clone());
        // 可能之前已经进行过merge,那么这里就需要将merge的老的目录删除掉(它可能是成功或者未成功的)
        if merge_dir_path.is_dir() {
            // 已经存在这个目录就需要将其删除掉
            std::fs::remove_dir_all(merge_dir_path.clone()).unwrap();
        }
        // 创建merge的目录
        if let Err(e) = std::fs::create_dir_all(merge_dir_path.clone()) {
            error!("failed to create merge dir path");
            return Err(Errors::DirPathCreateFailed);
        }
        // 创建临时的merge-db实例
        let mut merge_options = Options::default();
        merge_options.dir_path = merge_dir_path.clone();
        merge_options.file_size_threshlod = db.borrow().options.file_size_threshlod;
        let merge_db = Engine::open(merge_options)?;
        let merge_files = db.borrow_mut().get_merge_files()?;
        // 打开hint_file文件
        let hint_file = DataFile::new_hint_file(db.borrow().options.dir_path.clone()).unwrap();
        // 接下来就开始一次处理每一个old_file进行
        for file in merge_files.iter() {
            let mut offset = 0;
            loop {
                let logrecord_res: Result<ReadLogRecord> = file.read_log_record(offset);
                let (mut logrecord, size) = match logrecord_res {
                    Ok(res) => (res.logrecord, res.size),
                    Err(e) => {
                        if e == Errors::DataFileReadEOF {
                            break;
                        }
                        return Err(e);
                    }
                };
                // 在writeBatch之后我们的key的编码发生了改变,这里我们需要解析一下
                let (key, _) = db.borrow().parse_key(logrecord.key.clone());
                // 看在index里面这个key的pos是否对的上
                // 更新offset
                if let Some(pos) = db.borrow().indexer.get(key) {
                    // 如果确认是有效key,就写入
                    if pos.file_id == file.get_file_id() && pos.offset == offset {
                        let real_key = logrecord.key.clone();
                        logrecord.key =
                            WriteBatch::encode_key_seqno(Bytes::from(real_key), NO_TXN_SEQ_NO);
                    }
                    merge_db.append_log(&mut logrecord)?;
                    // 写hint file
                    let real_key = logrecord.key.clone();
                    hint_file
                        .write_hint_file_record(real_key, pos.clone())
                        .unwrap();
                }
                offset += size as u64;
            }
        }
        merge_db.sync().unwrap();
        hint_file.sync().unwrap();
        let no_merge_fileid = merge_files.last().unwrap().get_file_id() + 1;
        // 然后去拿没有参与到merge的文件
        let merge_finished_file =
            DataFile::new_finished_file(db.borrow().options.dir_path.clone())?;
        let log_record = LogRecord {
            key: MERGE_FNISHED_KEY.to_vec(),
            value: no_merge_fileid.to_string().into_bytes(),
            log_type: crate::data::log_record::LogRecordType::NORMAL,
        };
        let encode_record = log_record.encode();
        merge_finished_file.write(&encode_record).unwrap();
        merge_finished_file.sync().unwrap();
        Ok(())
    }

    fn get_merge_files(&mut self) -> Result<Vec<DataFile>> {
        let mut res_merge_datafiles = Vec::new();
        // 需要进行merge的文件id
        let mut merge_files_ids = Vec::new();
        // 拿到old file和active file
        let mut old_files = self.old_files.write();
        for file_id in old_files.keys() {
            merge_files_ids.push(*file_id)
        }
        // 拿到active file
        let mut active_file = self.data_file.write();
        let active_id = active_file.get_file_id();
        merge_files_ids.push(active_id);
        let old_active_file = DataFile::new(self.options.dir_path.clone(), active_id)?;
        old_files.insert(active_id, old_active_file);
        self.max_file_id = active_id + 1;
        // 创建一个新的active file
        let new_active_file = DataFile::new(self.options.dir_path.clone(), active_id + 1)?;
        *active_file = new_active_file;

        merge_files_ids.sort();

        for file_id in merge_files_ids {
            res_merge_datafiles.push(DataFile::new(self.options.dir_path.clone(), file_id)?);
        }
        return Ok(res_merge_datafiles);
    }

    pub fn load_merge_files(dir_path: PathBuf) -> Result<()> {
        let merge_path = get_merge_dirpath(dir_path.clone());
        // 拿到merge_path下的所有文件
        let read_dir = std::fs::read_dir(merge_path.clone()).unwrap();
        let mut merge_finished = false;
        let mut merge_names = Vec::new();
        for entry in read_dir.into_iter() {
            if let Ok(file) = entry {
                if file
                    .file_name()
                    .to_str()
                    .unwrap()
                    .ends_with(MERGE_FINISHED_FILE_NAME)
                {
                    merge_finished = true;
                } else {
                    merge_names.push(file.file_name());
                }
            }
        }
        // 如果没有完成merge,就删除掉旧的merge目录
        if !merge_finished {
            std::fs::remove_dir_all(merge_path).unwrap();
            return Ok(());
        }
        // merge完成,读取merge_finished_file看
        // 哪些文件被merge了
        let merge_file = DataFile::new_finished_file(merge_path.clone())?;
        let read_logrecord = merge_file.read_log_record(0)?;
        let v = String::from_utf8(read_logrecord.logrecord.value).unwrap();
        let no_merge_file_id = v.parse::<u32>().unwrap();
        // 将已经被merge过的文件给删除掉
        for file_id in 0..no_merge_file_id {
            let file_path = DataFile::get_file_name(dir_path.clone(), file_id);
            if file_path.is_file() {
                std::fs::remove_file(file_path).unwrap();
            }
        }

        // 移动merge的文件
        for file_name in merge_names {
            let ori_fil_path = merge_path.join(file_name.clone());
            let target_file_name = dir_path.join(file_name.clone());
            std::fs::rename(ori_fil_path, target_file_name).unwrap();
        }
        // 最后删除merge目录
        std::fs::remove_dir_all(merge_path).unwrap();
        Ok(())
    }

    pub fn load_hint_file(&self) -> Result<()> {
        let hint_file_path = self
            .options
            .dir_path
            .join(crate::data::data_file::HIT_FILE_NAME);
        if !hint_file_path.is_file() {
            return Ok(());
        }
        let hint_file = DataFile::new_hint_file(self.options.dir_path.clone())?;
        let mut offset = 0;
        loop {
            let logrecord_res: Result<ReadLogRecord> = hint_file.read_log_record(offset);
            let (logrecord, size) = match logrecord_res {
                Ok(res) => (res.logrecord, res.size),
                Err(e) => {
                    if e == Errors::DataFileReadEOF {
                        break;
                    }
                    return Err(e);
                }
            };
            let pos = LogRecordPos::decode(logrecord.value);
            self.indexer.put(logrecord.key.clone(), pos);
            offset += size as u64;
        }
        Ok(())
    }
}

fn get_merge_dirpath(dir_path: PathBuf) -> PathBuf {
    let file_name = dir_path.file_name().unwrap();
    let merge_name = format!("{}-{}", file_name.to_str().unwrap(), MERGE_NAME);
    let parent = dir_path.parent().unwrap();
    parent.to_path_buf().join(merge_name)
}
