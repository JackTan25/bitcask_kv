use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::encode_length_delimiter;
use std::sync::atomic::*;
use std::{collections::HashMap, sync::Arc};

use crate::data::log_record::{self, LogRecordType};
use crate::{
    data::log_record::{LogRecord, LogRecordType::*},
    db::Engine,
    errors::{
        Errors::{self, *},
        Result,
    },
    options::WriteBatchOptions,
};

pub const TXN_FIN: &[u8] = "TXN_FIN".as_bytes();

pub struct WriteBatch<'a> {
    pending_data: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    engine: &'a Engine,
    options: WriteBatchOptions,
}

impl Engine {
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        Ok(WriteBatch {
            pending_data: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options: options,
        })
    }
}

impl WriteBatch<'_> {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(KeyEmptyErr);
        }
        let log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            log_type: NORMAL,
        };
        let mut lock_guard = self.pending_data.lock();
        lock_guard.insert(key.to_vec(), log_record);
        Ok(())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(KeyEmptyErr);
        }
        // 看索引是否真的存在这个key
        let log_record_pos = self.engine.indexer.get(key.to_vec());
        // 不存在直接返回即可
        if log_record_pos.is_none() {
            return Ok(());
        }
        let log_record = LogRecord {
            key: key.to_vec(),
            value: Vec::new(),
            log_type: DELETED,
        };
        let mut guard = self.pending_data.lock();
        guard.insert(key.to_vec(), log_record).unwrap();
        Ok(())
    }

    pub(crate) fn encode_key_seqno(key: Bytes, seq_no: usize) -> Vec<u8> {
        let mut encode_key = BytesMut::new();
        encode_length_delimiter(seq_no, &mut encode_key).unwrap();
        encode_key.extend_from_slice(&key);
        encode_key.to_vec()
    }

    // 批量原子提交
    pub fn commit(&self) -> Result<()> {
        let mut guard = self.pending_data.lock();
        if guard.len() == 0 {
            return Ok(());
        }
        if guard.len() > self.options.batch_max_rows as usize {
            return Err(Errors::ExceedBatchMaxRows);
        }
        // 保证串行化提交
        let _lock = self.engine.batch_commit_lock.lock();
        let mut pos_map = HashMap::new();
        // 维护全局seq_no
        self.engine.seq_no.fetch_add(1, Ordering::SeqCst);
        for (_, item) in guard.iter() {
            let mut log_record = LogRecord {
                key: WriteBatch::encode_key_seqno(
                    Bytes::from(item.key.clone()),
                    self.engine.seq_no.load(Ordering::SeqCst),
                ),
                value: item.value.clone(),
                log_type: item.log_type,
            };
            // 将每一条记录进行写盘
            let pos = self.engine.append_log(&mut log_record).unwrap();
            // 现在还不能更新到索引当中，要保证全部写盘成功后才能算成功
            pos_map.insert(item.key.clone(), pos);
        }
        // 最后添加标记,记录我们的事务完成标记
        let mut log_record = LogRecord {
            key: WriteBatch::encode_key_seqno(
                Bytes::from(TXN_FIN),
                self.engine.seq_no.load(Ordering::SeqCst),
            ),
            value: Default::default(),
            log_type: TXNCOMMITTED,
        };
        self.engine.append_log(&mut log_record).unwrap();
        // 写入完成后，加载到索引当中来
        for (_, item) in guard.iter() {
            let pos = pos_map.get(&item.key).unwrap();
            if item.log_type == LogRecordType::NORMAL {
                self.engine.indexer.put(item.key.to_vec(), *pos);
                continue;
            }

            if item.log_type == LogRecordType::DELETED {
                self.engine.indexer.delete(item.key.to_vec());
                continue;
            }
        }
        guard.clear();
        Ok(())
    }
}

#[cfg(test)]
mod write_batch_test {
    use std::path::PathBuf;

    use crate::{db::Engine, options::Options, write_batch::*};

    #[test]
    fn test_write_bacth() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/test_write_batch");
        opts.file_size_threshlod = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        let res1 = engine.new_write_batch(WriteBatchOptions::default());
        assert!(res1.is_ok());
        let write_batch = res1.unwrap();
        // 没有提交是看不到的
        write_batch
            .put(Bytes::from("key"), Bytes::from("value"))
            .unwrap();
        let res2 = engine.get(Bytes::from("key"));
        assert!(res2.is_err());
        // 提交后可以看到
        write_batch.commit().unwrap();
        let res3 = engine.get(Bytes::from("key"));
        assert!(res3.is_ok());
        engine.close().unwrap();
        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let res4 = engine2.get(Bytes::from("key"));
        assert!(res4.is_ok());
        assert_eq!(res4.unwrap(), Bytes::from("value"));
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
