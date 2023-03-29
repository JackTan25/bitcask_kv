mod btree;
use crate::errors::Result;
use bytes::Bytes;
// use crate::data::log_record::LogRecordPos;
use crate::{data::log_record::LogRecordPos, options::IndexType};
pub(crate) trait Indexer: Send + Sync {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    fn delete(&self, key: Vec<u8>) -> bool;
    fn iterator(&mut self, options: IndexIteratorOptions) -> Box<dyn IndexIterator>;
    fn list_keys(&self) -> Result<Vec<Bytes>>;
}

pub(crate) fn NewIndexer(index_type: IndexType) -> Box<dyn Indexer> {
    match index_type {
        IndexType::Btree => Box::new(btree::Btree::new()),
        IndexType::SkipList => todo!(),
    }
}

pub(crate) trait IndexIterator: Sync + Send {
    fn seek(&mut self, key: &Vec<u8>);

    fn rewind(&mut self);

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}

// 添加配置项，用于指定迭代器的查询方案
pub struct IndexIteratorOptions {
    // 指定是否由大到小来查
    reverse: bool,
    // 指定查询的key的前缀
    prefix: Vec<u8>,
}

// 实现默认配置
impl Default for IndexIteratorOptions {
    fn default() -> Self {
        Self {
            reverse: false,
            prefix: Default::default(),
        }
    }
}

impl IndexIteratorOptions {
    pub(crate) fn NewOptions(flag: bool, prefix: Vec<u8>) -> IndexIteratorOptions {
        IndexIteratorOptions {
            prefix: prefix,
            reverse: flag,
        }
    }
}
