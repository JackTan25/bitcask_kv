mod btree;
// use crate::data::log_record::LogRecordPos;
use crate::{data::log_record::LogRecordPos, options::IndexType};
pub(crate) trait Indexer {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    fn delete(&self, key: Vec<u8>) -> bool;
}

pub(crate) fn NewIndexer(index_type: IndexType) -> Box<dyn Indexer> {
    match index_type {
        IndexType::Btree => Box::new(btree::Btree::new()),
        IndexType::SkipList => todo!(),
        _ => panic!("unknown index type"),
    }
}
