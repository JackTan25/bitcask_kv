mod btree;
// use crate::data::log_record::LogRecordPos;
use crate::data::log_record::LogRecordPos;
pub trait Indexer {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    fn delete(&self, key: Vec<u8>) -> bool;
}
