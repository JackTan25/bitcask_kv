use std::{sync::Arc,collections::BTreeMap};

use parking_lot::RwLock;
use crate::data::log_record::LogRecordPos;

use super::Indexer;
// BtreeMap本身是并发不安全的，因此我们需要加锁
pub struct Btree{
    tree : Arc<RwLock<BTreeMap<Vec<u8>,LogRecordPos>>>,
}

impl Indexer for Btree{
    fn put(&self,key:Vec<u8>,pos:LogRecordPos) -> bool {
        // 拿到写锁
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self,key:Vec<u8>) -> Option<LogRecordPos> {
        // 拿到读锁
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self,key:Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        // 查看remove结果情况怎么样?
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }
}

impl Btree{
    pub fn new() -> Self{
        Self {
            tree:Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

// 添加test
#[cfg(test)]
mod test_btree{
    use super::*;

    #[test]
    fn test_btree_put(){
        let btree = Btree::new();
        let flag = btree.put("key1".as_bytes().to_vec(), LogRecordPos{
            file_id:0,
            offset:10,
        });
        assert_eq!(flag,true);
        let flag = btree.put("key2".as_bytes().to_vec(), LogRecordPos{
            file_id:0,
            offset:20,
        });
        assert_eq!(flag,true);
    }

    #[test]
    fn test_btree_get(){
        // put
        let btree = Btree::new();
        let flag = btree.put("key1".as_bytes().to_vec(), LogRecordPos{
            file_id:0,
            offset:10,
        });
        assert_eq!(flag,true);
        let flag = btree.put("key2".as_bytes().to_vec(), LogRecordPos{
            file_id:0,
            offset:20,
        });
        assert_eq!(flag,true);

        // get
        let log = btree.get("key1".as_bytes().to_vec());
        assert!(log.is_some());
        let log = btree.get("key2".as_bytes().to_vec());
        assert!(log.is_some());
        let log = btree.get("key3".as_bytes().to_vec());
        assert!(log.is_none());
    }
    #[test]
    fn test_btree_detele(){
        // put
        let btree = Btree::new();
        let flag = btree.put("key1".as_bytes().to_vec(), LogRecordPos{
            file_id:0,
            offset:10,
        });
        assert_eq!(flag,true);
        let flag = btree.put("key2".as_bytes().to_vec(), LogRecordPos{
            file_id:0,
            offset:20,
        });
        assert_eq!(flag,true);

        // get
        let log = btree.get("key1".as_bytes().to_vec());
        assert!(log.is_some());
        let log = btree.get("key2".as_bytes().to_vec());
        assert!(log.is_some());
        let log = btree.get("key3".as_bytes().to_vec());
        assert!(log.is_none());

        // delete 
        let flag = btree.delete("key1".as_bytes().to_vec());
        assert_eq!(flag,true);
        let flag = btree.delete("key1".as_bytes().to_vec());
        assert_eq!(flag,false);
        let flag = btree.delete("key2".as_bytes().to_vec());
        assert_eq!(flag,true);
        let log = btree.get("key1".as_bytes().to_vec());
        assert!(log.is_none());
        let log = btree.get("key2".as_bytes().to_vec());
        assert!(log.is_none());
    }
}