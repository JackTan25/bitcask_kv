use std::{collections::BTreeMap, sync::Arc};

use super::{IndexIterator, IndexIteratorOptions, Indexer};
use crate::data::log_record::LogRecordPos;
use crate::errors::*;
use bytes::Bytes;
use parking_lot::RwLock;
// BtreeMap本身是并发不安全的，因此我们需要加锁
pub struct Btree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl Indexer for Btree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        // 拿到写锁
        let mut write_guard = self.tree.write();
        // insert如果已经有这个key了，就会把老的old_value返回,然后替换掉
        // 如果原本没有这个key,就直接插入kv，然后返回None
        write_guard.insert(key, pos);
        // 这里是直接返回true的，后面会根据读到的是旧数据来
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        // 拿到读锁
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        // 查看remove结果情况怎么样
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }

    fn iterator(&mut self, options: IndexIteratorOptions) -> Box<dyn IndexIterator> {
        // 获取读锁
        let read_guard = self.tree.read();
        let mut items = Vec::with_capacity(read_guard.len());
        for (key, log_record_pos) in read_guard.iter() {
            items.push((key.clone(), log_record_pos.clone()));
        }
        if options.reverse {
            items.reverse()
        }
        Box::new(BtreeIndexIterator {
            current_idx: 0,
            items: items,
            options: options,
        })
    }

    fn list_keys(&self) -> Result<Vec<Bytes>> {
        // 获取读锁
        let read_guard = self.tree.read();
        let mut res: Vec<Bytes> = Vec::new();
        for item in read_guard.iter() {
            res.push(Bytes::copy_from_slice(item.0));
        }
        Ok(res)
    }
}

impl Btree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

pub struct BtreeIndexIterator {
    // 迭代器当前读到哪里了
    current_idx: usize,
    // key和record位置信息
    items: Vec<(Vec<u8>, LogRecordPos)>,
    // 配置项,决定怎么读
    options: IndexIteratorOptions,
}

// 迭代器在生成的时候会根据options里面指定的顺序来做个排序
impl IndexIterator for BtreeIndexIterator {
    fn seek(&mut self, key: &Vec<u8>) {
        self.current_idx = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                return x.cmp(&key).reverse();
            } else {
                return x.cmp(&key);
            }
        }) {
            Ok(idx) => idx,
            Err(insert_idx) => insert_idx,
        }
    }

    fn rewind(&mut self) {
        self.current_idx = 0
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.current_idx >= self.items.len() {
            return None;
        }
        while let Some(item) = self.items.get(self.current_idx) {
            self.current_idx += 1;
            let key = &item.0;
            if key.starts_with(&self.options.prefix) {
                return Some((&item.0, &item.1));
            }
            if self.current_idx >= self.items.len() {
                return None;
            }
        }
        return None;
    }
}

// 添加test
#[cfg(test)]
mod test_btree {
    use super::*;

    #[test]
    fn test_btree_put() {
        let btree = Btree::new();
        let flag = btree.put(
            "key1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 10,
            },
        );
        assert_eq!(flag, true);
        let flag = btree.put(
            "key2".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 20,
            },
        );
        assert_eq!(flag, true);
    }

    #[test]
    fn test_btree_get() {
        // put
        let btree = Btree::new();
        let flag = btree.put(
            "key1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 10,
            },
        );
        assert_eq!(flag, true);
        let flag = btree.put(
            "key2".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 20,
            },
        );
        assert_eq!(flag, true);

        // get
        let log = btree.get("key1".as_bytes().to_vec());
        assert!(log.is_some());
        assert_eq!(log.unwrap().file_id, 0);
        assert_eq!(log.unwrap().offset, 10);

        let log = btree.get("key2".as_bytes().to_vec());
        assert!(log.is_some());
        assert_eq!(log.unwrap().file_id, 0);
        assert_eq!(log.unwrap().offset, 20);

        let log = btree.get("key3".as_bytes().to_vec());
        assert!(log.is_none());
    }
    #[test]
    fn test_btree_detele() {
        // put
        let btree = Btree::new();
        let flag = btree.put(
            "key1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 10,
            },
        );
        assert_eq!(flag, true);
        let flag = btree.put(
            "key2".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 20,
            },
        );
        assert_eq!(flag, true);

        // get
        let log = btree.get("key1".as_bytes().to_vec());
        assert!(log.is_some());
        assert_eq!(log.unwrap().file_id, 0);
        assert_eq!(log.unwrap().offset, 10);

        let log = btree.get("key2".as_bytes().to_vec());
        assert!(log.is_some());
        assert_eq!(log.unwrap().file_id, 0);
        assert_eq!(log.unwrap().offset, 20);

        // delete
        let flag = btree.delete("key1".as_bytes().to_vec());
        assert_eq!(flag, true);
        let flag = btree.delete("key1".as_bytes().to_vec());
        assert_eq!(flag, false);
        let flag = btree.delete("key2".as_bytes().to_vec());
        assert_eq!(flag, true);
        let log = btree.get("key1".as_bytes().to_vec());
        assert!(log.is_none());
        let log = btree.get("key2".as_bytes().to_vec());
        assert!(log.is_none());
    }

    #[test]
    fn test_btree_iterator_seek_next_rewind() {
        // 对应空数据的情况
        let mut bt = Btree::new();
        let mut iter1 = bt.iterator(IndexIteratorOptions::default());
        iter1.seek(&"key1".as_bytes().to_vec());
        assert!(iter1.next().is_none());

        // 对于多条数据的情况
        bt.put(
            "bbc".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 0,
            },
        );
        bt.put(
            "bcc".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 0,
            },
        );
        bt.put(
            "cbb".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 0,
            },
        );

        // 1.定位到开头
        let mut iter2 = bt.iterator(IndexIteratorOptions::default());
        iter2.seek(&"a".as_bytes().to_vec());
        let mut res = iter2.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bbc".as_bytes().to_vec());
        res = iter2.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bcc".as_bytes().to_vec());
        res = iter2.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "cbb".as_bytes().to_vec());
        res = iter2.next();
        assert!(res.is_none());
        // 2.定位到结尾
        iter2.seek(&"key".as_bytes().to_vec());
        res = iter2.next();
        assert!(res.is_none());

        // 多条数据遍历带前缀
        let mut iter3 = bt.iterator(IndexIteratorOptions::NewOptions(
            false,
            "c".as_bytes().to_vec(),
        ));
        iter3.seek(&"a".as_bytes().to_vec());
        res = iter3.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "cbb".as_bytes().to_vec());
        res = iter3.next();
        assert!(res.is_none());

        // 多条数据反向遍历
        let mut iter4 = bt.iterator(IndexIteratorOptions::NewOptions(
            true,
            "".as_bytes().to_vec(),
        ));
        iter4.seek(&"c".as_bytes().to_vec());
        res = iter4.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bcc".as_bytes().to_vec());
        assert!(res.is_some());
        res = iter4.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bbc".as_bytes().to_vec());
        assert!(res.is_some());
        res = iter4.next();
        assert!(res.is_none());

        // 测试rewind
        iter4.rewind();
        res = iter4.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "cbb".as_bytes().to_vec());
        assert!(res.is_some());
        res = iter4.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bcc".as_bytes().to_vec());
        assert!(res.is_some());
        res = iter4.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bbc".as_bytes().to_vec());
        assert!(res.is_some());
        res = iter4.next();
        assert!(res.is_none());
    }
}
