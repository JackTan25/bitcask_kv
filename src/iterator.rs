use std::sync::Arc;

use crate::errors::Result;
use crate::{
    db::Engine,
    index::{IndexIterator, IndexIteratorOptions},
};
use bytes::Bytes;
use parking_lot::RwLock;

impl Engine {
    pub fn iter(&mut self, options: IndexIteratorOptions) -> Iterator {
        Iterator {
            iter: Arc::new(RwLock::new(self.indexer.iterator(options))),
            engine: self,
        }
    }

    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.indexer.list_keys()
    }

    // 对所有kv数据执行操作,直到fn返回false,就
    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        Self: Sized,
        F: Fn(Bytes, Bytes) -> bool,
    {
        Ok(())
    }
}

// 定义engine层面给用户直接使用的iterator
pub struct Iterator<'a> {
    iter: Arc<RwLock<Box<dyn IndexIterator>>>,
    engine: &'a Engine,
}

impl Iterator<'_> {
    fn seek(&mut self, key: &Vec<u8>) {
        let mut write_guard = self.iter.write();
        write_guard.seek(key);
    }

    fn rewind(&mut self) {
        let mut write_guard = self.iter.write();
        write_guard.rewind();
    }

    fn next(&mut self) -> Option<(Bytes, Bytes)> {
        let mut write_guard = self.iter.write();
        let item = write_guard.next();
        if let Some((key, log_record_pos)) = item {
            let value = self
                .engine
                .get_value_by_pos(log_record_pos)
                .expect("fail to read data file");
            return Some((Bytes::from(key.to_vec()), value));
        }
        None
    }
}

#[cfg(test)]
mod test_engine_iterator {
    use std::path::PathBuf;

    use bytes::Bytes;

    use crate::db::Engine;
    use crate::index::IndexIteratorOptions;
    use crate::options::Options;
    #[test]
    fn test_engine_iterator() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/iterator");
        //对于空数据的情况
        let mut engine = Engine::open(opts.clone()).unwrap();
        let mut iter1 = engine.iter(IndexIteratorOptions::default());
        iter1.seek(&"key1".as_bytes().to_vec());
        assert!(iter1.next().is_none());

        //对于多条数据的情况
        let mut res = engine.put(Bytes::from("bbc"), Bytes::from("value1"));
        assert!(res.is_ok());
        res = engine.put(Bytes::from("bcc"), Bytes::from("value2"));
        assert!(res.is_ok());
        res = engine.put(Bytes::from("cbb"), Bytes::from("value3"));
        assert!(res.is_ok());

        // 1.定位到开头
        let mut iter2 = engine.iter(IndexIteratorOptions::default());
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
        let mut iter3 = engine.iter(IndexIteratorOptions::NewOptions(
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
        let mut iter4 = engine.iter(IndexIteratorOptions::NewOptions(
            true,
            "".as_bytes().to_vec(),
        ));
        iter4.seek(&"c".as_bytes().to_vec());
        res = iter4.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bcc".as_bytes().to_vec());
        res = iter4.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bbc".as_bytes().to_vec());
        res = iter4.next();
        assert!(res.is_none());

        // 多条数据反向遍历
        let mut iter4 = engine.iter(IndexIteratorOptions::NewOptions(
            true,
            "".as_bytes().to_vec(),
        ));
        iter4.seek(&"c".as_bytes().to_vec());
        res = iter4.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bcc".as_bytes().to_vec());
        res = iter4.next();
        assert!(res.is_some());
        assert_eq!(*res.unwrap().0, "bbc".as_bytes().to_vec());
        res = iter4.next();
        assert!(res.is_none());
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_list_keys() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/listkeys");
        let engine = Engine::open(opts.clone()).unwrap();
        let mut keys = engine.list_keys().unwrap();
        assert_eq!(keys.len(), 0);
        //对于多条数据的情况
        let mut res = engine.put(Bytes::from("bbc"), Bytes::from("value1"));
        assert!(res.is_ok());
        res = engine.put(Bytes::from("bcc"), Bytes::from("value2"));
        assert!(res.is_ok());
        res = engine.put(Bytes::from("cbb"), Bytes::from("value3"));
        assert!(res.is_ok());
        keys = engine.list_keys().unwrap();
        assert_eq!(keys.len(), 3);
        let mut idx = 0;
        for key in keys.iter() {
            match idx {
                0 => assert_eq!(*key, Bytes::from("bbc")),
                1 => assert_eq!(*key, Bytes::from("bcc")),
                2 => assert_eq!(*key, Bytes::from("cbb")),
                _ => (),
            }
            idx += 1;
        }
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_fold() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/fold");
        let engine = Engine::open(opts.clone()).unwrap();
        let keys = engine.list_keys().unwrap();
        assert_eq!(keys.len(), 0);
        //对于多条数据的情况
        let mut res = engine.put(Bytes::from("bbc"), Bytes::from("value1"));
        assert!(res.is_ok());
        res = engine.put(Bytes::from("bcc"), Bytes::from("value2"));
        assert!(res.is_ok());
        res = engine.put(Bytes::from("cbb"), Bytes::from("value3"));
        assert!(res.is_ok());

        let res = engine.fold(|key, value| -> bool {
            // 应该输出bbc,bcc
            if key.ge("cbb") {
                return false;
            }
            println!("key:{:?},value:{:?}", key, value);
            true
        });
        assert!(res.is_ok());
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
