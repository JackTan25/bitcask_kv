use std::path::PathBuf;

use bitcask_kv::{db, options::*};
use bytes::Bytes;
fn main() {
    let options = Options::default();
    let engine = db::Engine::open(options).expect("open engine failed");
    let put_res = engine.put(Bytes::from("key1"), Bytes::from("value1"));
    assert!(put_res.is_ok());
    let get_res = engine.get(Bytes::from("key1"));
    assert!(get_res.is_ok());
    let put_res = engine.put(Bytes::from("key1"), Bytes::from("new value"));
    assert!(put_res.is_ok());
    let get_res = engine.get(Bytes::from("key1"));
    assert!(get_res.is_ok());
    println!("{:?}", get_res);
    assert_eq!(get_res.unwrap(), Bytes::from("new value"));
    engine.delete(Bytes::from("key1")).expect("delete failed");
    let get_res = engine.get(Bytes::from("key1"));
    assert!(get_res.is_err());
    engine.delete(Bytes::from("key1")).expect("delete failed");
    // std::fs::remove_dir_all(Options::default().dir_path).unwrap();
}
