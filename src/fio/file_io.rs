use super::IOManager;
use crate::errors::{Errors, Result};
use log::error;
use parking_lot::RwLock;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::prelude::FileExt,
    path::PathBuf,
    sync::Arc,
};

pub struct FileIO {
    file: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file_name: &PathBuf) -> Result<Self> {
        // 添加创建文件的配置参数
        match OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .read(true)
            .open(file_name)
        {
            Ok(file) => Ok(FileIO {
                file: Arc::new(RwLock::new(file)),
            }),
            Err(err) => {
                error!("fail to new a file {}", err);
                Err(Errors::FailNewDataFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        // 获取读锁
        let read_guard = self.file.read();
        let err = read_guard.read_at(buf, offset);
        match err {
            Ok(read_size) => return Ok(read_size),
            Err(e) => {
                // 利用log包打印log信息
                error!("fail to read data from file,{}", e);
                return Err(Errors::FailReadFromFile);
            }
        };
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        // 获取写锁
        let mut write_guard = self.file.write();
        let write_res = write_guard.write(buf);
        match write_res {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("fail to write data to file {}", e);
                return Err(Errors::FailWriteDataToFile);
            }
        }
    }

    fn sync(&self) -> Result<()> {
        // 获取读锁
        let read_guard = self.file.read();
        if let Err(err) = read_guard.sync_all() {
            error!("fail to sync data to file {}", err);
            return Err(Errors::FailSyncDataToFile);
        }
        Ok(())
    }
}

// 使用 cargo --fmt
#[cfg(test)]
mod test_file_io {
    use std::{fs, path::PathBuf};

    use super::*;
    #[test]
    fn test_write() {
        let path = PathBuf::from("/tmp/a.data");
        let file_io = FileIO::new(&path);
        assert!(file_io.is_ok());
        let fd = file_io.unwrap();

        let write_res = fd.write("key1".as_bytes());
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 4);

        let write_res2 = fd.write("key2".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 4);

        // remove file
        let remove_res = fs::remove_file(&path);
        assert!(remove_res.is_ok());
    }

    #[test]
    fn test_read() {
        let path = PathBuf::from("/tmp/b.data");
        let file_io = FileIO::new(&path);
        assert!(file_io.is_ok());
        let fd = file_io.unwrap();

        let write_res = fd.write("key1".as_bytes());
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 4);

        let write_res2 = fd.write("key2".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 4);

        // read
        let mut buf = [0u8; 5];
        let read_res = fd.read(&mut buf, 0);
        assert!(read_res.is_ok());
        assert_eq!(read_res.unwrap(), 5);

        let read_res2 = fd.read(&mut buf, 5);
        assert!(read_res2.is_ok());
        assert_eq!(read_res2.unwrap(), 3);

        // remove file
        let remove_res = fs::remove_file(&path);
        assert!(remove_res.is_ok())
    }

    #[test]
    fn test_sync() {
        let path = PathBuf::from("/tmp/c.data");
        let file_io = FileIO::new(&path);
        assert!(file_io.is_ok());
        let fd = file_io.unwrap();

        let write_res = fd.write("key1".as_bytes());
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 4);

        let write_res2 = fd.write("key2".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 4);

        let sync_res = fd.sync();
        assert!(sync_res.is_ok());

        // remove file
        let remove_res = fs::remove_file(&path);
        assert!(remove_res.is_ok());
    }
}
