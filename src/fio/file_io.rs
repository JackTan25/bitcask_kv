use std::{fs::File, sync::Arc, os::unix::prelude::FileExt, io::Write};
use crate::errors::{Result,Errors};
use log::error;
use parking_lot::RwLock;
use super::IOManager;

pub struct FileIO{
    file : Arc<RwLock<File>>,
}

impl IOManager for FileIO{
    fn read(&self,buf:&mut [u8],offset:u64) ->Result<usize> {
        // 获取读锁
        let read_guard = self.file.read();
        let err = read_guard.read_at(buf, offset);
        match err{
            Ok(read_size) => return Ok(read_size),
            Err(e) =>{
                // 利用log包打印log信息
                error!("fail to read data from file,{}",e);
                return Err(Errors::FailReadFromFile)
            }
        };
    }

    fn write(&self,buf:&[u8]) -> Result<usize> {
        // 获取写锁
        let mut write_guard = self.file.write();
        let write_res = write_guard.write(buf);
        match write_res{
            Ok(n)=> return Ok(n),
            Err(e) => {
                error!("fail to write data to file {}",e);
                return Err(Errors::FailWriteDataToFile)
            }
        }
    }

    fn sync(&self) -> Result<()> {
        // 获取读锁
        let read_guard = self.file.read();
        if let Err(err) = read_guard.sync_all(){
            error!("fail to sync data to file {}",err);
            return Err(Errors::FailSyncDataToFile)
        }
        Ok(())
    }
}