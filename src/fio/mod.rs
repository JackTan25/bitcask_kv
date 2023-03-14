mod file_io;
use std::path::PathBuf;

use crate::errors::Result;

use self::file_io::FileIO;
// Sync 和 Send保证并发安全
// trait的方法的可见性和trait一样，比如下面的方法就全是
// pub,同时trait不能有普通字段,只能有,关联类型
// 参见iterator的type Item
pub trait IOManager: Sync + Send {
    // 从文件指定位置读取数据到buf,buf有多长就读多少，尽量读满
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    // 将buf的数据写到文件当中
    fn write(&self, buf: &[u8]) -> Result<usize>;
    // 持久化数据
    fn sync(&self) -> Result<()>;
}

pub fn new_io_manager(file_name: &PathBuf) -> Result<impl IOManager> {
    FileIO::new(file_name)
}
