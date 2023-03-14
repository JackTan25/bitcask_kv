use prost::length_delimiter_len;

// 实现了Copy trait的对象一定实现了Clone
// 实现Clone的没有实现Copy的只能在堆上
// 而实现了Copy的必然实现了Clone，既可以
// 在堆上也可以在栈上也可以在栈上
// Clone会alloc内存，Copy不会，但是Copy
// 实现了Clone,所以也会有内存alloc
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    // 在当前项目包可见即可
    pub(crate) file_id: u32,
    // 在当前项目包可见即可
    pub(crate) offset: u64,
}

#[derive(PartialEq)]
pub enum LogRecordType {
    NORMAL = 1,
    DELETED = 2,
}

impl LogRecordType {
    pub fn from_byte(recordType: u8) -> LogRecordType {
        match recordType {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            _ => panic!("unknown record type"),
        }
    }
}

// 定义日志存储结构
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) log_type: LogRecordType,
}

pub struct ReadLogRecord {
    pub(crate) logrecord: LogRecord,
    pub(crate) size: i64,
}

impl LogRecord {
    pub fn crc32(&self) -> u32 {
        todo!()
    }

    pub fn encode(&self) -> Vec<u8> {
        todo!()
    }

    // 获取logrecord的header长度的理论最大值
    pub fn max_logrecord_header() -> usize {
        std::mem::size_of::<u8>() + length_delimiter_len(std::mem::size_of::<u32>()) * 2
    }
}
