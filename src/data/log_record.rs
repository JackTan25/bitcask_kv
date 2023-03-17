use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

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

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum LogRecordType {
    NORMAL = 1,
    DELETED = 2,
}

impl LogRecordType {
    pub fn from_byte(record_type: u8) -> LogRecordType {
        match record_type {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            _ => panic!("unknown record type"),
        }
    }
}
// 在磁盘上的存储方式是
/**
 * | Type |     KeySize      | ValueSize         |     Key     |     Value      |  Crc32  |
 *  1 byte  变长(最多5bytes)    变长(最多5bytes)    变长(真实key)  变长(真实value)   4 bytes
 */
// 定义日志存储结构
#[derive(Debug)]
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
        let (_, crc32) = self.encode_and_crc();
        return crc32;
    }

    pub fn encode(&self) -> Vec<u8> {
        let (encode, _) = self.encode_and_crc();
        return encode;
    }

    fn encode_and_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::new();
        // 预分配内存
        buf.reserve(self.encode_length());

        // 添加logRecord的type
        buf.put_u8(self.log_type as u8);

        // 添加keysize and valuesize
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        // 添加key and value
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // 添加crc32，使用的是crc32fast库
        let mut hash = crc32fast::Hasher::new();
        hash.update(&buf);
        let crc32 = hash.finalize();
        buf.put_u32(crc32);
        return (buf.to_vec(), crc32);
    }

    fn encode_length(&self) -> usize {
        std::mem::size_of::<u8>()   // type
        + length_delimiter_len(self.key.len()) // keysize
        + length_delimiter_len(self.value.len()) // valuesize
        + self.key.len() // key
        + self.value.len() // value
        + 4 // crc32
    }

    // 获取logrecord的header长度的理论最大值
    pub fn max_logrecord_header() -> usize {
        std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
    }
}

#[cfg(test)]
mod log_record_test {
    use super::{LogRecord, LogRecordType::*};

    #[test]
    fn test_encode_and_crc() {
        // 1.测试一条普通的LogRecord
        let log_record1 = LogRecord {
            key: "key1".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
            log_type: NORMAL,
        };
        let enc1 = log_record1.encode();
        assert!(enc1.len() > 5);
        assert_eq!(2820586739, log_record1.crc32());

        // 2.测试一条value为空的LogRecord
        let log_record2 = LogRecord {
            key: "key2".as_bytes().to_vec(),
            value: Default::default(),
            log_type: NORMAL,
        };
        let enc2 = log_record2.encode();
        assert!(enc2.len() > 5);
        assert_eq!(882605098, log_record2.crc32());

        // 3.测试一条类型为Delete的LogRecord
        let log_record3 = LogRecord {
            key: "key3".as_bytes().to_vec(),
            value: "value3".as_bytes().to_vec(),
            log_type: DELETED,
        };
        let enc3 = log_record3.encode();
        assert!(enc3.len() > 5);
        assert_eq!(1816502328, log_record3.crc32());
    }
}
