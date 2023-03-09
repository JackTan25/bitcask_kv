#[derive(Clone,Copy,Debug)]
pub struct LogRecordPos{
    // 在当前项目包可见即可
    pub(crate) file_id:u32,
    // 在当前项目包可见即可
    pub(crate) offset:u64,
}