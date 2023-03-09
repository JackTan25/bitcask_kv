// 实现了Copy trait的对象一定实现了Clone
// 实现Clone的没有实现Copy的只能在堆上
// 而实现了Copy的必然实现了Clone，既可以
// 在堆上也可以在栈上也可以在栈上
// Clone会alloc内存，Copy不会，但是Copy
// 实现了Clone,所以也会有内存alloc
#[derive(Clone,Copy,Debug)]
pub struct LogRecordPos{
    // 在当前项目包可见即可
    pub(crate) file_id:u32,
    // 在当前项目包可见即可
    pub(crate) offset:u64,
}