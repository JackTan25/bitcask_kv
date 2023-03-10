pub(crate) mod log_record;
pub(crate) mod data_file;
/*
    可见性规则
    1.如果我们的LogRecordPos是pub(crate),那么就说明
    它只能对于当前的包data是可见,也就是data下的文
    件都可见
    2.但是由于我们的log_record也是pub(crate),这
    就说明我们的log_record对data的上级包也是可见
    的,也就是根包(crate)
    3.这样在btree里面可以直接使用LogRecordPos,但
    是LogRecordPos本身也必须至少是pub(crate)的,也
    可以是pub,因为Indexer这个Trait自身就是
    pub(crate)的,那么他的方法的参数也都得至少是Pub(crate)，
    但是如果Indexer这个Trait本身是pub(crate)的，那么他的
    方法的参数也都得是Pub，那么LogRecordPos就必须是Pub的了
*/