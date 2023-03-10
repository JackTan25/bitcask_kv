use std::path::PathBuf;

pub struct Options{
    pub dir_path : PathBuf,
    pub file_size_threshlod:u64,
    pub sync :bool,
}
