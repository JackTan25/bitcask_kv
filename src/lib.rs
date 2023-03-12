// 记得使用cargo fmt --all 来格式整个项目
mod data;
mod errors;
mod fio;
mod index;
// 这里使用pub是因为我们db是整个项目的
// 对外使用接口
pub mod db;
mod options;
