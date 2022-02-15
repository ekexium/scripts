pub mod error;
pub mod workload;

pub type Result<T> = std::result::Result<T, error::MyError>;
