use thiserror::Error;

#[derive(Error, Debug)]
pub enum MyError {
    #[error("{0}")]
    StringError(String),
    #[error("sqlx error: {sqlx:?}")]
    SqlxError {
        #[from]
        sqlx: sqlx::Error,
    },
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Rng(#[from] rand::Error),
}
