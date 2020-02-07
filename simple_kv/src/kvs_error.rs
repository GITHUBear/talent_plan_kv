use failure::Fail;
use std::io;
use std::result;

/// Use `Fail` to define the error of Kvs
#[derive(Debug, Fail)]
pub enum KvsError {
    /// IO Error
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Serde Error
    #[fail(display = "{}", _0)]
    SerdeError(#[cause] serde_json::Error),
    /// Undefined Command Error
    #[fail(display = "Undefined Command Error")]
    UndefCmdline,
    /// Key Not Found
    #[fail(display = "Key Not Found")]
    KeyNotFound,
}

/// Convert io::Error to KvsError
impl From<io::Error> for KvsError {
    fn from(io_err: io::Error) -> KvsError {
        KvsError::Io(io_err)
    }
}

/// Convert serde_json::Error to KvsError
impl From<serde_json::Error> for KvsError {
    fn from(serde_err: serde_json::Error) -> KvsError {
        KvsError::SerdeError(serde_err)
    }
}

/// Define `Result<T>` for convenience
pub type Result<T> = result::Result<T, KvsError>;