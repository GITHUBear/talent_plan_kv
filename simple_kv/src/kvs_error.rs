use failure::Fail;
use std::io;
use std::result;
use std::string::FromUtf8Error;

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
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Parse Engine Name Fail
    #[fail(display = "Parse Engine Name Fail")]
    ParseEngineNameErr,
    /// Server Err Message to Client
    #[fail(display = "{}", _0)]
    StringErr(String),
    /// Sled error
    #[fail(display = "sled error: {}", _0)]
    SledErr(#[cause] sled::Error),
    /// Key or value is invalid UTF-8 sequence
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8Error(#[cause] FromUtf8Error),
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

/// Convert sled::Error to KvsError
impl From<sled::Error> for KvsError {
    fn from(sled_err: sled::Error) -> KvsError {
        KvsError::SledErr(sled_err)
    }
}

/// Convert FromUtf8Error to KvsError
impl From<FromUtf8Error> for KvsError {
    fn from(utf8err: FromUtf8Error) -> KvsError {
        KvsError::Utf8Error(utf8err)
    }
}

/// Define `Result<T>` for convenience
pub type Result<T> = result::Result<T, KvsError>;
