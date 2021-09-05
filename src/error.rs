use std::fmt;
use std::io;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::sync::TryLockError;
use std::{error, string::FromUtf8Error};

use crate::engines::kvs3::Storage;

/// Generic Error because right now i'm to lazy to implement an actually good
/// error class
#[derive(Debug)]
pub struct GenericError {
    details: String,
}

impl GenericError {
    /// Generate a new `GenericError` message
    pub fn new(msg: &str) -> GenericError {
        GenericError {
            details: msg.to_string(),
        }
    }
}

impl Into<GenericError> for &str {
    fn into(self) -> GenericError {
        GenericError {
            details: self.to_string(),
        }
    }
}

impl Into<GenericError> for String {
    fn into(self) -> GenericError {
        GenericError { details: self }
    }
}

impl fmt::Display for GenericError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl error::Error for GenericError {
    fn description(&self) -> &str {
        &self.details
    }
}

/// The `kvError` keeps track of the errors that the database runs into
#[derive(Debug)]
pub enum KvError {
    /// The `General` error is used when we don't know the specific error that was caused
    Io(io::Error),
    /// The `Serialize` error is used to capture an error triggered by serde_bincode
    Serialize(bincode::ErrorKind),
    /// The `Json` error is used to capture any issues had with dealing with json
    Json(serde_json::Error),
    /// The `KeyNotFound` is used when searching for a key in the database can't be found
    KeyNotFound(GenericError),
    /// The `UnexpectedCommandType` is used when the user issues a command we don't understand
    UnexpectedCommandType(GenericError),
    /// The `Parse` error is used to trigger an error when parsing database files
    Parse(GenericError),
    /// The `Utf8` error is used to throw an error when trying to get a value from our log file
    Utf8(FromUtf8Error),
    /// The `Compact` error is used when we fail to compact the active log
    Compact(GenericError),
    /// Sled error
    Sled(sled::Error),
    /// Poison read error
    Lock(GenericError),
    /// Error with a string message
    StringError(GenericError),
}

/// `Result` is a error helper for `KvError`
pub type Result<T> = std::result::Result<T, KvError>;

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KvError::Io(ref err) => write!(f, "File Not Found: {}", err),
            KvError::Serialize(ref err) => write!(f, "Bincode Err: {}", err),
            KvError::Json(ref err) => write!(f, "Json Err: {}", err),
            KvError::KeyNotFound(ref err) => write!(f, "KeyNotFound Err: {}", err),
            KvError::UnexpectedCommandType(ref err) => write!(f, "Command type Err: {}", err),
            KvError::Parse(ref err) => write!(f, "Prase Err: {}", err),
            KvError::Utf8(ref err) => write!(f, "Utf8 Err: {}", err),
            KvError::Compact(ref err) => write!(f, "Compact Err: {}", err),
            KvError::Sled(ref err) => write!(f, "Sled Err: {}", err),
            KvError::StringError(ref err) => write!(f, "String Error: {}", err),
            KvError::Lock(ref err) => write!(f, "Lock Error: {}", err),
        }
    }
}

impl error::Error for KvError {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            KvError::Io(ref err) => Some(err),
            KvError::Serialize(ref err) => Some(err),
            KvError::Json(ref err) => Some(err),
            KvError::KeyNotFound(ref err) => Some(err),
            KvError::UnexpectedCommandType(ref err) => Some(err),
            KvError::Parse(ref err) => Some(err),
            KvError::Utf8(ref err) => Some(err),
            KvError::Compact(ref err) => Some(err),
            KvError::Sled(ref err) => Some(err),
            KvError::StringError(ref err) => Some(err),
            KvError::Lock(ref err) => Some(err),
        }
    }
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError::Io(err)
    }
}

impl From<bincode::ErrorKind> for KvError {
    fn from(err: bincode::ErrorKind) -> KvError {
        KvError::Serialize(err)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> Self {
        KvError::Json(err)
    }
}

impl From<std::boxed::Box<bincode::ErrorKind>> for KvError {
    fn from(err: std::boxed::Box<bincode::ErrorKind>) -> KvError {
        KvError::Serialize(*err)
    }
}

impl From<FromUtf8Error> for KvError {
    fn from(err: FromUtf8Error) -> KvError {
        KvError::Utf8(err)
    }
}

impl From<sled::Error> for KvError {
    fn from(err: sled::Error) -> Self {
        KvError::Sled(err)
    }
}

impl From<TryLockError<RwLockReadGuard<'_, Vec<Storage>>>> for KvError {
    fn from(e: TryLockError<RwLockReadGuard<'_, Vec<Storage>>>) -> Self {
        KvError::Lock(format!("Read Lock Err: {}", e).into())
    }
}

impl From<TryLockError<RwLockWriteGuard<'_, Vec<Storage>>>> for KvError {
    fn from(e: TryLockError<RwLockWriteGuard<'_, Vec<Storage>>>) -> Self {
        KvError::Lock(format!("Write Lock Err: {}", e).into())
    }
}
