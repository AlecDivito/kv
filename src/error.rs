use std::error;
use std::fmt;

#[derive(Debug)]
pub struct GenericError {
    details: String,
}

impl GenericError {
    fn new(msg: &str) -> GenericError {
        GenericError {
            details: msg.to_string(),
        }
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
    /// Error with a string message
    StringError(String),
}

/// `Result` is a error helper for `KvError`
pub type Result<T> = result::Result<T, KvError>;

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvError::Io(ref err) => write!(f, "File Not Found: {}", err),
            KvError::Serialize(ref err) => write!(f, "Json Err: {}", err),
            KvError::KeyNotFound(ref err) => write!(f, "KeyNotFound Err: {}", err),
            KvError::Parse(ref err) => write!(f, "Prase Err: {}", err),
            KvError::Utf8(ref err) => write!(f, "Utf8 Err: {}", err),
            KvError::Compact(ref err) => write!(f, "Compact Err: {}", err),
        }
    }
}

impl error::Error for KvError {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            KvError::Io(ref err) => Some(err),
            KvError::Serialize(ref err) => Some(err),
            KvError::KeyNotFound(ref err) => Some(err),
            KvError::Parse(ref err) => Some(err),
            KvError::Utf8(ref err) => Some(err),
            KvError::Compact(ref err) => Some(err),
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
