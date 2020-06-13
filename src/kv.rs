use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::path::PathBuf;
use std::{result, string::FromUtf8Error};

#[derive(Debug)]
pub struct CustomError {
    details: String,
}

impl CustomError {
    fn new(msg: &str) -> CustomError {
        CustomError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl error::Error for CustomError {
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
    KeyNotFound(CustomError),
    /// The `Parse` error is used to trigger an error when parsing database files
    Parse(CustomError),
    /// The `Decrypt` error is used to throw an error when trying to get a value from our log file
    Decrypt(FromUtf8Error),
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
            KvError::Decrypt(ref err) => write!(f, "Decrypt Err: {}", err),
        }
    }
}

impl error::Error for KvError {
    fn description(&self) -> &str {
        match *self {
            KvError::Io(ref err) => err.description(),
            KvError::Serialize(ref err) => err.description(),
            KvError::KeyNotFound(ref err) => err.description(),
            KvError::Parse(ref err) => err.description(),
            KvError::Decrypt(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            KvError::Io(ref err) => Some(err),
            KvError::Serialize(ref err) => Some(err),
            KvError::KeyNotFound(ref err) => Some(err),
            KvError::Parse(ref err) => Some(err),
            KvError::Decrypt(ref err) => Some(err),
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
        KvError::Decrypt(err)
    }
}

#[derive(Default, Deserialize, Serialize, Debug)]
struct Command<'a> {
    value: Option<&'a str>,
    key: &'a str,
}

impl<'a> Command<'a> {
    pub fn insert(key: &'a str, value: &'a str) -> Command<'a> {
        Command {
            key,
            value: Some(value),
        }
    }

    pub fn remove(key: &'a str) -> Command<'a> {
        Command { key, value: None }
    }

    pub fn is_remove(&self) -> bool {
        self.value.is_none()
    }

    pub fn get_value_length(&self) -> u64 {
        self.value.unwrap().chars().count() as u64
    }
}

#[derive(Debug)]
struct LogPointer {
    value_length: u64,
    value_position: u64,
}

const BINCODE_STRING_LENGTH_OFFSET: u64 = 8;
const BINCODE_STRING_OPTION_OFFSET: u64 = 1;
const BINCODE_STRING_OFFSET: u64 = BINCODE_STRING_LENGTH_OFFSET + BINCODE_STRING_OPTION_OFFSET;

impl LogPointer {
    pub fn write(offset: u64, command: &Command) -> LogPointer {
        let value_length = command.get_value_length();
        let value_position = offset + BINCODE_STRING_OFFSET;
        LogPointer {
            value_length,
            value_position,
        }
    }
}

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk
///
/// Example:
///
/// ```rust
/// # use kvs::{KvStore, KvError, Result};
/// # fn main() -> Result<()> {
/// let mut store = KvStore::open("")?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct KvStore {
    directory: PathBuf,
    map: HashMap<String, LogPointer>,
}

const ACTIVE_FILE: &'static str = "index.database";

impl KvStore {
    /// Create a `kvStore`
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
            directory: PathBuf::new().join(".database"),
        }
    }

    /// Build a `kvStore` from a database folder
    pub fn open(folder: impl Into<PathBuf>) -> Result<KvStore> {
        let path = folder.into();
        let index_path = Path::new(&path).join(ACTIVE_FILE);
        let index_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(index_path)?;

        // https://doc.rust-lang.org/beta/std/io/trait.Read.html#method.chain
        let index_file_length = index_file.metadata().unwrap().len();
        let mut reader = BufReader::new(index_file);
        let mut map = HashMap::new();
        let mut length_buffer = [0; 8];
        while reader.seek(SeekFrom::Current(0)).unwrap() < index_file_length {
            // read length of command
            reader.read_exact(&mut length_buffer)?;
            let offset = reader.seek(SeekFrom::Current(0)).unwrap();
            let bytes_to_read: u64 = unsafe { std::mem::transmute(length_buffer) };
            let mut command_buffer: Vec<u8> = vec![0; bytes_to_read as usize];
            // read command
            reader.read_exact(&mut command_buffer)?;
            let command: Command = bincode::deserialize(&command_buffer)?;
            // save command to map
            if command.is_remove() {
                map.remove(command.key);
            } else {
                map.insert(
                    String::from(command.key),
                    LogPointer::write(offset, &command),
                );
            }
        }

        Ok(KvStore {
            map,
            directory: path,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // create a value representing the `set` command, containing key and value
        let command = Command::insert(&key, &value);

        // Serialize the `Command` to a String
        let command_buffer = bincode::serialize(&command)?;
        let command_buffer_length_buffer =
            unsafe { std::mem::transmute::<usize, [u8; 8]>(command_buffer.len()).to_vec() };

        // Append serialized command to log file
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(self.directory.join(ACTIVE_FILE))?;
        let offset = file.metadata().unwrap().len() + 8;
        file.write(&command_buffer_length_buffer)?;
        file.write(&command_buffer)?;
        file.flush()?;

        // Add command to hashmap as log pointer
        self.map
            .insert(key.clone(), LogPointer::write(offset, &command));
        // return () if successful
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        // Checks the map for log pointer
        let log_pointer = match self.map.get(&key) {
            Some(v) => v,
            // If no log pointer found, throw `KeyNotFound` error
            None => return Ok(None),
        };
        // If success
        //   Find the value from the file
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(self.directory.join(ACTIVE_FILE))?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(log_pointer.value_position))?;
        let mut handle = reader.take(log_pointer.value_length);
        let mut buffer = vec![0; log_pointer.value_length as usize];
        handle.read(&mut buffer[..])?;
        //   Deserialize the command to get the last recorded value of the key
        Ok(Some(String::from_utf8(buffer)?))
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        // Checks the map for log pointer
        // If no log pointer found, throw `KeyNotFound` error
        match self.map.get(&key) {
            Some(v) => v,
            // If no log pointer found, throw `KeyNotFound` error
            None => {
                return Err(KvError::KeyNotFound(CustomError::new(
                    "Key could not be found inside database",
                )))
            }
        };
        // If success
        //   create a value representing the "rm" command, containing it's key
        let command = Command::remove(&key);

        //   append the serialized command to the log
        let mut file = self.get_index_file()?;
        let command_buffer = bincode::serialize(&command)?;
        let command_buffer_length_buffer =
            unsafe { std::mem::transmute::<usize, [u8; 8]>(command_buffer.len()).to_vec() };
        file.write(&command_buffer_length_buffer)?;
        file.write(&command_buffer)?;
        file.flush()?;
        self.map.remove(&key);

        //   return (), exit
        Ok(())
    }

    fn get_index_file(&self) -> Result<File> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(self.directory.join(ACTIVE_FILE))?;
        Ok(file)
    }
}
