use crate::engines::KvsEngine;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error;
use std::fmt::{self};
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
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
    /// The `Compact` error is used when we fail to compact the active log
    Compact(CustomError),
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
            KvError::Decrypt(ref err) => Some(err),
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
        KvError::Decrypt(err)
    }
}

#[derive(Default, Deserialize, Serialize, Debug)]
struct Command<'a> {
    value: Option<&'a str>,
    key: &'a str,
    timestamp: u128,
}

impl<'a> Command<'a> {
    pub fn insert(key: &'a str, value: &'a str, timestamp: u128) -> Command<'a> {
        Command {
            key,
            value: Some(value),
            timestamp,
        }
    }

    pub fn remove(key: &'a str, timestamp: u128) -> Command<'a> {
        Command {
            key,
            value: None,
            timestamp,
        }
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
    file_path: PathBuf,
    value_length: u64,
    value_position: u64,
    timestamp: u128,
}

const BINCODE_STRING_LENGTH_OFFSET: u64 = 8;
const BINCODE_STRING_OPTION_OFFSET: u64 = 1;
const BINCODE_STRING_OFFSET: u64 = BINCODE_STRING_LENGTH_OFFSET + BINCODE_STRING_OPTION_OFFSET;

impl LogPointer {
    pub fn write(file_path: PathBuf, offset: u64, command: &Command) -> LogPointer {
        let value_length = command.get_value_length();
        let value_position = offset + BINCODE_STRING_OFFSET;
        LogPointer {
            file_path,
            value_length,
            value_position,
            timestamp: command.timestamp,
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
/// let mut store = KvStore::open("./")?;
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
    logs: HashSet<PathBuf>,
}

const FILE_SUFFIX: &'static str = ".database";
const ACTIVE_FILE: &'static str = "index.database";
const COMPACT_FILE: &'static str = "compact.database";
const MAX_LOG_FILE_SIZE: u64 = 64 * 1024;

impl KvStore {
    /// Create a `kvStore`
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
            directory: PathBuf::new().join(".database"),
            logs: HashSet::new(),
        }
    }

    /// Build a `kvStore` from a database folder
    pub fn open(folder: impl Into<PathBuf>) -> Result<KvStore> {
        let database_path = folder.into();

        let mut logs = HashSet::new();
        let mut map = HashMap::new();

        for entry in std::fs::read_dir(&database_path)? {
            let entry = entry?;
            let file_name = &entry.file_name().into_string().unwrap();
            if !file_name.ends_with(FILE_SUFFIX) {
                continue;
            }

            logs.insert(entry.path());
            let file = std::fs::OpenOptions::new().read(true).open(entry.path())?;
            let file_length = file.metadata().unwrap().len();
            let mut reader = BufReader::new(file);
            let mut command_length_buffer = [0; 8];
            // read
            while reader.seek(SeekFrom::Current(0)).unwrap() < file_length {
                reader.read_exact(&mut command_length_buffer)?;
                let offset = reader.seek(SeekFrom::Current(0)).unwrap();
                let command_length: u64 = u64::from_be_bytes(command_length_buffer);
                let mut command_buffer: Vec<u8> = vec![0; command_length as usize];
                reader.read_exact(&mut command_buffer)?;
                let command: Command = bincode::deserialize(&command_buffer)?;

                let pointer = map.get(command.key);
                if pointer.is_none() {
                    map.insert(
                        String::from(command.key),
                        LogPointer::write(entry.path(), offset, &command),
                    );
                } else {
                    let pointer = pointer.unwrap();
                    if pointer.timestamp < command.timestamp {
                        if command.is_remove() {
                            map.remove(command.key);
                        } else {
                            map.insert(
                                String::from(command.key),
                                LogPointer::write(entry.path(), offset, &command),
                            );
                        }
                    }
                }
            }
        }

        Ok(KvStore {
            map,
            directory: database_path,
            logs,
        })
    }

    fn get_index_file(&self) -> Result<File> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(self.directory.join(ACTIVE_FILE))?;
        Ok(file)
    }

    fn generate_log_file_name(&self) -> String {
        format!("log_{}.database", self.logs.len())
    }

    fn now(&self) -> u128 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    fn compact(&mut self) -> Result<()> {
        // create compact file
        let compact_path = Path::new(&self.directory).join(COMPACT_FILE);
        let mut compact_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&compact_path)?;

        for (key, _) in &self.map {
            let value = self.get(key.clone())?;
            if value.is_none() {
                return Err(KvError::Compact(CustomError::new(
                    "All keys must point to values",
                )));
            }
            let value = value.unwrap();
            let command = Command::insert(&key, &value, self.now());
            let command_buffer = bincode::serialize(&command)?;
            let command_buffer_length_buffer = u64::to_be_bytes(command_buffer.len() as u64);
            compact_file.write(&command_buffer_length_buffer)?;
            compact_file.write(&command_buffer)?;
        }
        compact_file.flush()?;

        let log_path = Path::new(&self.directory).join(self.generate_log_file_name());
        std::fs::rename(&compact_path, &log_path)?;
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            if entry.path().ne(&log_path) {
                std::fs::remove_file(&entry.path())?;
            }
        }
        self.logs.clear();
        self.logs.insert(log_path);

        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten
    fn set(&mut self, key: String, value: String) -> Result<()> {
        // create a value representing the `set` command, containing key and value
        let command = Command::insert(&key, &value, self.now());

        // Serialize the `Command` to a String
        let command_buffer = bincode::serialize(&command)?;
        let command_buffer_length_buffer = u64::to_be_bytes(command_buffer.len() as u64);

        // Append serialized command to log file
        let active_path = self.directory.join(ACTIVE_FILE);
        let mut file = if self.logs.insert(self.directory.join(ACTIVE_FILE)) {
            std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&active_path)?
        } else {
            std::fs::OpenOptions::new()
                .append(true)
                .open(&active_path)?
        };
        let offset = file.metadata().unwrap().len() + 8;
        file.write(&command_buffer_length_buffer)?;
        file.write(&command_buffer)?;
        file.flush()?;

        // Add command to hashmap as log pointer
        self.map.insert(
            key.clone(),
            LogPointer::write(active_path, offset, &command),
        );
        // return () if successful

        if offset > MAX_LOG_FILE_SIZE {
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
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
            .open(&log_pointer.file_path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(log_pointer.value_position))?;
        let mut handle = reader.take(log_pointer.value_length);
        let mut buffer = vec![0; log_pointer.value_length as usize];
        handle.read(&mut buffer[..])?;
        //   Deserialize the command to get the last recorded value of the key
        Ok(Some(String::from_utf8(buffer)?))
    }

    /// Remove a given key.
    fn remove(&mut self, key: String) -> Result<()> {
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
        let command = Command::remove(&key, self.now());

        //   append the serialized command to the log
        let mut file = self.get_index_file()?;
        let command_buffer = bincode::serialize(&command)?;
        let command_buffer_length_buffer = u64::to_be_bytes(command_buffer.len() as u64);
        file.write(&command_buffer_length_buffer)?;
        file.write(&command_buffer)?;
        file.flush()?;
        self.map.remove(&key);

        //   return (), exit
        Ok(())
    }
}
