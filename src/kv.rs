use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::path::PathBuf;
use std::result;

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
    /// The `Json` error is used to capture an error triggered by serde_json
    Json(serde_json::error::Error),
    /// The `KeyNotFound` is used when searching for a key in the database can't be found
    KeyNotFound(CustomError),
}

/// `Result` is a error helper for `KvError`
pub type Result<T> = result::Result<T, KvError>;

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvError::Io(ref err) => write!(f, "File Not Found: {}", err),
            KvError::Json(ref err) => write!(f, "Json Err: {}", err),
            KvError::KeyNotFound(ref _err) => write!(f, "Key not found"),
        }
    }
}

impl error::Error for KvError {
    fn description(&self) -> &str {
        match *self {
            KvError::Io(ref err) => err.description(),
            KvError::Json(ref err) => err.description(),
            // KvError::KeyNotFound(ref err) => err.description(),
            KvError::KeyNotFound(ref _err) => "Key not found",
        }
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            KvError::Io(ref err) => Some(err),
            KvError::Json(ref err) => Some(err),
            KvError::KeyNotFound(ref err) => Some(err),
        }
    }
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError::Io(err)
    }
}

impl From<serde_json::error::Error> for KvError {
    fn from(err: serde_json::error::Error) -> KvError {
        KvError::Json(err)
    }
}

#[derive(Default, Serialize, Deserialize)]
struct Command {
    key: String,
    value: Option<String>,
    remove: bool,
}

impl Command {
    pub fn insert(key: String, value: String) -> Command {
        Command {
            key,
            value: Some(value),
            remove: false,
        }
    }

    pub fn remove(key: String) -> Command {
        Command {
            key,
            value: None,
            remove: true,
        }
    }
}

impl std::string::ToString for Command {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
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
    map: HashMap<String, String>,
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
        let reader = BufReader::new(index_file);
        let mut map: HashMap<String, String> = HashMap::new();
        reader
            .lines()
            .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
            .for_each(|command: Command| {
                if command.remove {
                    map.remove(&command.key);
                } else {
                    map.insert(command.key, command.value.unwrap());
                }
            });
        Ok(KvStore {
            map,
            directory: path,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key.clone(), value.clone());
        let command = Command::insert(key, value);
        self.append_to_database(command)
        // create a value representing the `set` command, containing key and value
        // Serialize the `Command` to a String
        // Append serialized command to log file
        // return () if successful
        // return KvError on fail
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        if self.map.get(&key).is_none() {
            return Ok(None);
        }
        // Read entire log, one command at a time, record affected key and file
        // offset of the command to an in-memory key -> log pointer map
        // Checks the map for log pointer
        // If no log pointer found, throw `KeyNotFound` error
        // If success
        //   Deserialize the command to get the last recorded value of the key
        //   Print the value to stdout
        Ok(self.map.get(&key).cloned())
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        // convert to map_err()
        self.key_exists_check(&key)?;
        // Read entire log
        // Check the map if given key exists
        // If no log pointer found, throw `KeyNotFound` error
        let cmd = Command::remove(key.clone());
        self.map.remove(&key);
        self.append_to_database(cmd)
        // If success
        //   create a value representing the "rm" command, containing it's key
        //   append the serialized command to the log
        //   return (), exit
    }

    fn append_to_database(&self, command: Command) -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.directory.join(ACTIVE_FILE))
            .unwrap();
        writeln!(file, "{}", command.to_string())?;
        return Ok(());
    }

    fn key_exists_check(&self, key: &str) -> Result<()> {
        if self.map.get(key).is_none() {
            return Err(KvError::KeyNotFound(CustomError::new(
                "Key could not be found inside database",
            )));
        }
        Ok(())
    }
}
