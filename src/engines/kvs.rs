use super::Result;
use crate::{engines::KvsEngine, GenericError, KvError};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

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

#[derive(Debug, Clone)]
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

    pub fn rewrite(&mut self, file_path: PathBuf, offset: u64, command: &Command) {
        let value_length = command.get_value_length();
        let value_position = offset + BINCODE_STRING_OFFSET;
        self.file_path = file_path;
        self.value_length = value_length;
        self.value_position = value_position;
        self.timestamp = command.timestamp;
    }
}

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk
/// ```
#[derive(Default, Clone)]
pub struct KvStore {
    directory: PathBuf,
    map: Arc<HashMap<String, LogPointer>>,
    logs: Arc<HashSet<PathBuf>>,
}

const FILE_SUFFIX: &'static str = ".database";
const ACTIVE_FILE: &'static str = "index.database";
const COMPACT_FILE: &'static str = "compact.database";
const MAX_LOG_FILE_SIZE: u64 = 64 * 1024;

impl KvStore {
    /// Create a `kvStore`
    pub fn new() -> KvStore {
        KvStore {
            map: Arc::new(HashMap::new()),
            directory: PathBuf::new().join(".database"),
            logs: Arc::new(HashSet::new()),
        }
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

    fn compact(&mut self) -> Result<()> {
        // create compact file
        let compact_path = Path::new(&self.directory).join(COMPACT_FILE);
        let mut compact_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&compact_path)?;

        // Create the new log file name
        let new_log_path = Path::new(&self.directory).join(self.generate_log_file_name());
        let mut offset = 8; // keep the offset to the start of the next record

        // Create new map
        let mut map = HashMap::new();

        // loop over current keys and write them all to the compact file
        for key in self.map.keys() {
            // try and get the value from the database
            let value = self.get(key.clone())?;
            if value.is_none() {
                return Err(KvError::Compact(GenericError::new(
                    "All keys must point to values",
                )));
            }
            let value = value.unwrap();
            // create the command to insert into database
            let command = Command::insert(&key, &value, now());
            let command_buffer = bincode::serialize(&command)?;
            let command_buffer_length_buffer = u64::to_be_bytes(command_buffer.len() as u64);
            map.insert(
                key.clone(),
                LogPointer::write(new_log_path.clone(), offset, &command),
            );
            compact_file.write(&command_buffer_length_buffer)?;
            compact_file.write(&command_buffer)?;
            offset =
                offset + command_buffer.len() as u64 + command_buffer_length_buffer.len() as u64;
        }
        compact_file.flush()?;

        // rename the compact file into the new log file
        std::fs::rename(&compact_path, &new_log_path)?;

        // delete old files
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            if entry.path().ne(&new_log_path) {
                std::fs::remove_file(&entry.path())?;
            }
        }
        self.logs.clear();
        self.logs.insert(new_log_path);
        self.map = Arc::new(map);
        Ok(())
    }

    fn secret_get(&mut self, log_pointer: &mut LogPointer) -> Result<Option<String>> {
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
}

impl KvsEngine for KvStore {
    /// Build a `kvStore` from a database folder
    fn open(folder: impl Into<PathBuf>) -> Result<KvStore> {
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
            map: Arc::new(map),
            directory: database_path,
            logs: Arc::new(logs),
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten
    fn set(&self, key: String, value: String) -> Result<()> {
        // create a value representing the `set` command, containing key and value
        let command = Command::insert(&key, &value, now());

        // Serialize the `Command` to a String
        let command_buffer = bincode::serialize(&command)?;
        let command_buffer_length_buffer = u64::to_be_bytes(command_buffer.len() as u64);

        // Append serialized command to log file
        let active_path = self.directory.join(ACTIVE_FILE);
        // let mut file = if self.logs.insert(self.directory.join(ACTIVE_FILE)) {
        //     std::fs::OpenOptions::new()
        //         .append(true)
        //         .create(true)
        //         .open(&active_path)?
        // } else {
        //     std::fs::OpenOptions::new()
        //         .append(true)
        //         .open(&active_path)?
        // };
        // let offset = file.metadata().unwrap().len() + 8;
        // file.write(&command_buffer_length_buffer)?;
        // file.write(&command_buffer)?;
        // file.flush()?;

        // Add command to hashmap as log pointer
        // self.map.insert(
        //     key.clone(),
        //     LogPointer::write(active_path, offset, &command),
        // );
        // return () if successful

        // if offset > MAX_LOG_FILE_SIZE {
        // self.compact()?;
        // }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        let keyy = key.clone();
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
        let temp = buffer.clone();
        if let Err(_) = String::from_utf8(temp.clone()) {
            info!(
                "{}",
                format!(
                    "Can't convert key {} to value {:?} to utf8 with log pointer {:?}",
                    keyy, temp, log_pointer
                )
            );
        }
        Ok(Some(String::from_utf8(buffer).map_err(|_| {
            KvError::StringError(
                format!(
                    "Can't convert key {} to value {:?} to utf8 with log pointer {:?}",
                    keyy, temp, log_pointer
                )
                .into(),
            )
        })?))
    }

    /// Remove a given key.
    fn remove(&self, key: String) -> Result<()> {
        // Checks the map for log pointer
        // If no log pointer found, throw `KeyNotFound` error
        match self.map.get(&key) {
            Some(v) => v,
            // If no log pointer found, throw `KeyNotFound` error
            None => {
                return Err(KvError::KeyNotFound(GenericError::new(
                    "Key could not be found inside database",
                )))
            }
        };
        // If success
        //   create a value representing the "rm" command, containing it's key
        let command = Command::remove(&key, now());

        //   append the serialized command to the log
        let mut file = self.get_index_file()?;
        let command_buffer = bincode::serialize(&command)?;
        let command_buffer_length_buffer = u64::to_be_bytes(command_buffer.len() as u64);
        file.write(&command_buffer_length_buffer)?;
        file.write(&command_buffer)?;
        file.flush()?;
        Ok(())
    }
}
