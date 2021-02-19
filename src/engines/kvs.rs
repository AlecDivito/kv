use super::Result;
use crate::{engines::KvsEngine, GenericError, KvError};
use serde::{Deserialize, Serialize};
use std::fs::{metadata, DirEntry, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    rc::Rc,
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

#[derive(Debug)]
struct LogPointer {
    log_pointer: Rc<PathBuf>,
    value_length: u64,
    value_position: u64,
    timestamp: u128,
}

const BINCODE_STRING_LENGTH_OFFSET: u64 = 8;
const BINCODE_STRING_OPTION_OFFSET: u64 = 1;
const BINCODE_STRING_OFFSET: u64 = BINCODE_STRING_LENGTH_OFFSET + BINCODE_STRING_OPTION_OFFSET;

impl LogPointer {
    pub fn write<'b>(log_pointer: Rc<PathBuf>, offset: u64, command: &Command<'b>) -> LogPointer {
        let value_length = command.get_value_length();
        let value_position = offset + BINCODE_STRING_OFFSET;
        LogPointer {
            log_pointer,
            value_length,
            value_position,
            timestamp: command.timestamp,
        }
    }

    pub fn new(
        log_pointer: Rc<PathBuf>,
        offset: u64,
        value_length: u64,
        timestamp: u128,
    ) -> LogPointer {
        let value_position = offset + BINCODE_STRING_OFFSET;
        LogPointer {
            log_pointer,
            value_length,
            value_position,
            timestamp: timestamp,
        }
    }

    pub fn re_point(&mut self, new_log_pointer: Rc<PathBuf>) {
        self.log_pointer = new_log_pointer;
    }
}

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk
/// ```
pub struct KvStore {
    map: HashMap<String, LogPointer>,
    directory: PathBuf,
    active_file: File,
    active_path: Rc<PathBuf>,
    active_size: u64,
    logs: HashSet<Rc<PathBuf>>,
}

fn now() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

const FILE_SUFFIX: &'static str = "kvs";
const COMPACT_FILE: &'static str = "merge.kvs";
const MAX_LOG_FILE_SIZE: u64 = 5 * 64 * 1024;
const MAX_LOG_FILES: usize = 16 + 1;

impl KvStore {
    /// Build a `kvStore` from a database folder
    pub fn open<'b>(folder: impl Into<PathBuf>) -> Result<KvStore> {
        // 1. Load given directory, create variables
        let directory = folder.into();
        let mut logs = HashSet::new();
        let mut map: HashMap<String, LogPointer> = HashMap::new();

        // 3. For each file or folder found in directory, loop and save `.kvs` files
        for entry in std::fs::read_dir(&directory)? {
            // 1. Skip all files that don't match our file suffix '.kvs'
            let entry = entry?;
            let file_name = entry.file_name();
            if !file_name.to_string_lossy().ends_with(FILE_SUFFIX) {
                continue;
            }
            let path = Rc::new(entry.path());
            logs.insert(path);
        }

        // 4. If this is the first time loading files, create the first
        if logs.len() == 0 {
            logs.insert(Rc::new(directory.join(format!("log_{}.{}", 0, FILE_SUFFIX))));
        }

        // 5. Load all references from data files into map
        for log_pointer in &logs {
            // 1. Open file, get length, create reader, u64 as [u8]
            let file = std::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(log_pointer.as_path())?;
            let file_length = file.metadata().unwrap().len();
            if file_length == 0 {
                break;
            }
            let mut reader = BufReader::new(file);
            let mut command_length_buffer = [0; 8];

            // 2. Read the file and stop when the entire file has been read
            while reader.seek(SeekFrom::Current(0))? < file_length {
                // 1. Read the length of the record and get pointer to it
                reader.read_exact(&mut command_length_buffer)?;
                let record_pointer = reader.seek(SeekFrom::Current(0))?;
                // 2. Get the command length
                let command_length: u64 = u64::from_be_bytes(command_length_buffer);
                // 3. Read entire command from file
                let mut command_buffer: Vec<u8> = vec![0; command_length as usize];
                reader.read_exact(&mut command_buffer)?;
                let command: Command = bincode::deserialize(&command_buffer)?;

                // 5. Try and find it, or insert it into out map
                let key = command.key.to_string();
                let new_pointer = LogPointer::write(log_pointer.clone(), record_pointer, &command);
                match map.entry(key) {
                    Entry::Occupied(mut old) => {
                        let a = old.get();
                        if a.timestamp < new_pointer.timestamp {
                            if command.is_remove() {
                                old.remove();
                            } else {
                                old.insert(new_pointer);
                            }
                        }
                    }
                    Entry::Vacant(v) => {
                        v.insert(new_pointer);
                    }
                }
            }
        }

        // 6. Find the file with the lowest byte size and store a FileHandler to it
        let mut files = logs.iter()
            .map(|p| {
                // unwrap here is technically wrong but it should be fine
                let length = std::fs::metadata(p.as_ref()).unwrap().len();
                (length, p)
            });

        // We unwrap here because we know that there will always be at least
        // 1 element inside of the logs list
        let mut active_path = files.next().unwrap();
        for (length, pointer) in files {
            if active_path.0 > length {
                active_path = (length, pointer);
            }
        }

        let active_path = active_path.1.clone();

        // 7. Open up the active file
        let active_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(active_path.as_ref())?;

        // 8. Get file size for active file
        let active_size = metadata(active_path.as_ref())?.len();

        Ok(KvStore {
            map,
            directory,
            active_file,
            active_path,
            active_size,
            logs,
        })
    }

    fn generate_log_file_name(&self, index: usize) -> String {
        let filename= format!("log_{}.{}", index, FILE_SUFFIX);
        if self.directory.join(&filename).exists() {
            self.generate_log_file_name(index + 1)
        } else {
            filename
        }
    }

    // Rotate closes the active file and creates a new one
    fn rotate(&mut self) -> Result<()> {
        // get all old logs before creating a new active file
        let old_logs = std::fs::read_dir(&self.directory)?
            .map(|e| e.unwrap())
            .filter(|f| f.file_name().to_string_lossy().ends_with(FILE_SUFFIX))
            .collect::<Vec<DirEntry>>();
            
        // create active file
        self.active_size = 0;
        self.active_path = Rc::new(self.directory.join(self.generate_log_file_name(self.logs.len())));
        self.logs.insert(self.active_path.clone());
        self.active_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(self.active_path.as_ref())?;

        // merge all old files togther if there are too many, delete the old ones
        if old_logs.len() > MAX_LOG_FILES {
            self.merge()?;

            // delete all old logs
            old_logs
                .iter()
                .for_each(|f| std::fs::remove_file(f.path()).unwrap());
        }


        Ok(())
    }

    // Go through `ALL` records and merge them all into one file
    fn merge(&mut self) -> Result<()> {
        // Create compact file
        let merge_path = Rc::new(self.directory.join(self.generate_log_file_name(self.logs.len())));
        let compact_path = self.directory.join(COMPACT_FILE);
        let mut merge_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&compact_path)?;

        // loop over current keys and write them all to the compact file
        let mut offset = 8; // keep the offset to the start of the next record
        for (key, ptr) in &mut self.map {
            // try and get the value from the database
            let value = KvStore::read_from_log_pointer(ptr)?.ok_or(KvError::Compact(
                GenericError::new("All keys must point to values"),
            ))?;
            // create the command to insert into database
            let command = Command::insert(&key, &value, now());
            let command_buffer = bincode::serialize(&command)?;
            let command_buffer_length_buffer = u64::to_be_bytes(command_buffer.len() as u64);
            ptr.re_point(merge_path.clone());
            merge_file.write(&command_buffer_length_buffer)?;
            merge_file.write(&command_buffer)?;
            offset =
                offset + command_buffer.len() as u64 + command_buffer_length_buffer.len() as u64;
        }
        merge_file.flush()?;

        // rename the compact file into the new log file
        std::fs::rename(&compact_path, merge_path.as_path())?;

        self.logs.insert(merge_path.clone());

        Ok(())
    }

    fn read_from_log_pointer(log_pointer: &LogPointer) -> Result<Option<String>> {
        // If success Find the value from the file
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(&log_pointer.log_pointer.as_ref())?;
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
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten
    fn set(&mut self, key: String, value: String) -> Result<()> {
        // create a value representing the `set` command, containing key and value
        let timestamp = now();
        let command_buffer = {
            let command = Command::insert(&key, &value, timestamp);
            bincode::serialize(&command)
        }?;

        // Serialize the `Command` to a String
        let command_buffer_length_buffer = u64::to_be_bytes(command_buffer.len() as u64);

        // Append serialized command to log file
        let offset = self.active_size + command_buffer_length_buffer.len() as u64;
        self.active_size = offset + command_buffer.len() as u64;
        self.active_file.write(&command_buffer_length_buffer)?;
        self.active_file.write(&command_buffer)?;
        self.active_file.flush()?;

        // Add command to hashmap as log pointer
        let log_pointer = LogPointer::new(
            self.active_path.clone(),
            offset,
            value.len() as u64,
            timestamp,
        );
        self.map.insert(key, log_pointer);

        if offset > MAX_LOG_FILE_SIZE {
            self.rotate()?;
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
            None => return Ok(None),
        };
        // If success Find the value from the file
        KvStore::read_from_log_pointer(log_pointer)
    }

    /// Remove a given key.
    fn remove(&mut self, key: String) -> Result<()> {
        // Checks the map for log pointer
        // If no log pointer found, throw `KeyNotFound` error
        if self.map.get(&key).is_none() {
            return Err(KvError::KeyNotFound(GenericError::new(
                "Key could not be found inside database",
            )));
        }

        // If success
        //   create a value representing the "rm" command, containing it's key
        let command = Command::remove(&key, now());

        //   append the serialized command to the log
        let command_buffer = bincode::serialize(&command)?;
        let command_length = command_buffer.len() as u64;
        let command_buffer_length_buffer = command_length.to_be_bytes();
        // save byte offsets
        let offset = self.active_size + 8;
        self.active_size = offset + command_length;
        // write to log
        self.active_file.write(&command_buffer_length_buffer)?;
        self.active_file.write(&command_buffer)?;
        self.active_file.flush()?;
        // remove key from map
        self.map.remove(&key);

        // see if we need to rotate our logs
        if self.active_size > MAX_LOG_FILE_SIZE {
            self.rotate()?;
        }
        //   return (), exit
        Ok(())
    }
}
