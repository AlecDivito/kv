use std::{collections::{BTreeMap, HashMap}, fs::File, io::{BufRead, BufReader, BufWriter, Write}, path::PathBuf, pin::Pin, sync::{Arc, Mutex, RwLock}};

use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{common::now, KvError};

#[derive(Default, Deserialize, Serialize, Debug)]
struct Record {
    crc: u32,
    timestamp: u128,
    key: String,
    value: Option<String>,
}

impl Record {
    pub fn new(key: String, value: Option<String>) -> Self {
        let timestamp = now();
        let mut record = Self {
            crc: 0,
            timestamp,
            key,
            value,
        };
        record.crc = record.calculate_crc();
        record
    }

    pub fn calculate_crc(&self) -> u32 {
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&self.timestamp.to_be_bytes());
        digest.update(self.key.as_bytes());
        digest.update(
            self.value
                .clone()
                .unwrap_or(String::with_capacity(0))
                .as_bytes(),
        );
        digest.finalize()
    }

    pub fn value(&self) -> String {
        self.value.clone().unwrap_or("".to_string())
    }

    pub fn is_delete_record(&self) -> bool {
        self.value.is_none()
    }
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Record({}, {}): {} -> {}",
            self.crc,
            self.timestamp,
            self.key,
            self.value.as_ref().unwrap_or(&"".to_string())
        )
    }
}

#[derive(Clone)]
struct MemoryTable {
    inner: Arc<RwLock<BTreeMap<String, Option<String>>>>,
    size: Arc<RwLock<usize>>,
}

impl MemoryTable {
    fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            size: Arc::new(RwLock::new(0)),
        }
    }

    fn from_write_ahead_log(path: impl Into<PathBuf>) -> crate::Result<Self> {
        let table = Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            size: Arc::new(RwLock::new(0)),
        };
        let mut reader = BufReader::new(File::open(path.into())?);
        while reader.fill_buf().unwrap().len() != 0 {
            let record: Record = bincode::deserialize_from(&mut reader).unwrap();
            if record.crc != record.calculate_crc() {
                let actual_crc = record.calculate_crc();
                trace!("{} is corrupt (Actual {})", record, actual_crc);
                continue;
            }
            table.append(record);
        }

        Ok(table)
    }

    fn append(&self, record: Record) {
        trace!("Appending {}", &record);
        let mut size = self.size.write().unwrap();
        let mut map = self.inner.write().unwrap();
        let value_size = record.value().len();
        let key_size = record.key.len();
        *size = match map.insert(record.key, record.value) {
            Some(old_value) => (*size - old_value.unwrap_or("".into()).len()) + value_size,
            None => *size + key_size + value_size,
        };
    }

    fn get(&self, key: &str) -> crate::Result<Option<String>> {
        let map = self.inner.read().unwrap();
        match map.get(key) {
            Some(value) => Ok(value.clone()),
            None => Err(KvError::KeyNotFound(
                format!("Key {:?} could not be found", key).into(),
            )),
        }
    }

    fn size(&self) -> usize {
        *self.size.read().unwrap()
    }

    fn drain_to_segment(&self, path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let writer = BufWriter::new(File::create(&path.into())?);
        let table = self.inner.read().unwrap();
        for (key, value) in table.iter() {
            let record = Record::new(key.clone(), value.clone());
            let bytes = bincode::serialize(&record)?;
            writer.write(&bytes)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
/// SSTable stores records in a sorted order that a user has submitted to be
/// saved inside of the key value store. A write-ahead-log is also written to
/// disk just in case the database goes offline during operation.
pub struct SSTable {
    inner: MemoryTable,
    write_ahead_log: Arc<Mutex<BufWriter<File>>>,
    write_ahead_log_path: Pin<Box<PathBuf>>,
}

impl SSTable {
    /// Create a new SSTable and pass the directory in where a write-ahead-log
    /// should be created to save data on write.
    pub fn from_directory(directory: impl Into<PathBuf>) -> crate::Result<Self> {
        let directory = directory.into();
        let path = directory.join(format!("{}.wal", Uuid::new_v4()));
        let writer = BufWriter::new(File::create(&path)?);
        Ok(Self {
            inner: MemoryTable::new(),
            write_ahead_log: Arc::new(Mutex::new(writer)),
            write_ahead_log_path: Pin::new(Box::new(path)),
        })
    }

    /// Restore an SSTable from it's write-ahead-log.
    pub fn from_write_ahead_log(path: impl Into<PathBuf>) -> crate::Result<Self> {
        let path = path.into();
        let inner = MemoryTable::from_write_ahead_log(&path)?;
        let writer = BufWriter::new(File::create(&path)?);

        Ok(Self {
            inner,
            write_ahead_log: Arc::new(Mutex::new(writer)),
            write_ahead_log_path: Pin::new(Box::new(path)),
        })
    }

    /// Append a key value to the SSTable and write it to our log
    pub fn append(&self, key: String, value: Option<String>) -> crate::Result<()> {
        let record = Record::new(key, value);
        let bytes = bincode::serialize(&record)?;
        self.write_ahead_log.lock().unwrap().write(&bytes)?;
        self.inner.append(record);
        Ok(())
    }

    /// Check to see if a key exists inside of the SSTable
    pub fn get(&self, key: &str) -> crate::Result<String> {
        match self.inner.get(key)? {
            Some(key) => Ok(key.clone()),
            None => Err(KvError::KeyNotFound(
                format!("Key {:?} could not be found", key).into(),
            )),
        }
    }

    /// Rotate the SSTable from memory onto disk as segment file. Return the path
    /// to the new segment file.
    pub fn rotate(&self) -> crate::Result<Segment> {
        let mut segment_log_path = PathBuf::from(self.write_ahead_log_path.as_path());
        segment_log_path.set_extension("table");
        Ok(self.inner.drain_to_segment(&segment_log_path)?);
    }

    /// Get the size in bytes of the current SSTable
    pub fn size(&self) -> usize {
        self.inner.size()
    }
}

#[derive(Clone)]
/// An index that maps records in a file a log file keys  
pub struct Segment {
    index: Pin<Box<HashMap<String, Hint>>,
    segment_path: Pin<Box<PathBuf>>,
    size: Pin<Box<usize>>,
}
