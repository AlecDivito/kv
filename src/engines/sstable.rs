use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
};

use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::now;

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub struct Hint {
    timestamp: u128,
    value_size: usize,
    value_position: usize,
    key: String,
}

impl Hint {
    pub fn new(record: &Record, value_position: usize) -> Self {
        Self {
            timestamp: record.timestamp,
            value_size: record.value().len(),
            value_position,
            key: record.key.clone(),
        }
    }
}

impl std::fmt::Display for Hint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Hint({}): ({}, {}, {})",
            self.key, self.timestamp, self.value_position, self.value_size
        )
    }
}

#[derive(Default, Deserialize, Serialize, Debug)]
pub struct Record {
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

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> String {
        self.value.clone().unwrap_or("".to_string())
    }

    pub(crate) fn timestamp(&self) -> u128 {
        self.timestamp
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

#[derive(Clone, Debug)]
/// MemoryTable keeps a tree of key and values in sorted order. Once it reaches
/// a certian size, the table is moved to disk and a new empty one would take
/// its place.
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
        let path = path.into();
        debug!("Building memory table from redo log {:?}", &path);
        let table = Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            size: Arc::new(RwLock::new(0)),
        };
        let mut reader = BufReader::new(File::open(path)?);
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

    fn append(&self, record: Record) -> usize {
        let mut size = self.size.write().unwrap();
        let mut map = self.inner.write().unwrap();
        trace!("Memory Size {}: Appending {}", size, &record);
        let value_size = record.value().len();
        let key_size = record.key.len();
        *size = match map.insert(record.key, record.value) {
            Some(old_value) => (*size - old_value.unwrap_or("".into()).len()) + value_size,
            None => *size + key_size + value_size,
        };
        *size
    }

    fn get(&self, key: &str) -> Option<String> {
        let map = self.inner.read().unwrap();
        match map.get(key) {
            Some(value) => value.clone(),
            None => None,
        }
    }

    fn drain_to_segment(&self, path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let path = path.into();
        debug!("Draining memory table to segment {:?}", path);
        let mut index = HashMap::new();
        let mut writer = BufWriter::new(File::create(&path)?);
        let mut file_size = 0;
        let table = self.inner.read().unwrap();
        for (key, value) in table.iter() {
            let record = Record::new(key.clone(), value.clone());
            let bytes = bincode::serialize(&record)?;
            file_size += writer.write(&bytes)?;
            let hint = Hint::new(&record, file_size - record.value().len());
            trace!("Wrote {} to segment, added {} to index", record, hint);
            index.insert(record.key, hint);
        }
        drop(table);
        self.inner.write().unwrap().clear();
        Ok(Segment::new(index, &path, file_size))
    }
}

impl std::fmt::Display for MemoryTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MemoryTable(Size: {}, entries: {})",
            self.size.read().unwrap(),
            self.inner.read().unwrap().len()
        )
    }
}

#[derive(Clone, Debug)]
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
    pub fn new(directory: impl Into<PathBuf>) -> crate::Result<Self> {
        let directory = directory.into();
        let path = (&directory).join(format!("{}.redo", Uuid::new_v4()));
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
    pub fn append(&self, key: String, value: Option<String>) -> crate::Result<usize> {
        let record = Record::new(key, value);
        let bytes = bincode::serialize(&record)?;
        let mut lock = self.write_ahead_log.lock().unwrap();
        lock.write(&bytes)?;
        lock.flush()?;
        drop(lock);
        Ok(self.inner.append(record))
    }

    /// Check to see if a key exists inside of the SSTable
    pub fn get(&self, key: &str) -> Option<String> {
        self.inner.get(key)
    }

    /// Save the SSTable from memory onto disk as segment file. Return the path
    /// to the new segment file.
    pub fn save(&self, segment_path: impl Into<PathBuf>) -> crate::Result<Segment> {
        self.inner.drain_to_segment(segment_path)
    }
}

impl std::fmt::Display for SSTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SSTable({}, {:?})",
            self.inner, self.write_ahead_log_path
        )
    }
}

impl Drop for SSTable {
    fn drop(&mut self) {
        let path = self.write_ahead_log_path.as_path();
        trace!("Attempting to remove redo log {:?}", &path);
        match std::fs::remove_file(path) {
            Ok(_) => info!("Successfully removed redo log {:?}", &path),
            Err(e) => error!("Failed to remove redo log {:?} with error {:?}", &path, e),
        };
    }
}

#[derive(Clone, Debug)]
/// An index that maps records in a file a log file keys  
pub struct Segment {
    index: Pin<Box<HashMap<String, Hint>>>,
    segment_path: Pin<Box<PathBuf>>,
    size: Pin<Box<usize>>,
}

impl Segment {
    pub fn new(
        index: HashMap<String, Hint>,
        segment_path: impl Into<PathBuf>,
        size: usize,
    ) -> Self {
        let path = segment_path.into();
        debug!("Create new Segment with {} items {:?}", index.len(), &path);
        Self {
            index: Pin::new(Box::new(index)),
            segment_path: Pin::new(Box::new(path)),
            size: Pin::new(Box::new(size)),
        }
    }

    pub fn from_log(path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let segment_path = path.into();
        debug!("Reading segment from log: {:?}", &segment_path);
        let mut size = 0;
        let mut index = HashMap::new();
        let mut reader = BufReader::new(File::open(&segment_path)?);
        while reader.fill_buf().unwrap().len() != 0 {
            let record: Record = bincode::deserialize_from(&mut reader).unwrap();
            size += bincode::serialized_size(&record)? as usize;
            if record.crc != record.calculate_crc() {
                let actual_crc = record.calculate_crc();
                trace!("{} is corrupt (Actual {})", record, actual_crc);
                continue;
            }
            let hint = Hint::new(&record, size - record.value().len());
            trace!("Read record {}, adding {} to hashmap", &record, &hint);
            index.insert(record.key, hint);
        }
        Ok(Self::new(index, segment_path, size))
    }

    pub fn get(&self, key: &str) -> crate::Result<Option<String>> {
        debug!("Searching for {} in {:?}", key, self.segment_path);
        let hint = match self.index.get(key) {
            Some(hint) => hint,
            None => {
                trace!("Key {} not found in {:?}", key, self.segment_path);
                return Ok(None);
            }
        };
        let mut reader = BufReader::new(File::open(&*self.segment_path)?);
        let mut value = vec![0u8; hint.value_size];
        reader.seek(SeekFrom::Start(hint.value_position as u64))?;
        reader.read(&mut value)?;
        Ok(Some(String::from_utf8(value).unwrap()))
    }
}

impl std::fmt::Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Segment({} bytes, {} indicies -> {:?}) ",
            self.size,
            self.index.len(),
            self.segment_path
        )
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        debug!("Dropped {}", self);
    }
}

pub struct SegmentReader {
    path: PathBuf,
    reader: BufReader<File>,
    complete: bool,
    pub value: Option<Record>,
}

impl SegmentReader {
    pub fn new(segment: &Segment) -> crate::Result<Self> {
        trace!("Creating segment reader from {}", segment);
        let path = (*segment.segment_path).clone();
        let reader = BufReader::new(File::open(&path)?);
        Ok(Self {
            path,
            reader,
            value: None,
            complete: false,
        })
    }

    pub fn next(&mut self) -> crate::Result<()> {
        if self.value.is_none() {
            if !self.done() {
                let record = bincode::deserialize_from(&mut self.reader)?;
                trace!("Found next {} in {:?}", record, self.path);
                self.value.insert(record);
            }
        }
        Ok(())
    }

    pub fn done(&mut self) -> bool {
        self.reader.fill_buf().unwrap().len() == 0 && self.value.is_none()
    }

    pub fn complete(&mut self) {
        self.complete = true;
    }
}

impl Drop for SegmentReader {
    fn drop(&mut self) {
        if self.complete {
            trace!("Dropping segment reader {:?}. Deleting file.", &self.path);
            std::fs::remove_file(&self.path).unwrap();
        }
    }
}
