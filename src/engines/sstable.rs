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

    pub fn is_delete_record(&self) -> bool {
        self.value.is_none()
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

#[derive(Clone)]
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

    fn append(&self, record: Record) -> usize {
        trace!("Appending {}", &record);
        let mut size = self.size.write().unwrap();
        let mut map = self.inner.write().unwrap();
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

    fn size(&self) -> usize {
        *self.size.read().unwrap()
    }

    fn drain_to_segment(&self, path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let mut index = HashMap::new();
        let writer = BufWriter::new(File::create(&path.into())?);
        let table = self.inner.read().unwrap();
        let file_size = 0;
        for (key, value) in table.iter() {
            let record = Record::new(key.clone(), value.clone());
            let bytes = bincode::serialize(&record)?;
            file_size += writer.write(&bytes)?;
            let hint = Hint::new(&record, file_size - record.value().len());
            index.insert(record.key, hint);
        }
        drop(table);
        self.inner.write().unwrap().clear();
        Ok(Segment::new(index, path, file_size))
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
    pub fn new(directory: impl Into<PathBuf>) -> crate::Result<Self> {
        let directory = directory.into();
        let path = (&directory).join(format!("{}.log", Uuid::new_v4()));
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
        self.write_ahead_log.lock().unwrap().write(&bytes)?;
        Ok(self.inner.append(record))
    }

    /// Check to see if a key exists inside of the SSTable
    pub fn get(&self, key: &str) -> Option<String> {
        self.inner.get(key)
    }

    /// Rotate the SSTable from memory onto disk as segment file. Return the path
    /// to the new segment file.
    pub fn rotate(&self, segment_path: impl Into<PathBuf>) -> crate::Result<Segment> {
        self.inner.drain_to_segment(segment_path)
    }

    /// Get the size in bytes of the current SSTable
    pub fn size(&self) -> usize {
        self.inner.size()
    }
}

#[derive(Clone)]
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
        Self {
            index: Pin::new(Box::new(index)),
            segment_path: Pin::new(Box::new(segment_path.into())),
            size: Pin::new(Box::new(size)),
        }
    }

    pub fn from_log(path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let segment_path = path.into();
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
            index.insert(record.key, hint);
        }
        Ok(Self::new(index, segment_path, size))
    }

    pub fn get(&self, key: &str) -> crate::Result<Option<String>> {
        let hint = match self.index.get(key) {
            Some(hint) => hint,
            None => return Ok(None),
        };
        let mut reader = BufReader::new(File::open(*self.segment_path)?);
        let mut value = vec![0u8; hint.value_size];
        debug!("Reading {} from {:?}", key, self.segment_path);
        reader.seek(SeekFrom::Start(hint.value_position as u64))?;
        reader.read(&mut value)?;
        Ok(Some(String::from_utf8(value).unwrap()))
    }

    pub fn open(&self) -> crate::Result<BufReader<File>> {
        Ok(BufReader::new(File::open(*self.segment_path)?))
    }
}

pub struct SegmentReader {
    reader: BufReader<File>,
    value: Option<Record>,
}

impl SegmentReader {
    pub fn new(segment: &Segment) -> crate::Result<Self> {
        let reader = segment.open()?;
        let mut segment_reader = Self {
            reader,
            value: None,
        };
        Ok(segment_reader)
    }

    pub fn next(&mut self) -> crate::Result<()> {
        if self.value.is_none() {
            if !self.done() {
                self.value = Some(bincode::deserialize_from(self.reader)?)
            }
        }
        Ok(())
    }

    pub fn take(&mut self) -> Option<Record> {
        self.value.take()
    }

    pub fn peek(&self) -> &mut Option<Record> {
        &mut self.value
    }

    pub fn done(&mut self) -> bool {
        self.reader.fill_buf().unwrap().len() == 0 && self.value.is_none()
    }
}
