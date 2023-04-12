use std::{
    collections::BTreeMap,
    fmt::Debug,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
};

use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::now;
use crate::datastructures::bloom::BloomFilter;

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub struct Record {
    crc: u32,
    timestamp: u128,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl Record {
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
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
        digest.update(&self.key);
        digest.update(self.value.as_ref().unwrap_or(&vec![]));
        digest.finalize()
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> Option<&Vec<u8>> {
        self.value.as_ref()
    }
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Record({}, {}): {} -> {}",
            self.crc,
            self.timestamp,
            String::from_utf8_lossy(&self.key),
            self.value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v))
                .unwrap_or("None".into())
        )
    }
}

/// MemoryTable keeps a tree of key and values in sorted order. Once it reaches
/// a certian size, the table is moved to disk and a new empty one would take
/// its place.
#[derive(Clone, Debug)]
struct MemoryTable {
    inner: Arc<RwLock<MemTable>>,
}

#[derive(Clone, Debug)]
struct MemTable {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    size: usize,
}

impl MemoryTable {
    fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemTable {
                map: BTreeMap::new(),
                size: 0,
            })),
        }
    }

    fn from_write_ahead_log(path: impl AsRef<Path>) -> crate::Result<Self> {
        debug!("Building memory table from redo log {:?}", &path.as_ref());
        let table = Self::new();
        let mut reader = BufReader::new(File::open(path.as_ref())?);
        while !reader.fill_buf().unwrap().is_empty() {
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
        let value_size = record.value().map(|v| v.len()).unwrap_or(0);
        let key_size = record.key.len();
        let mut lock = self.inner.write().unwrap();

        trace!("Memory Size {}: Appending {}", lock.size, &record);

        lock.size = match lock.map.insert(record.key, record.value) {
            Some(old_value) => lock.size - old_value.map(|v| v.len()).unwrap_or(0) + value_size,
            None => lock.size + key_size + value_size,
        };
        let size = lock.size;
        drop(lock);
        size
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.inner.read().unwrap().map.get(key) {
            Some(value) => value.clone(),
            None => None,
        }
    }

    /// Drain memory table to file and return it as a segment.
    fn drain_to_segment(&self, path: impl AsRef<Path>) -> crate::Result<Segment> {
        debug!("Draining memory table to segment {:?}", path.as_ref());

        let mut writer = BufWriter::new(File::create(path.as_ref())?);

        let table = self.inner.read().unwrap();
        let number_of_records = table.map.len();
        let mut index = Index::new(number_of_records);
        let mut block_start = writer.write(&number_of_records.to_be_bytes())?;
        let mut size = block_start;

        for (key, value) in table.map.iter() {
            let record = Record::new(key.clone(), value.clone());
            let bytes = bincode::serialize(&record)?;
            block_start += index.add(block_start, record)?;
            size += writer.write(&bytes)?;
        }

        drop(table);

        Ok(Segment::new(index, path.as_ref(), size))
    }
}

impl std::fmt::Display for MemoryTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let writer = self.inner.read().unwrap();
        write!(
            f,
            "MemoryTable(Size: {}, entries: {})",
            writer.size,
            writer.map.len()
        )
    }
}

/// SSTable stores records in a sorted order that a user has submitted to be
/// saved inside of the key value store. A write-ahead-log is also written to
/// disk just in case the database goes offline during operation.
#[derive(Clone, Debug)]
pub struct SSTable {
    inner: MemoryTable,
    write_ahead_log: Arc<Mutex<BufWriter<File>>>,
}

impl SSTable {
    /// Create a new SSTable and pass the directory in where a write-ahead-log
    /// should be created to save data on write.
    pub fn new(directory: impl AsRef<Path>) -> crate::Result<Self> {
        info!("Creating new SSTable: {:?}.redo", directory.as_ref());
        let path = directory.as_ref().join(format!("{}.redo", Uuid::new_v4()));
        let writer = BufWriter::new(File::create(path)?);
        Ok(Self {
            inner: MemoryTable::new(),
            write_ahead_log: Arc::new(Mutex::new(writer)),
        })
    }

    /// Restore an SSTable from it's write-ahead-log.
    pub fn from_write_ahead_log(path: impl AsRef<Path>) -> crate::Result<Self> {
        info!("Restoring SSTable from: {:?}", path.as_ref());
        let inner = MemoryTable::from_write_ahead_log(path.as_ref())?;
        let writer = BufWriter::new(File::create(path.as_ref())?);

        Ok(Self {
            inner,
            write_ahead_log: Arc::new(Mutex::new(writer)),
        })
    }

    /// Append a key value to the SSTable and write it to our log
    pub fn append(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> crate::Result<usize> {
        let record = Record::new(key, value);
        let bytes = bincode::serialize(&record)?;
        let mut lock = self.write_ahead_log.lock().unwrap();
        lock.write_all(&bytes)?;
        drop(lock);
        Ok(self.inner.append(record))
    }

    /// Check to see if a key exists inside of the SSTable
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.get(key)
    }

    /// Save the SSTable from memory onto disk as segment file. Return the path
    /// to the new segment file.
    pub fn save(&self, segment_path: impl AsRef<Path>) -> crate::Result<Segment> {
        self.inner.drain_to_segment(segment_path)
    }
}

impl std::fmt::Display for SSTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SSTable({})", self.inner)
    }
}

impl Drop for SSTable {
    fn drop(&mut self) {
        // let path = self.write_ahead_log_path.as_path();
        // trace!("Attempting to remove redo log {:?}", &path);
        // match std::fs::remove_file(path) {
        //     Ok(_) => info!("Successfully removed redo log {:?}", &path),
        //     Err(e) => error!("Failed to remove redo log {:?} with error {:?}", &path, e),
        // };
    }
}

#[derive(Clone, Debug)]
pub struct BlockHint {
    key: Vec<u8>,
    number_of_elements: usize,
    block_size: u64,
    block_start: u64,
}

pub enum Compare {
    Equal,
    Higher,
    Lower,
}

impl BlockHint {
    pub fn new(block_start: u64) -> Self {
        Self {
            key: Vec::new(),
            number_of_elements: 0,
            block_size: 0,
            block_start,
        }
    }

    fn init_block(&mut self, record: Record, record_size: u64) {
        self.key = record.key().to_vec();
        self.block_size = record_size;
        self.number_of_elements = 1;
    }

    pub fn add(&mut self, record: Record) -> crate::Result<(u64, Option<BlockHint>)> {
        let record_size = bincode::serialized_size(&record)?;
        let mut next_block = None;
        if self.block_size == 0 {
            // Adding the first block
            self.init_block(record, record_size);
        } else {
            let new_block_size = self.block_size + record_size;
            if new_block_size - self.block_start > 4096 {
                // create a new block
                let mut new_block = BlockHint::new(self.block_start + self.block_size);
                new_block.init_block(record, record_size);
                next_block = Some(new_block);
            } else {
                // add to the current block
                self.number_of_elements += 1;
                self.block_size = new_block_size;
            }
        }
        Ok((record_size, next_block))
    }

    pub fn compare(&self, key: &[u8]) -> Compare {
        if self.key == key {
            Compare::Equal
        } else if self.key.deref() < key {
            Compare::Higher
        } else {
            Compare::Lower
        }
    }

    pub fn size(&self) -> usize {
        self.key.len()
            + self.number_of_elements.to_be_bytes().len()
            + self.block_size.to_be_bytes().len()
            + self.block_start.to_be_bytes().len()
    }

    pub(crate) fn search_for(
        &self,
        segment_path: &Pin<PathBuf>,
        key: &[u8],
    ) -> crate::Result<Option<Vec<u8>>> {
        let mut reader = BufReader::new(File::open(segment_path.to_path_buf())?);
        reader.seek(SeekFrom::Start(self.block_start))?;

        let mut counter = 0;
        while counter <= self.number_of_elements {
            if reader.fill_buf().unwrap().is_empty() {
                return Ok(None);
            }
            counter += 1;
            let record: Record = bincode::deserialize_from(&mut reader)?;
            if record.key == key {
                return Ok(record.value);
            }
        }
        Ok(None)
    }
}

pub struct Index {
    filter: BloomFilter,
    hints: Vec<BlockHint>,
    element_size: usize,
    byte_size: u64,
}

impl Index {
    pub fn new(estimated_elements: usize) -> Self {
        let filter = BloomFilter::new(estimated_elements, 0.001);
        Self {
            filter,
            hints: Vec::new(),
            element_size: 0,
            byte_size: 0,
        }
    }

    pub fn add(&mut self, block_start: usize, record: Record) -> crate::Result<usize> {
        if record.crc != record.calculate_crc() {
            let actual_crc = record.calculate_crc();
            error!("{} is corrupt (Actual {})", record, actual_crc);
            return Ok(bincode::serialized_size(&record)? as usize);
        }
        self.filter.insert(&String::from_utf8_lossy(record.key()));
        let block = match self.hints.last_mut() {
            Some(block) => block,
            None => {
                let block = BlockHint::new(block_start as u64);
                self.hints.push(block);
                self.hints.last_mut().unwrap()
            }
        };
        let (record_size, new_block) = block.add(record)?;
        self.byte_size += record_size;
        if let Some(block) = new_block {
            self.hints.push(block);
        }
        Ok(record_size as usize)
    }

    pub fn get(&self, key: &[u8]) -> Option<&BlockHint> {
        if !self.filter.contains(&String::from_utf8_lossy(key)) {
            None
        } else {
            Some(self.search(key))
        }
    }

    fn search(&self, key: &[u8]) -> &BlockHint {
        let mut middle = self.hints.len() / 2;
        let mut hints = &self.hints[..];
        loop {
            if hints.len() == 1 {
                return &hints[0];
            }
            match hints[middle].compare(key) {
                Compare::Higher => {
                    hints = &hints[middle..self.hints.len()];
                    middle /= 2;
                }
                Compare::Lower => {
                    hints = &hints[0..middle];
                    middle /= 2;
                }
                Compare::Equal => return &hints[middle],
            }
        }
    }
}

impl Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index")
            .field("hints", &self.hints)
            .field("element_size", &self.element_size)
            .field("byte_size", &self.byte_size)
            .finish()
    }
}

impl std::fmt::Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let index_size = self.hints.iter().fold(0, |o, h| o + h.size());
        write!(
            f,
            "Index(data size: {}, index size: {}, element size: {})",
            self.byte_size, index_size, self.element_size
        )
    }
}
/// An index that maps records in a file a log file keys  
pub struct Segment {
    index: Pin<Box<Index>>,
    segment_path: Pin<PathBuf>,
    size: Pin<Box<usize>>,
    should_remove: Pin<Box<bool>>,
}

impl Segment {
    pub fn new(index: Index, segment_path: impl Into<PathBuf>, size: usize) -> Self {
        let path = segment_path.into();
        debug!("Create new Segment with {} items {:?}", index, &path);
        Self {
            index: Pin::new(Box::new(index)),
            segment_path: Pin::new(path),
            size: Pin::new(Box::new(size)),
            should_remove: Pin::new(Box::new(false)),
        }
    }

    pub fn from_log(path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let segment_path = path.into();
        debug!("Reading segment from log: {:?}", &segment_path);
        let mut reader = BufReader::new(File::open(&segment_path)?);
        let mut size_buffer = 0_usize.to_be_bytes();
        let mut block_start = reader.read(&mut size_buffer)?;
        let elements = usize::from_be_bytes(size_buffer);

        let mut index = Index::new(elements);
        while !reader.fill_buf().unwrap().is_empty() {
            let record: Record = bincode::deserialize_from(&mut reader).unwrap();
            block_start += index.add(block_start, record)?;
        }
        Ok(Self::new(index, segment_path, block_start))
    }

    pub fn from_segments(
        path: impl Into<PathBuf>,
        mut readers: Vec<SegmentReader>,
    ) -> crate::Result<Segment> {
        // initialize variables
        let segment_path = path.into();
        let estimated_elements = readers.iter().fold(0, |o, r| o + r.elements);
        let start: usize = 0;
        let mut writer = BufWriter::new(File::create(&segment_path)?);
        let mut block_start = writer.write(&start.to_be_bytes())?;
        let mut index = Index::new(estimated_elements);
        let mut size = 0;
        let mut count: usize = 0;

        loop {
            // read the next record inside of the segment file
            for reader in readers.iter_mut() {
                reader.next()?;
            }

            // get all of the values from the readers
            let mut records = readers
                .iter()
                .filter_map(|r| r.value.as_ref())
                .collect::<Vec<_>>();

            // however, if there was no records left, then leave the loop
            if records.is_empty() {
                break;
            }

            // sort by key so we have an ordered list from largest to smallest
            records.sort_by_key(|f| f.value.as_deref());
            records.reverse();

            // remove the first value and take all of the other keys that are equal to it
            let mut groupped_records = vec![records.pop().unwrap()];
            for record in records {
                if record.key == groupped_records[0].key {
                    groupped_records.push(record);
                }
            }

            // again, sort by timestamp, take the newest one (highest timestamp)
            groupped_records.sort_by_key(|r| r.timestamp);
            let writeable_record = groupped_records.pop().unwrap();

            // write the record to our database
            let bytes = bincode::serialize(&writeable_record)?;
            block_start += index.add(block_start, writeable_record.clone())?;
            size += writer.write(&bytes)?;
            count += 1;
        }

        // rewrite first 8 bytes to have the correct count of elements in the file
        writer.rewind()?;
        writer.write_all(&count.to_be_bytes())?;

        Ok(Segment::new(index, segment_path, size))
    }

    pub fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        debug!(
            "Searching for {} in {:?}",
            String::from_utf8_lossy(key),
            self.segment_path
        );
        if let Some(block_hint) = self.index.get(key) {
            Ok(block_hint
                .search_for(&self.segment_path, key)?
                .map(|v| v.to_vec()))
        } else {
            Ok(None)
        }
    }

    pub fn mark_for_removal(&mut self) {
        *self.should_remove = true;
    }
}

impl std::fmt::Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Segment({} bytes, {} -> {:?}) ",
            self.size, self.index, self.segment_path
        )
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("index", &self.index)
            .field("segment_path", &self.segment_path)
            .field("size", &self.size)
            .finish()
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if *self.should_remove {
            trace!("Dropping segment {:?}. Deleting file.", &self.segment_path);
            if self.segment_path.exists() {
                std::fs::remove_file(&*self.segment_path).unwrap();
            } else {
                error!(
                    "Failed to delete segment {:?} as the file no longer exists",
                    self.segment_path
                );
            }
        }
    }
}

pub struct SegmentReader {
    path: PathBuf,
    reader: BufReader<File>,
    elements: usize,
    pub value: Option<Record>,
}

impl SegmentReader {
    pub fn new(segment: &Segment) -> crate::Result<Self> {
        trace!("Creating segment reader from {}", segment);
        let path = PathBuf::from(&*segment.segment_path.clone());
        let mut reader = BufReader::new(File::open(&path)?);
        let mut size_buffer = 0_usize.to_be_bytes();
        reader.read_exact(&mut size_buffer)?;
        let elements = usize::from_be_bytes(size_buffer);
        Ok(Self {
            path,
            reader,
            elements,
            value: None,
        })
    }

    pub fn next(&mut self) -> crate::Result<()> {
        if self.value.is_none() && !self.done() {
            let record = bincode::deserialize_from(&mut self.reader)?;
            trace!("Found next {} in {:?}", record, self.path);
            let _ = self.value.insert(record);
        }
        Ok(())
    }

    pub fn done(&mut self) -> bool {
        self.reader.fill_buf().unwrap().is_empty() && self.value.is_none()
    }
}
