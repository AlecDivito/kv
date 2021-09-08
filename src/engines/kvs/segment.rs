use std::{fmt::Debug, io::SeekFrom, path::PathBuf, pin::Pin};

use async_bincode::AsyncBincodeReader;
use futures::StreamExt;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};

use crate::datastructures::bloom::BloomFilter;

use super::record::Record;

#[derive(Clone, Debug)]
pub struct BlockHint {
    key: String,
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
            key: String::new(),
            number_of_elements: 0,
            block_size: 0,
            block_start,
        }
    }

    fn init_block(&mut self, record: Record, record_size: u64) {
        self.key = record.key().to_string();
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

    pub fn compare(&self, key: &str) -> Compare {
        if self.key == key {
            Compare::Equal
        } else if self.key.as_str() < key {
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

    pub async fn search_for(
        &self,
        segment_path: &Pin<PathBuf>,
        key: &str,
    ) -> crate::Result<Option<String>> {
        let mut reader = BufReader::new(File::open(segment_path.to_path_buf()).await?);
        reader.seek(SeekFrom::Start(self.block_start)).await?;

        let mut bincode_reader = AsyncBincodeReader::<_, Record>::from(reader);
        while let Some(record) = bincode_reader.next().await {
            let record = record?;
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
        self.filter.insert(record.key());
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

    pub fn get(&self, key: &str) -> Option<&BlockHint> {
        if !self.filter.contains(key) {
            None
        } else {
            Some(self.search(key))
        }
    }

    fn search(&self, key: &str) -> &BlockHint {
        let mut middle = self.hints.len() / 2;
        let mut hints = &self.hints[..];
        loop {
            if hints.len() == 1 {
                return &hints[0];
            }
            match hints[middle].compare(key) {
                Compare::Higher => {
                    hints = &hints[middle..self.hints.len()];
                    middle = middle / 2;
                }
                Compare::Lower => {
                    hints = &hints[0..middle];
                    middle = middle / 2;
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

    pub async fn from_log(path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let segment_path = path.into();
        debug!("Reading segment from log: {:?}", &segment_path);
        let mut reader = BufReader::new(File::open(&segment_path).await?);
        let mut size_buffer = (0 as usize).to_be_bytes();
        let mut block_start = reader.read(&mut size_buffer).await?;
        let elements = usize::from_be_bytes(size_buffer);
        let mut bincode_reader = AsyncBincodeReader::<_, Record>::from(reader);

        let mut index = Index::new(elements);
        while let Some(record) = bincode_reader.next().await {
            block_start += index.add(block_start, record?)?;
        }
        Ok(Self::new(index, segment_path, block_start))
    }

    pub async fn from_segments(
        path: impl Into<PathBuf>,
        mut readers: Vec<SegmentReader>,
    ) -> crate::Result<Segment> {
        // initialize variables
        let segment_path = path.into();
        let estimated_elements = readers.iter().fold(0, |o, r| o + r.elements);
        let start: usize = 0;
        let mut writer = BufWriter::new(File::create(&segment_path).await?);
        let mut block_start = writer.write(&start.to_be_bytes()).await?;
        let mut index = Index::new(estimated_elements);
        let mut size = 0;
        let mut count: usize = 0;

        loop {
            // read the next record inside of the segment file
            for reader in readers.iter_mut() {
                reader.next().await?;
            }

            // get all of the values from the readers
            let mut records: Vec<&mut Option<Record>> = readers
                .iter_mut()
                .map(|r| &mut r.value)
                .filter(|r| r.is_some())
                .collect();

            // however, if there was no records left, then leave the loop
            if records.is_empty() {
                break;
            }

            // sort by key so we have an ordered list from largest to smallest
            records.sort_by_key(|f| f.as_ref().unwrap().key().to_string());
            records.reverse();

            // remove the first value and take all of the other keys that are equal to it
            let mut groupped_records = vec![records.pop().unwrap().take().unwrap()];
            for record in records {
                if record.as_ref().unwrap().key == groupped_records[0].key {
                    groupped_records.push(record.take().unwrap());
                }
            }

            // again, sort by timestamp, take the newest one (highest timestamp)
            groupped_records.sort_by_key(|r| r.timestamp);
            let writeable_record = groupped_records.pop().unwrap();

            // write the record to our database
            let mut bytes = bincode::serialize(&writeable_record)?;
            block_start += index.add(block_start, writeable_record)?;
            size += writer.write(&mut bytes).await?;
            count += 1;
        }

        // rewrite first 8 bytes to have the correct count of elements in the file
        writer.seek(SeekFrom::Start(0)).await?;
        writer.write(&count.to_be_bytes()).await?;

        Ok(Segment::new(index, segment_path, size))
    }

    pub async fn get(&self, key: &str) -> crate::Result<Option<String>> {
        debug!("Searching for {} in {:?}", key, self.segment_path);
        if let Some(block_hint) = self.index.get(key) {
            Ok(block_hint.search_for(&self.segment_path, key).await?)
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
    reader: AsyncBincodeReader<BufReader<File>, Record>,
    elements: usize,
    complete: bool,
    pub value: Option<Record>,
}

impl SegmentReader {
    pub async fn new(segment: &Segment) -> crate::Result<Self> {
        trace!("Creating segment reader from {}", segment);
        let path = PathBuf::from(&*segment.segment_path.clone());
        let mut reader = BufReader::new(File::open(&path).await?);
        let mut size_buffer = (0 as usize).to_be_bytes();
        reader.read(&mut size_buffer).await?;
        let elements = usize::from_be_bytes(size_buffer);
        let bincode_reader = AsyncBincodeReader::<_, Record>::from(reader);
        Ok(Self {
            path,
            reader: bincode_reader,
            elements,
            complete: false,
            value: None,
        })
    }

    pub async fn next(&mut self) -> crate::Result<()> {
        if self.value.is_none() && !self.complete {
            if let Some(record) = self.reader.next().await {
                let record = record?;
                trace!("Found next {} in {:?}", record, self.path);
                self.value.insert(record);
            } else {
                self.complete = true;
            }
        }
        Ok(())
    }

    pub fn done(&mut self) -> bool {
        self.complete
    }
}
