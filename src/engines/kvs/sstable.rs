use std::{collections::BTreeMap, path::PathBuf, pin::Pin, sync::Arc};

use async_bincode::AsyncBincodeReader;
use futures::StreamExt;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufReader, BufWriter},
    sync::RwLock,
};
use uuid::Uuid;

use crate::engines::kvs::{
    record::Record,
    segment::{Index, Segment},
};

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

    async fn from_write_ahead_log(path: impl Into<PathBuf>) -> crate::Result<Self> {
        let path = path.into();
        debug!("Building memory table from redo log {:?}", &path);
        let table = Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            size: Arc::new(RwLock::new(0)),
        };
        let reader = BufReader::new(File::open(path).await?);
        let mut bincode_reader = AsyncBincodeReader::<_, Record>::from(reader);

        while let Some(record) = bincode_reader.next().await {
            let record = record?;
            if record.crc != record.calculate_crc() {
                let actual_crc = record.calculate_crc();
                trace!("{} is corrupt (Actual {})", record, actual_crc);
                continue;
            }
            table.append(record).await;
        }

        Ok(table)
    }

    async fn append(&self, record: Record) -> usize {
        let mut size = self.size.write().await;
        let mut map = self.inner.write().await;
        trace!("Memory Size {}: Appending {}", size, &record);
        let value_size = record.value().len();
        let key_size = record.key.len();
        *size = match map.insert(record.key, record.value) {
            Some(old_value) => (*size - old_value.unwrap_or("".into()).len()) + value_size,
            None => *size + key_size + value_size,
        };
        *size
    }

    async fn get(&self, key: &str) -> Option<String> {
        let map = self.inner.read().await;
        match map.get(key) {
            Some(value) => value.clone(),
            None => None,
        }
    }

    async fn drain_to_segment(&self, path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let segment_path = path.into();
        debug!("Draining memory table to segment {:?}", segment_path);

        let table = self.inner.read().await;
        let element_length = table.len();
        let mut writer = BufWriter::new(File::create(&segment_path).await?);
        let mut size = writer.write(&element_length.to_be_bytes()).await?;

        let mut index = Index::new(element_length);
        let mut block_start = size;
        for (key, value) in table.iter() {
            let record = Record::new(key.clone(), value.clone());
            let mut bytes = bincode::serialize(&record)?;
            block_start += index.add(block_start, record)?;
            size += writer.write(&mut bytes).await?;
        }

        drop(table);
        self.inner.write().await.clear();
        Ok(Segment::new(index, &segment_path, size))
    }
}

impl std::fmt::Display for MemoryTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // write!(
        //     f,
        //     "MemoryTable(Size: {}, entries: {})",
        //     self.size.read().await,
        //     self.inner.read().await.len()
        // )

        write!(f, "MemoryTable(Unable to read values due to async")
    }
}

#[derive(Clone, Debug)]
/// SSTable stores records in a sorted order that a user has submitted to be
/// saved inside of the key value store. A write-ahead-log is also written to
/// disk just in case the database goes offline during operation.
pub struct SSTable {
    inner: MemoryTable,
    write_ahead_log: Arc<RwLock<BufWriter<File>>>,
    write_ahead_log_path: Pin<Box<PathBuf>>,
}

impl SSTable {
    /// Create a new SSTable and pass the directory in where a write-ahead-log
    /// should be created to save data on write.
    pub async fn new(directory: impl Into<PathBuf>) -> crate::Result<Self> {
        let directory = directory.into();
        let path = (&directory).join(format!("{}.redo", Uuid::new_v4()));
        let writer = BufWriter::new(File::create(&path).await?);
        Ok(Self {
            inner: MemoryTable::new(),
            write_ahead_log: Arc::new(RwLock::new(writer)),
            write_ahead_log_path: Pin::new(Box::new(path)),
        })
    }

    /// Restore an SSTable from it's write-ahead-log.
    pub async fn from_write_ahead_log(path: impl Into<PathBuf>) -> crate::Result<Self> {
        let path = path.into();
        let inner = MemoryTable::from_write_ahead_log(&path).await?;
        let writer = BufWriter::new(File::create(&path).await?);

        Ok(Self {
            inner,
            write_ahead_log: Arc::new(RwLock::new(writer)),
            write_ahead_log_path: Pin::new(Box::new(path)),
        })
    }

    /// Append a key value to the SSTable and write it to our log
    pub async fn append(&self, key: String, value: Option<String>) -> crate::Result<usize> {
        let record = Record::new(key, value);
        let bytes = bincode::serialize(&record)?;
        let mut lock = self.write_ahead_log.write().await;
        lock.write(&bytes).await?;
        lock.flush().await?;
        drop(lock);
        Ok(self.inner.append(record).await)
    }

    /// Check to see if a key exists inside of the SSTable
    pub async fn get(&self, key: &str) -> Option<String> {
        self.inner.get(key).await
    }

    /// Save the SSTable from memory onto disk as segment file. Return the path
    /// to the new segment file.
    pub async fn save(&self, segment_path: impl Into<PathBuf>) -> crate::Result<Segment> {
        self.inner.drain_to_segment(segment_path).await
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
