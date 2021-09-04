use std::{
    collections::HashMap,
    convert::TryInto,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
};

use uuid::Uuid;

use crate::{engines::sstable::Hint, KvError, KvsEngine};

use super::sstable::{Record, SSTable, Segment, SegmentReader};

enum Storage {
    SSTable(SSTable),
    Segment(Segment),
}

impl Storage {
    pub fn segment(&self) -> Option<&Segment> {
        match self {
            Storage::SSTable(_) => None,
            Storage::Segment(s) => Some(s),
        }
    }

    pub fn sstable(&self) -> Option<&SSTable> {
        match self {
            Storage::SSTable(s) => Some(s),
            Storage::Segment(_) => None,
        }
    }
}

struct Level {
    level: Pin<Box<usize>>,
    directory: Pin<Box<PathBuf>>,
    segments: Pin<Box<[Option<Storage>; 5]>>,
    index: Pin<Box<usize>>,
}

impl Level {
    pub fn new(directory: impl Into<PathBuf>, level: usize) -> crate::Result<Self> {
        let directory = directory.into();
        let reader = BufReader::new(File::open((&directory).join("order"))?);
        let segments = [None; 5];
        let mut line = String::new();
        let mut index = 0;
        loop {
            let size = reader.read_line(&mut line)?;
            if size == 0 {
                break;
            }
            if index >= 5 {
                // TODO: instead of failing here, we may want to just compact our
                // files...
                return Err(KvError::Parse("To many level files found".into()));
            }
            segments[index] = Some(Storage::Segment(Segment::from_log(
                (&directory).join(line),
            )?));
        }
        Ok(Self {
            level: Pin::new(Box::new(level)),
            directory: Pin::new(Box::new(directory)),
            segments: Pin::new(Box::new(segments)),
            index: Pin::new(Box::new(index)),
        })
    }

    pub fn insert(
        &mut self,
        storage: Storage,
        next_path: impl Into<PathBuf>,
    ) -> crate::Result<Option<Segment>> {
        let segment = match storage {
            Storage::SSTable(s) => {
                s.rotate((&self.directory).join(format!("{}.log", Uuid::new_v4())))?
            }
            Storage::Segment(s) => s,
        };
        let segments = *self.segments;
        segments[*self.index] = Some(Storage::Segment(segment));

        // Check to see if we need to rotate the files
        let avalible_segments = self
            .segments
            .iter()
            .fold(5, |count, el| count - if el.is_some() { 1 } else { 0 });

        // do the merge if we are at full capacity
        Ok(if avalible_segments == 5 {
            let merge = self.merge(next_path)?;
            for s in self.segments.iter_mut() {
                *s = None
            }
            Some(merge)
        } else {
            None
        })
    }

    pub fn get(&self, key: &str) -> crate::Result<Option<String>> {
        for level in self.segments.iter() {
            if let Some(segment) = level.as_ref() {
                if let Some(value) = match segment {
                    Storage::SSTable(s) => s.get(key),
                    Storage::Segment(s) => s.get(key)?,
                } {
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    fn merge(&self, path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let mut readers = self
            .segments
            .iter()
            .filter_map(|o| o.as_ref().unwrap().segment())
            .filter_map(|segment| SegmentReader::new(segment).ok())
            .collect::<Vec<SegmentReader>>();

        let mut index = HashMap::new();
        let mut size = 0;
        let segment_path = path.into();
        let writer = BufWriter::new(File::create(&segment_path)?);
        loop {
            for reader in &readers {
                reader.next()?;
            }

            // 1. Find the keys with the lowest
            // remember that files do not have dulicates
            let mut records = readers
                .iter_mut()
                .map(|f| f.peek())
                .filter(|f| f.is_none())
                .collect::<Vec<&mut Option<Record>>>();
            records.sort_by_key(|f| f.as_ref().unwrap().key());
            records.reverse();
            // 2. If records is empty, then we've merged all the files
            if records.is_empty() {
                break;
            }
            // 3. Pop the first value from the record
            let mut next_records = vec![records.pop().unwrap().take().unwrap()];
            for record in records {
                if record.as_ref().unwrap().key() == next_records[0].key() {
                    next_records.push(record.take().unwrap());
                }
            }
            // 4. sort the next records by timestamp
            next_records.sort_by_key(|f| f.timestamp());
            let record = next_records.pop().unwrap();
            // 5. write the value
            let bytes = bincode::serialize(&record)?;
            size += writer.write(&bytes)?;
            let hint = Hint::new(&record, size - record.value().len());
            index.insert(record.key().to_string(), hint);
        }
        Ok(Segment::new(index, segment_path, size))
    }
}

#[derive(Clone)]
struct Levels {
    inner: Arc<RwLock<Vec<Level>>>,
}

impl Levels {
    pub fn new(directory: impl Into<PathBuf>) -> crate::Result<Self> {
        let directory = directory.into(); // parent directory;
        let mut level = 2;
        let mut levels = vec![Level::new(&directory, 1)?];
        loop {
            let lvl_dir = (&directory).join(format!("lv{}", level));
            if !lvl_dir.exists() {
                break;
            }
            levels.push(Level::new(lvl_dir, level)?);
            level += 1;
        }

        Ok(Self {
            inner: Arc::new(RwLock::new(levels)),
        })
    }

    pub fn insert(&self, sstable: SSTable) {
        let inner = self.inner.clone();
        std::thread::spawn(move || {
            // the first level always exists
            let mut index = 0;
            let mut storage = Storage::SSTable(sstable);
            loop {
                let inner = // TODO: plz
                let next = 
                if let Some(segment) = inner.read().unwrap().get(index).unwrap().insert(storage) {
                    storage = segment;
                } else {
                    break;
                }
            }
        });
    }

    pub fn get(&self, key: &str) -> crate::Result<Option<String>> {
        let levels = self.inner.read().unwrap();
        for level in levels.iter() {
            if let Some(value) = level.get(key)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

/// KvStore stores all the data for the kvstore
#[derive(Clone)]
pub struct KvStore {
    directory: Pin<Box<PathBuf>>,
    sstable: Arc<RwLock<SSTable>>,
    levels: Levels,
}

impl KvStore {
    fn write(&self, key: String, value: Option<String>) -> crate::Result<()> {
        let new_size = self.sstable.read().unwrap().append(key, value)?;

        if new_size > 256 * 1000 * 1000 {
            // sstable is too large, rotate
            let sstable = self.sstable.write().unwrap();
            let old_sstable = sstable.clone();
            *sstable = SSTable::new(*self.directory)?;
            drop(sstable);
            self.levels.insert(old_sstable);
        }
        Ok(())
    }

    pub fn add(&self, key: String, value: String) -> crate::Result<()> {
        self.write(key, Some(value))
    }

    pub fn remove(&self, key: String) -> crate::Result<()> {
        self.write(key, None)
    }
}

impl KvsEngine for KvStore {
    fn open(folder: impl Into<PathBuf>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let directory: PathBuf = folder.into();
        if !directory.exists() {
            debug!("Failed to find {:?}; creating it", directory);
            std::fs::create_dir_all(&directory)?;
        } else {
            if !directory.is_dir() {
                debug!("Linked directory {:?} is a file", directory);
                return Err(KvError::Parse(
                    format!("{:?} is not a directory", directory).into(),
                ));
            }
        }

        let levels = Levels::new(directory.as_path())?;
        let sstable = SSTable::new(&directory)?;

        info!("State read, application ready for requests");
        Ok(Self {
            directory: Pin::new(Box::new(directory)),
            sstable: Arc::new(RwLock::new(sstable)),
            levels,
        })
    }

    fn set(&self, key: String, value: String) -> crate::Result<()> {
        self.add(key, value)
    }

    fn get(&self, key: String) -> crate::Result<Option<String>> {
        match self.sstable.read().unwrap().get(&key) {
            Some(value) => Ok(Some(value)),
            None => match self.levels.get(&key)? {
                Some(value) => Ok(Some(value)),
                None => Err(KvError::KeyNotFound(
                    format!("Key {:?} could not be found", key).into(),
                )),
            },
        }
    }

    fn remove(&self, key: String) -> crate::Result<()> {
        self.remove(key)
    }
}
