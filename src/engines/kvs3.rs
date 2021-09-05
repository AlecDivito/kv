use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
};

use crate::{common::now, engines::sstable::Hint, KvError, KvsEngine};

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
    directory: Pin<PathBuf>,
    segments: Pin<Box<Vec<Storage>>>,
}

impl Level {
    pub fn new(directory: impl Into<PathBuf>, level: usize) -> crate::Result<Self> {
        let directory = directory.into();
        let dirs = std::fs::read_dir(&directory)?;
        let mut log_paths = vec![];
        for entry in dirs {
            let entry = entry?;
            if entry.path().is_dir() {
                continue;
            }
            if !entry.path().ends_with("log") {
                continue;
            }
            log_paths.push(entry.path());
        }
        // sort log paths by their file stem number
        log_paths.sort_by_key(|f| {
            f.file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u128>()
                .unwrap()
        });

        let mut segments = vec![];
        for path in log_paths {
            segments.push(Storage::Segment(Segment::from_log(path)?));
        }

        Ok(Self {
            directory: Pin::new(directory),
            level: Pin::new(Box::new(level)),
            segments: Pin::new(Box::new(segments)),
        })
    }

    pub fn save_sstables(
        &mut self,
        next_path: impl Into<PathBuf>,
    ) -> crate::Result<Option<Segment>> {
        for segment in self.segments.iter_mut() {
            if let Storage::SSTable(s) = segment {
                let new_segment = s.save(self.directory.join(format!("{}.log", now())))?;
                *segment = Storage::Segment(new_segment);
            };
        }

        Ok(if self.segments.len() >= 5 {
            let merge = self.merge(next_path)?;
            self.segments.clear();
            Some(merge)
        } else {
            None
        })
    }

    pub fn add(&mut self, storage: Storage) {
        self.segments.push(storage);
    }

    pub fn get(&self, key: &str) -> crate::Result<Option<String>> {
        for level in self.segments.iter().rev() {
            if let Some(value) = match level {
                Storage::SSTable(s) => s.get(key),
                Storage::Segment(s) => s.get(key)?,
            } {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    fn merge(&self, path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let mut readers = self
            .segments
            .iter()
            .filter_map(|o| o.segment())
            .filter_map(|segment| SegmentReader::new(segment).ok())
            .collect::<Vec<SegmentReader>>();

        let mut index = HashMap::new();
        let mut size = 0;
        let segment_path = path.into();
        let mut writer = BufWriter::new(File::create(&segment_path)?);
        loop {
            for reader in &mut readers {
                reader.next()?;
            }

            // 1. Find the keys with the lowest
            // remember that files do not have dulicates
            let mut records = readers
                .iter_mut()
                .map(|f| f.peek())
                .filter(|f| f.is_none())
                .collect::<Vec<&mut Option<Record>>>();
            records.sort_by_key(|f| f.as_ref().unwrap().key().to_string());
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

            // 4. sort the next records by timestamp, save the one with the highest timestamp
            next_records.sort_by_key(|f| f.timestamp());
            let record = next_records.pop().unwrap();

            // 5. write the value
            let bytes = bincode::serialize(&record)?;
            size += writer.write(&bytes)?;
            let hint = Hint::new(&record, size - record.value().len());
            index.insert(record.key().to_string(), hint);
        }

        // When segment readers drop, we delete the files they we're reading.
        Ok(Segment::new(index, segment_path, size))
    }
}

#[derive(Clone)]
struct Levels {
    inner: Arc<RwLock<Vec<Level>>>,
    directory: Arc<RwLock<PathBuf>>,
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
            directory: Arc::new(RwLock::new(directory)),
        })
    }

    pub fn insert(&self, sstable: SSTable) {
        self.inner.write().unwrap()[0].add(Storage::SSTable(sstable));
        let inner = self.inner.clone();
        let directory = self.directory.clone();
        std::thread::spawn(move || {
            let directory = directory.read().unwrap();
            let mut index = 0;
            let mut level_index = 2;
            let mut next_path = (&directory).join(format!("lv{}", level_index));
            if !next_path.exists() {
                std::fs::create_dir(&next_path).unwrap();
            }
            let mut new_segment_file = inner.write().unwrap()[0].save_sstables(&next_path).unwrap();
            loop {
                if new_segment_file.is_none() {
                    return;
                }
                let mut inner = inner.read().unwrap();
                let level = match inner.get(index) {
                    Some(level) => level,
                    None => {
                        let level = Level::new(&*directory, level_index).unwrap();
                        inner.push(level);
                        inner.get(index).unwrap()
                    }
                };
                level_index += 1;
                index += 1;

                next_path = (&*directory).join(format!("lv{}", level_index));
                level.add(Storage::Segment(new_segment_file.take().unwrap()));
                new_segment_file = level.save_sstables(next_path).unwrap();
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
    directory: Arc<RwLock<PathBuf>>,
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
            *sstable = SSTable::new(*self.directory.read().unwrap())?;
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
            directory: Arc::new(RwLock::new(directory)),
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
