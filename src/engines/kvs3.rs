use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
};

use crate::{common::now, engines::sstable::Hint, KvError, KvsEngine};

use super::sstable::{Record, SSTable, Segment, SegmentReader};

#[derive(Debug)]
pub enum Storage {
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

impl std::fmt::Display for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Storage::SSTable(s) => write!(f, "Storage({})", s),
            Storage::Segment(s) => write!(f, "Storage({})", s),
        }
    }
}

#[derive(Clone)]
pub struct Level {
    level: Pin<Box<usize>>,
    directory: Pin<PathBuf>,
    segments: Arc<RwLock<Vec<Storage>>>,
}

impl Level {
    pub fn new(directory: impl Into<PathBuf>, level: usize) -> crate::Result<Self> {
        debug!("Finding all files being added to level {}", level);
        let directory = directory.into();
        let dirs = std::fs::read_dir(&directory)?;
        let mut log_paths = vec![];
        for entry in dirs {
            let entry = entry?;
            if entry.path().is_dir() {
                continue;
            }
            let path = entry.path();
            if path.extension().unwrap_or(OsStr::new("")) != "log" {
                continue;
            }
            trace!("Added {:?} to level {}", entry.path(), level);
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

        trace!("Logs are sorted {:?}", log_paths);
        let mut segments = vec![];
        for path in log_paths {
            segments.push(Storage::Segment(Segment::from_log(path)?));
        }

        debug!("Level {} indices set {:?}", level, segments);
        Ok(Self {
            directory: Pin::new(directory),
            level: Pin::new(Box::new(level)),
            segments: Arc::new(RwLock::new(segments)),
        })
    }

    /// Update level mainly does 2 operations. The first is to find any SSTable
    /// and convert it into a Segment with an index. After which, it will resave
    /// it to the level as a segment.
    ///
    /// With the level having the correct state, it then tries to merge it's file
    /// if, and only if, it reaches the given threshold.
    pub fn update_level(&self, next_path: impl Into<PathBuf>) -> crate::Result<Option<Segment>> {
        let segments = self.segments.try_read()?;
        let length = segments.len();
        if let Some((index, table)) = segments.iter().enumerate().find_map(|(u, s)| {
            if let Some(t) = s.sstable() {
                Some((u, t))
            } else {
                None
            }
        }) {
            let new_segment = table.save(self.directory.join(format!("{}.log", now())))?;
            trace!("Created new {} from {}", new_segment, table);
            let length = segments.len();
            drop(segments);
            self.segments.write().unwrap()[index] = Storage::Segment(new_segment);
            trace!(
                "Level {} segments have been updated to {}",
                self.level,
                length
            );
        } else {
            drop(segments);
        }

        trace!("Level {}: Segments before merge {}", self.level, length);
        Ok(if length > clamp(10 * (*self.level), 2) {
            let merge = self.merge(next_path)?;
            Some(merge)
        } else {
            None
        })
    }

    pub fn add(&self, storage: Storage) -> crate::Result<()> {
        trace!(
            "Adding {} to {:?}",
            storage,
            self.segments.try_read()?.len()
        );
        self.segments.write().unwrap().push(storage);
        Ok(())
    }

    pub fn get(&self, key: &str) -> crate::Result<Option<String>> {
        for level in self.segments.try_read()?.iter().rev() {
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
        let segment_path = path.into().join(format!("{}.log", now()));
        info!("Merging level {} into {:?}", self.level, &segment_path);
        let segments_lock = self.segments.read().unwrap();
        let mut segments = segments_lock
            .iter()
            .enumerate()
            .filter(|(_, s)| s.segment().is_some())
            .map(|(index, s)| (index, SegmentReader::new(s.segment().unwrap())))
            .filter(|(_, reader)| reader.is_ok())
            .map(|(index, reader)| (index, reader.unwrap()))
            .collect::<Vec<(usize, SegmentReader)>>();
        trace!(
            "Of {} segments, found {} which are readable",
            segments_lock.len(),
            segments.len()
        );
        drop(segments_lock);

        let mut index = HashMap::new();
        let mut size = 0;
        let mut writer = BufWriter::new(File::create(&segment_path)?);
        loop {
            for reader in segments.iter_mut() {
                reader.1.next()?;
            }

            // 1. Find the keys with the lowest
            // remember that files do not have dulicates
            let mut records = segments
                .iter_mut()
                .filter(|f| f.1.value.is_some())
                .map(|f| &mut f.1.value)
                .collect::<Vec<&mut Option<Record>>>();
            records.sort_by_key(|f| f.as_ref().unwrap().key().to_string());
            records.reverse();

            // 2. If records is empty, then we've merged all the files
            if records.is_empty() {
                trace!("Records is empty. All files have successfully been compacted");
                break;
            }

            // 3. Pop the first value from the record
            let lowest = records.pop().unwrap().take().unwrap();
            trace!("Lowest record found was {}", lowest);
            let mut next_records = vec![lowest];
            for record in records {
                if record.as_ref().unwrap().key() == next_records[0].key() {
                    let r = record.take().unwrap();
                    trace!("Found same record: {}", r);
                    next_records.push(r);
                }
            }

            // 4. sort the next records by timestamp, save the one with the highest timestamp
            next_records.sort_by_key(|f| f.timestamp());
            let record = next_records.pop().unwrap();
            trace!("Appending {} to {:?}", record, &segment_path);

            // 5. write the value
            let bytes = bincode::serialize(&record)?;
            size += writer.write(&bytes)?;
            let hint = Hint::new(&record, size - record.value().len());
            index.insert(record.key().to_string(), hint);
        }

        let mut indexies = segments.iter().map(|i| &i.0).collect::<Vec<&usize>>();
        trace!(
            "Level {}: Removing old indices from segments {:?}",
            self.level,
            indexies
        );
        indexies.sort();

        let mut lock = self.segments.write().unwrap();
        for index in indexies.iter().rev() {
            trace!(
                "Level {}: Removing segment {}",
                self.level,
                lock.get(**index).unwrap()
            );
            lock.remove(**index);
        }
        drop(lock);

        for reader in segments.iter_mut() {
            reader.1.complete();
        }

        // When segment readers drop, we delete the files they we're reading.
        Ok(Segment::new(index, segment_path, size))
    }
}

fn clamp(level: usize, min: usize) -> usize {
    if level < min {
        min
    } else {
        level
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

    pub fn try_merge(&self) -> crate::Result<()> {
        let directory = (self.directory.read()?).clone();
        let mut index = 0;
        let mut level_index = 2;
        let mut new_segment_file = None;

        loop {
            let next_path = (&*directory).join(format!("lv{}", level_index));

            if !next_path.exists() {
                trace!("level folder does not exist. Creating {:?}", &next_path);
                std::fs::create_dir(&next_path)?;
            }
            let inner = self.inner.read()?;
            let level = match inner.get(index) {
                Some(level) => level.clone(),
                None => {
                    drop(inner);
                    let level = Level::new(&*directory, level_index)?;
                    self.inner.write()?.push(level.clone());
                    level
                }
            };
            if let Some(segment) = new_segment_file.take() {
                trace!("Attempting to merge index level {}", index);
                level.add(Storage::Segment(segment))?;
            }
            new_segment_file = level.update_level(next_path)?;
            if new_segment_file.is_none() {
                info!(
                    "Stopping merging at index level {} because no more merging is needed",
                    index
                );
                return Ok(());
            } else {
                info!(
                    "New segment file has been pushed to index {}. Continueing merge.",
                    index
                );
            }

            level_index = level_index + 1;
            index = index + 1;
        }
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

    pub fn add_table(&self, sstable: SSTable) -> crate::Result<()> {
        self.inner.read().unwrap()[0].add(Storage::SSTable(sstable))?;
        Ok(())
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
            let directory = &*self.directory.read().unwrap();
            let new_sstable = SSTable::new(directory)?;
            let mut sstable = self.sstable.write().unwrap();
            let old_sstable = std::mem::replace(&mut *sstable, new_sstable);
            drop(sstable);
            self.levels.add_table(old_sstable)?;
            let levels = self.levels.clone();
            std::thread::spawn(move || {
                if let Err(e) = levels.try_merge() {
                    error!("Failed to succesfully merge with error {}", e)
                } else {
                    info!("Successfully merged levels together");
                }
            });
        }
        Ok(())
    }

    /// Add a value to our key value store
    pub fn add(&self, key: String, value: String) -> crate::Result<()> {
        self.write(key, Some(value))
    }

    /// remove a value from our key value store
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

        let dir = std::fs::read_dir(&directory)?;
        let mut redo_log_path = None;
        for entry in dir {
            let entry = entry?;
            if let Some(s) = entry.path().extension() {
                if s == "redo" {
                    trace!("Found redo log: {:?}", entry.path());
                    redo_log_path = Some(PathBuf::from(entry.path()));
                    break;
                }
            }
        }

        let levels = Levels::new(directory.as_path())?;
        let sstable = match redo_log_path {
            Some(file) => SSTable::from_write_ahead_log(&file),
            None => SSTable::new(&directory),
        }?;

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
