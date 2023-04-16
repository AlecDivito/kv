use std::{
    collections::HashSet,
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use crate::{common::now, datastructures::matcher::PreparedPattern};

use super::sstable::{SSTable, Segment, SegmentReader};

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
    inner: Arc<RwLock<Lvl>>,
}

struct Lvl {
    level: usize,
    dir: PathBuf,
    segments: Vec<Storage>,
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
            inner: Arc::new(RwLock::new(Lvl {
                dir: directory,
                level,
                segments,
            })),
        })
    }

    /// Update level mainly does 2 operations. The first is to find any SSTable
    /// and convert it into a Segment with an index. After which, it will resave
    /// it to the level as a segment.
    ///
    /// With the level having the correct state, it then tries to merge it's file
    /// if, and only if, it reaches the given threshold.
    pub fn update_level(&self, next_path: impl AsRef<Path>) -> crate::Result<Option<Segment>> {
        let lock = self.inner.read().unwrap();
        let length = lock.segments.len();
        let level = lock.level;

        if let Some((index, table)) = lock
            .segments
            .iter()
            .enumerate()
            .find_map(|(u, s)| s.sstable().map(|t| (u, t)))
        {
            let new_segment = table.save(lock.dir.join(format!("{}.log", now())))?;
            trace!("Created new {} from {}", new_segment, table);
            let length = lock.segments.len();
            drop(lock);
            self.inner.write().unwrap().segments[index] = Storage::Segment(new_segment);
            trace!("Level {} segments have been updated to {}", level, length);
        } else {
            drop(lock);
        }

        trace!("Level {}: Segments before merge {}", level, length);
        Ok(if length > clamp(10 * level, 2) {
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
            self.inner.read().unwrap().segments.len()
        );
        self.inner.write().unwrap().segments.push(storage);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        for level in self.inner.read().unwrap().segments.iter().rev() {
            if let Some(value) = match level {
                Storage::SSTable(s) => s.get(key),
                Storage::Segment(s) => s.get(key)?,
            } {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn find(&self, pattern: &PreparedPattern) -> crate::Result<Vec<Vec<u8>>> {
        let mut keys = std::collections::HashSet::new();
        for level in self.inner.read().unwrap().segments.iter().rev() {
            let new_keys = match level {
                Storage::SSTable(s) => s.find(pattern),
                Storage::Segment(s) => s.find(pattern)?,
            };
            for key in new_keys {
                keys.insert(key);
            }
        }
        let keys = keys.into_iter().collect::<Vec<_>>();
        Ok(keys)
    }

    fn merge(&self, path: impl AsRef<Path>) -> crate::Result<Segment> {
        let segment_path = path.as_ref().join(format!("{}.log", now()));
        // get all of the relavent segments
        let lock = self.inner.read().unwrap();
        let storage_segments = lock
            .segments
            .iter()
            .enumerate()
            .filter(|(_, s)| s.segment().is_some())
            .collect::<Vec<(usize, &Storage)>>();
        // partition the segments and indexies
        let segment_readers: Vec<SegmentReader> = storage_segments
            .iter()
            .filter_map(|(_, s)| s.segment())
            .filter_map(|s| SegmentReader::new(s).ok())
            .collect();
        let mut indexies = storage_segments.iter().map(|i| i.0).collect::<Vec<usize>>();
        indexies.sort();
        drop(lock);

        // attempt the merging processes
        let segment = Segment::from_segments(segment_path, segment_readers)?;

        // on successful compaction, remove the segments touched
        let mut lock = self.inner.write().unwrap();
        for index in indexies.iter().rev() {
            if let Storage::Segment(segment) = lock.segments.get_mut(*index).unwrap() {
                segment.mark_for_removal();
                lock.segments.remove(*index);
            }
        }
        drop(lock);

        Ok(segment)
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
pub struct Levels {
    inner: Arc<RwLock<Vec<Level>>>,
    directory: Arc<RwLock<PathBuf>>,
}

impl Levels {
    pub fn new(directory: impl Into<PathBuf>) -> crate::Result<Self> {
        let directory = directory.into(); // parent directory;
        let mut level = 2;
        let mut levels = vec![Level::new(&directory, 1)?];
        loop {
            let lvl_dir = directory.join(format!("lv{}", level));
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
        let directory = (self.directory.read().unwrap()).clone();
        let mut index = 0;
        let mut level_index = 2;
        let mut new_segment_file = None;

        loop {
            let next_path = (*directory).join(format!("lv{}", level_index));

            if !next_path.exists() {
                trace!("level folder does not exist. Creating {:?}", &next_path);
                std::fs::create_dir(&next_path)?;
            }
            let inner = self.inner.read().unwrap();
            let level = match inner.get(index) {
                Some(level) => level.clone(),
                None => {
                    drop(inner);
                    let level = Level::new(&*directory, level_index)?;
                    self.inner.write().unwrap().push(level.clone());
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

            level_index += 1;
            index += 1;
        }
    }

    pub fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        let levels = self.inner.read().unwrap();
        for level in levels.iter() {
            if let Some(value) = level.get(key)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn find(&self, pattern: &PreparedPattern) -> crate::Result<HashSet<Vec<u8>>> {
        let mut keys = HashSet::new();
        let levels = self.inner.read().unwrap();
        for level in levels.iter() {
            for new_key in level.find(pattern)? {
                keys.insert(new_key);
            }
        }
        Ok(keys)
    }

    pub fn add_table(&self, sstable: SSTable) -> crate::Result<()> {
        self.inner.read().unwrap()[0].add(Storage::SSTable(sstable))?;
        Ok(())
    }
}
