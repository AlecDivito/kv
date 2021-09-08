use std::{ffi::OsStr, path::PathBuf, pin::Pin, sync::Arc};

use tokio::{fs::read_dir, sync::RwLock};

use crate::{
    common::now,
    engines::kvs::segment::{Segment, SegmentReader},
};

use super::sstable::SSTable;

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
    pub async fn new(directory: impl Into<PathBuf>, level: usize) -> crate::Result<Self> {
        debug!("Finding all files being added to level {}", level);
        let directory = directory.into();
        let mut dirs = read_dir(&directory).await?;
        let mut log_paths = vec![];
        while let Some(entry) = dirs.next_entry().await? {
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
            segments.push(Storage::Segment(Segment::from_log(path).await?));
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
    pub async fn update_level(
        &self,
        next_path: impl Into<PathBuf>,
    ) -> crate::Result<Option<Segment>> {
        let segments = self.segments.read().await;
        let length = segments.len();
        if let Some((index, table)) = segments.iter().enumerate().find_map(|(u, s)| {
            if let Some(t) = s.sstable() {
                Some((u, t))
            } else {
                None
            }
        }) {
            let new_segment = table
                .save(self.directory.join(format!("{}.log", now())))
                .await?;
            trace!("Created new {} from {}", new_segment, table);
            let length = segments.len();
            drop(segments);
            self.segments.write().await[index] = Storage::Segment(new_segment);
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
            let merge = self.merge(next_path).await?;
            Some(merge)
        } else {
            None
        })
    }

    pub async fn add(&self, storage: Storage) -> crate::Result<()> {
        trace!(
            "Adding {} to {:?}",
            storage,
            self.segments.read().await.len()
        );
        self.segments.write().await.push(storage);
        Ok(())
    }

    pub async fn get(&self, key: &str) -> crate::Result<Option<String>> {
        for level in self.segments.read().await.iter().rev() {
            if let Some(value) = match level {
                Storage::SSTable(s) => s.get(key).await,
                Storage::Segment(s) => s.get(key).await?,
            } {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    async fn merge(&self, path: impl Into<PathBuf>) -> crate::Result<Segment> {
        let segment_path = path.into().join(format!("{}.log", now()));
        // get all of the relavent segments

        // partition the segments and indexies
        let segments_lock = self.segments.read().await;
        let mut segment_readers = Vec::with_capacity(segments_lock.len());
        let mut indexies = Vec::with_capacity(segments_lock.len());
        for (index, storage) in segments_lock.iter().enumerate() {
            if let Storage::Segment(segment) = storage {
                segment_readers.push(SegmentReader::new(segment).await?);
                indexies.push(index);
            }
        }
        indexies.sort();
        drop(segments_lock);

        // attempt the merging processes
        let segment = Segment::from_segments(segment_path, segment_readers).await?;

        // on successful compaction, remove the segments touched
        let mut lock = self.segments.write().await;
        for index in indexies.iter().rev() {
            if let Storage::Segment(segment) = lock.get_mut(*index).unwrap() {
                segment.mark_for_removal();
                lock.remove(*index);
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
    pub async fn new(directory: impl Into<PathBuf>) -> crate::Result<Self> {
        let directory = directory.into(); // parent directory;
        let mut level = 2;
        let mut levels = vec![Level::new(&directory, 1).await?];
        loop {
            let lvl_dir = (&directory).join(format!("lv{}", level));
            if !lvl_dir.exists() {
                break;
            }
            levels.push(Level::new(lvl_dir, level).await?);
            level += 1;
        }

        Ok(Self {
            inner: Arc::new(RwLock::new(levels)),
            directory: Arc::new(RwLock::new(directory)),
        })
    }

    pub async fn try_merge(&self) -> crate::Result<()> {
        let directory = (self.directory.read().await).clone();
        let mut index = 0;
        let mut level_index = 2;
        let mut new_segment_file = None;

        loop {
            let next_path = (&*directory).join(format!("lv{}", level_index));

            if !next_path.exists() {
                trace!("level folder does not exist. Creating {:?}", &next_path);
                std::fs::create_dir(&next_path)?;
            }
            let inner = self.inner.read().await;
            let level = match inner.get(index) {
                Some(level) => level.clone(),
                None => {
                    drop(inner);
                    let level = Level::new(&*directory, level_index).await?;
                    self.inner.write().await.push(level.clone());
                    level
                }
            };
            if let Some(segment) = new_segment_file.take() {
                trace!("Attempting to merge index level {}", index);
                level.add(Storage::Segment(segment)).await?;
            }
            new_segment_file = level.update_level(next_path).await?;
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

    pub async fn get(&self, key: &str) -> crate::Result<Option<String>> {
        let levels = self.inner.read().await;
        for level in levels.iter() {
            if let Some(value) = level.get(key).await? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub async fn add_table(&self, sstable: SSTable) -> crate::Result<()> {
        self.inner.read().await[0]
            .add(Storage::SSTable(sstable))
            .await?;
        Ok(())
    }
}
