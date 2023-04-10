use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::{KvError, KvsEngine};

use self::{level::Levels, sstable::SSTable};

mod level;
mod sstable;

/// KvStore stores all the data for the kvstore
#[derive(Clone)]
pub struct KvStore {
    directory: Arc<PathBuf>,
    sstable: Arc<RwLock<SSTable>>,
    levels: Levels,
}

impl KvStore {
    fn write(&self, key: String, value: Option<String>) -> crate::Result<()> {
        let new_size = self.sstable.read().unwrap().append(key, value)?;

        if new_size > 256 * 1000 * 1000 {
            // sstable is too large, rotate
            let new_sstable = SSTable::new(&*self.directory)?;
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
        } else if !directory.is_dir() {
            debug!("Linked directory {:?} is a file", directory);
            return Err(KvError::Parse(
                format!("{:?} is not a directory", directory).into(),
            ));
        }

        let dir = std::fs::read_dir(&directory)?;
        let mut redo_log_path = None;
        for entry in dir {
            let entry = entry?;
            if let Some(s) = entry.path().extension() {
                if s == "redo" {
                    trace!("Found redo log: {:?}", entry.path());
                    // TODO: If we find multiple redo logs on startup, we should
                    // just compress them right now. At least we should include
                    // an option for the user to submit.
                    redo_log_path = Some(entry.path());
                    break;
                }
            }
        }

        let levels = Levels::new(directory.as_path())?;
        let sstable = match redo_log_path {
            Some(file) => SSTable::from_write_ahead_log(file),
            None => SSTable::new(&directory),
        }?;

        info!("State read, application ready for requests");
        Ok(Self {
            directory: Arc::new(directory),
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
