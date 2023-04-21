use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::{datastructures::matcher::prepare, KvError, KvsEngine};

use self::{config::Config, level::Levels, sstable::SSTable};

mod config;
mod level;
mod sstable;

/// KvStore stores all the data for the kvstore
#[derive(Clone)]
pub struct KvStore {
    config: Arc<Config>,
    sstable: Arc<RwLock<SSTable>>,
    levels: Levels,
}

impl KvStore {
    /// Create or restore a key value store. Given a folder location.
    pub fn new(folder: impl Into<PathBuf>) -> crate::Result<Self> {
        let config = Config::new(folder);
        config.init()?;
        let sstable = config.restore_wal()?;
        let levels = config.restore_levels()?;

        info!("State read, application ready for requests");

        Ok(Self {
            config: Arc::new(config),
            sstable: Arc::new(RwLock::new(sstable)),
            levels,
        })
    }

    fn write(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> crate::Result<()> {
        let new_size = self.sstable.read().unwrap().append(key, value)?;

        if self.config.should_rotate_wal(new_size) {
            // sstable is too large, rotate
            let mut sstable = self.sstable.write().unwrap();
            let old_sstable = self.config.replace_wal_inplace(&mut sstable)?;
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
    pub fn add(&self, key: Vec<u8>, value: Vec<u8>) -> crate::Result<()> {
        self.write(key, Some(value))
    }

    /// remove a value from our key value store
    pub fn remove(&self, key: Vec<u8>) -> crate::Result<()> {
        self.write(key, None)
    }
}

impl KvsEngine for KvStore {
    fn restore(folder: impl Into<PathBuf>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Self::new(folder)
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> crate::Result<()> {
        self.add(key, value)
    }

    fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        match self.sstable.read().unwrap().get(key) {
            Some(value) => Ok(Some(value)),
            None => match self.levels.get(key)? {
                Some(value) => Ok(Some(value)),
                None => Err(KvError::KeyNotFound(
                    format!("Key {:?} could not be found", key).into(),
                )),
            },
        }
    }

    fn find(&self, key: Vec<u8>) -> crate::Result<Vec<Vec<u8>>> {
        let pattern = prepare(key);
        let recent_keys = self.sstable.read().unwrap().find(&pattern);
        let mut keys = self.levels.find(&pattern)?;
        for key in recent_keys {
            keys.insert(key);
        }
        Ok(keys.into_iter().collect::<Vec<_>>())
    }

    fn remove(&self, key: Vec<u8>) -> crate::Result<()> {
        self.remove(key)
    }
}
