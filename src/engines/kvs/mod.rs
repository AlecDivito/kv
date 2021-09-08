use std::{path::PathBuf, sync::Arc};

use tokio::sync::RwLock;

use crate::{engines::kvs::level::Levels, KvError, KvsEngine};

use self::sstable::SSTable;

pub use self::level::{Level, Storage};

pub mod level;
mod record;
mod segment;
mod sstable;

/// KvStore stores all the data for the kvstore
#[derive(Clone)]
pub struct KvStore {
    directory: Arc<RwLock<PathBuf>>,
    sstable: Arc<RwLock<SSTable>>,
    levels: Levels,
}

impl KvStore {
    async fn write(&self, key: String, value: Option<String>) -> crate::Result<()> {
        let new_size = self.sstable.read().await.append(key, value).await?;

        if new_size > 256 * 1000 * 1000 {
            // sstable is too large, rotate
            let directory = &*self.directory.read().await;
            let new_sstable = SSTable::new(directory).await?;
            let mut sstable = self.sstable.write().await;
            let old_sstable = std::mem::replace(&mut *sstable, new_sstable);
            drop(sstable);
            self.levels.add_table(old_sstable).await?;
            let levels = self.levels.clone();
            let _ = tokio::spawn(async move {
                let result = levels.try_merge().await;
                if let Err(e) = &result {
                    error!("Failed to succesfully merge with error {}", e)
                } else {
                    info!("Successfully merged levels together");
                }
                result
            });
        }
        Ok(())
    }

    /// Add a value to our key value store
    pub async fn add(&self, key: String, value: String) -> crate::Result<()> {
        self.write(key, Some(value)).await
    }

    /// remove a value from our key value store
    pub async fn remove(&self, key: String) -> crate::Result<()> {
        self.write(key, None).await
    }
}

#[async_trait::async_trait]
impl KvsEngine for KvStore {
    async fn open(folder: PathBuf) -> crate::Result<Self>
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
                    // TODO: If we find multiple redo logs on startup, we should
                    // just compress them right now. At least we should include
                    // an option for the user to submit.
                    redo_log_path = Some(PathBuf::from(entry.path()));
                    break;
                }
            }
        }

        let levels = Levels::new(directory.as_path()).await?;
        let sstable = match redo_log_path {
            Some(file) => SSTable::from_write_ahead_log(&file).await,
            None => SSTable::new(&directory).await,
        }?;

        info!("State read, application ready for requests");
        Ok(Self {
            directory: Arc::new(RwLock::new(directory)),
            sstable: Arc::new(RwLock::new(sstable)),
            levels,
        })
    }

    async fn set(&self, key: String, value: String) -> crate::Result<()> {
        self.add(key, value).await?;
        Ok(())
    }

    async fn get(&self, key: String) -> crate::Result<Option<String>> {
        match self.sstable.read().await.get(&key).await {
            Some(value) => Ok(Some(value)),
            None => match self.levels.get(&key).await? {
                Some(value) => Ok(Some(value)),
                None => Err(KvError::KeyNotFound(
                    format!("Key {:?} could not be found", key).into(),
                )),
            },
        }
    }

    async fn remove(&self, key: String) -> crate::Result<()> {
        self.remove(key).await?;
        Ok(())
    }
}
