use std::path::PathBuf;

use crate::KvError;

use super::{level::Levels, sstable::SSTable};

const DEFAULT_WAL_SIZE: usize = 256 * 1000 * 1000;

pub struct Config {
    folder: PathBuf,
    max_wal_size: usize,
}

impl Config {
    /// Create a new config for the key value store
    pub fn new(folder: impl Into<PathBuf>) -> Self {
        let max_wal_size = std::env::var("KV_MAX_LOG_SIZE")
            .map(|v| v.parse::<usize>().unwrap_or(DEFAULT_WAL_SIZE))
            .unwrap_or(DEFAULT_WAL_SIZE);
        trace!("KV_MAX_WAL_SIZE set to {}", max_wal_size);
        Self {
            folder: folder.into(),
            max_wal_size,
        }
    }

    /// Create directory for database to execute in
    pub fn init(&self) -> crate::Result<()> {
        if !self.folder.exists() {
            debug!("Failed to find {:?}; creating it", self.folder);
            std::fs::create_dir_all(&self.folder)?;
        } else if !self.folder.is_dir() {
            debug!("Linked directory {:?} is a file", self.folder);
            return Err(KvError::Parse(
                format!("{:?} is not a directory", self.folder).into(),
            ));
        }

        Ok(())
    }

    /// Find a redo log in the database directory and return the path to it
    pub fn restore_wal(&self) -> crate::Result<SSTable> {
        let path = self.find_redo_log()?;
        match path {
            Some(file) => SSTable::from_write_ahead_log(file),
            None => SSTable::new(&self.folder),
        }
    }

    pub fn restore_levels(&self) -> crate::Result<Levels> {
        Levels::new(self.folder.as_path())
    }

    pub fn replace_wal_inplace(&self, dest: &mut SSTable) -> crate::Result<SSTable> {
        let new = SSTable::new(&self.folder)?;
        Ok(std::mem::replace(dest, new))
    }

    pub fn should_rotate_wal(&self, size: usize) -> bool {
        size > self.max_wal_size
    }

    fn find_redo_log(&self) -> crate::Result<Option<PathBuf>> {
        let dir = std::fs::read_dir(&self.folder)?;
        for entry in dir {
            let entry = entry?;
            if let Some(s) = entry.path().extension() {
                if s == "redo" {
                    trace!("Found redo log: {:?}", entry.path());
                    // TODO: If we find multiple redo logs on startup, we should
                    // just compress them right now. At least we should include
                    // an option for the user to submit.
                    return Ok(Some(entry.path()));
                }
            }
        }
        Ok(None)
    }
}
