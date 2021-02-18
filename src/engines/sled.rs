use super::KvsEngine;
use crate::{GenericError, KvError, Result};
use sled::{Db, Tree};

/// Implementation of Sled Key Value Store
#[derive(Clone)]
pub struct SledKvsEngine(Db);

impl SledKvsEngine {
    /// Creates a `SledKvsEngine` from `sled::Db`
    pub fn new(db: Db) -> Self {
        SledKvsEngine(db)
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.insert(key, value.into_bytes()).map(|_| ())?;
        tree.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.0;
        Ok(tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.remove(key)?
            .ok_or(KvError::KeyNotFound(GenericError::new(
                "Key could not be found inside database",
            )))?;
        tree.flush()?;
        Ok(())
    }
}
