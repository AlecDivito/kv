use std::path::PathBuf;

use super::KvsEngine;
use crate::{GenericError, KvError, Result};
use sled::{open, Db, Tree};

/// Implementation of Sled Key Value Store
#[derive(Clone)]
pub struct SledKvsEngine(Db);

impl KvsEngine for SledKvsEngine {
    fn open(folder: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        Ok(SledKvsEngine(open(folder.into())?))
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.insert(key, value).map(|_| ())?;
        tree.flush()?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let tree: &Tree = &self.0;
        let value = tree.get(key)?;
        Ok(value.map(|inner| inner.to_vec()))
        // .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
        // .map(String::from_utf8)
        // .transpose()
    }

    fn find(&self, _like: Vec<u8>) -> Result<Vec<Vec<u8>>> {
        todo!()
    }

    fn remove(&self, key: Vec<u8>) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.remove(key)?
            .ok_or(KvError::KeyNotFound(GenericError::new(
                "Key could not be found inside database",
            )))?;
        tree.flush()?;
        Ok(())
    }
}
