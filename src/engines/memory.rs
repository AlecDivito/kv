use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use crate::KvsEngine;

/// Key value store that keeps all data in memory
#[derive(Clone)]
pub struct KvInMemoryStore {
    map: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl KvsEngine for KvInMemoryStore {
    fn open(_: impl Into<std::path::PathBuf>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            map: Arc::new(RwLock::new(Default::default())),
        })
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> crate::Result<()> {
        self.map.write().unwrap().insert(key, value);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        Ok(self.map.read().unwrap().get(key).map(Clone::clone))
    }

    fn remove(&self, key: Vec<u8>) -> crate::Result<()> {
        let _ = self.map.write().unwrap().remove(&key);
        Ok(())
    }
}
