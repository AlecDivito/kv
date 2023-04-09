use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use crate::KvsEngine;

/// Key value store that keeps all data in memory
#[derive(Clone)]
pub struct KvInMemoryStore {
    map: Arc<RwLock<BTreeMap<String, String>>>,
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

    fn set(&self, key: String, value: String) -> crate::Result<()> {
        self.map.write().unwrap().insert(key, value);
        Ok(())
    }

    fn get(&self, key: String) -> crate::Result<Option<String>> {
        Ok(self.map.read().unwrap().get(&key).map(Clone::clone))
    }

    fn remove(&self, key: String) -> crate::Result<()> {
        let _ = self.map.write().unwrap().remove(&key);
        Ok(())
    }
}
