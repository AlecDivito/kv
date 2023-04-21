use std::{
    collections::BTreeMap,
    sync::{mpsc, Arc, RwLock},
};

use crate::{datastructures::matcher::prepare, KvsEngine};

use super::{Subscriber, UpdateResult};

/// Key value store that keeps all data in memory
#[derive(Clone)]
pub struct KvInMemoryStore {
    map: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    subscribers: Arc<RwLock<Vec<Arc<Subscriber>>>>,
}

impl KvInMemoryStore {
    /// Create a new in memory key value store
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(BTreeMap::new())),
            subscribers: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl Default for KvInMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvsEngine for KvInMemoryStore {
    fn restore(_: impl Into<std::path::PathBuf>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self::new())
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> crate::Result<()> {
        self.map.write().unwrap().insert(key, value);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        Ok(self.map.read().unwrap().get(key).map(Clone::clone))
    }

    fn find(&self, like: Vec<u8>) -> crate::Result<Vec<Vec<u8>>> {
        let mut keys = vec![];
        let tester = prepare(like);
        let read = self.map.read().unwrap();

        for key in read.keys() {
            if tester.test(key) {
                keys.push(key.to_vec());
            }
        }

        Ok(keys)
    }

    fn remove(&self, key: Vec<u8>) -> crate::Result<()> {
        let _ = self.map.write().unwrap().remove(&key);
        Ok(())
    }

    fn subscribe(&self, subscriber: Subscriber) -> crate::Result<()> {
        self.subscribers.write().unwrap().push(subscriber);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{KvInMemoryStore, KvsEngine};

    #[test]
    fn find_keys() {
        let kv = KvInMemoryStore::new();
        let test_keys = vec![
            b"that".to_vec(),
            b"them".to_vec(),
            b"this".to_vec(),
            b"those".to_vec(),
            b"thought".to_vec(),
        ];
        for (index, key) in test_keys.iter().enumerate() {
            kv.set(key.clone(), format!("value{}", index).into_bytes())
                .unwrap();
        }
        let keys = kv.find(b"th*".to_vec()).unwrap();
        assert_eq!(keys, test_keys);
    }
}
