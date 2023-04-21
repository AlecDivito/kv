//! This module provides various key value storage engines
//!

use std::path::PathBuf;
use std::sync::mpsc;

use crate::datastructures::matcher::{prepare, PreparedPattern};
use crate::Result;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Update {
    Set { key: Vec<u8>, value: Vec<u8> },
    Del { key: Vec<u8> },
}

type UpdateResult = crate::Result<Update>;

/// Trait for a key value storage engine
pub trait KvsEngine: Clone + Send + Sync {
    /// Build a Kvstore from a database folder
    fn restore(folder: impl Into<PathBuf>) -> Result<Self>
    where
        Self: Sized;

    /// Open a database inside of the KvStore. The data inside of this tree will
    /// only be selectable if you include the value in key.
    fn open(self, name: String) -> Result<Tree<Self>> {
        Tree::new(self, name)
    }

    /// Sets the value of a string key to a string.
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is not written successfully
    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// Gets the string value of a given string key.
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// Return an error if the value is not read successfullly
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// Return an error if the key does not exist or value failed to be read
    fn remove(&self, key: Vec<u8>) -> Result<()>;

    /// Find a collection of key values for a given pattern.
    ///
    /// A pattern is a string value that includes literals ('a', '1', '/', ect),
    /// wildcards ('_'), and any ('*'). Using these elements together, you can
    /// search for a collection of strings.
    ///
    /// # Errors
    ///
    /// Return an error if we failed to complete the read of the keys
    fn find(&self, pattern: Vec<u8>) -> Result<Vec<Vec<u8>>>;

    /// Subscribe to a key updates given a key pattern.
    ///
    /// A pattern is a string value that includes literals ('a', '1', '/', ect),
    /// wildcards ('_'), and any ('*'). Using these elements together, you can
    /// search for a collection of strings.
    ///
    /// # Errors
    ///
    /// Return an error if we fail to setup a subscriber
    fn subscribe(&self, subscriber: Subscriber) -> Result<()>;
}

pub struct Subscriber {
    pattern: PreparedPattern,
    tx: mpsc::Sender<UpdateResult>,
}

impl Subscriber {
    pub fn new(pattern: Vec<u8>) -> (Self, mpsc::Receiver<UpdateResult>) {
        let pattern = prepare(pattern);
        let (tx, rx) = mpsc::channel();
        let this = Self { pattern, tx };
        (this, rx)
    }

    pub fn update(&self, update: Update) -> Result<()> {
        let is_valud_key = match &update {
            Update::Set { key, value } => self.pattern.test(&key),
            Update::Del { key } => self.pattern.test(&key),
        };
        if is_valud_key {
            self.tx.send(Ok(update))?;
        }
        Ok(())
    }
}

/// kvs is this libraries implementation of a key value store
pub mod kvs;

/// kvs store that keeps all data in a library
pub mod memory;

/// sled is a already implemented library in rust
pub mod sled;
mod tree;

pub use self::kvs::KvStore;
pub use self::memory::KvInMemoryStore;
pub use self::sled::SledKvsEngine;
use self::tree::Tree;
