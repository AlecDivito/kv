//! This module provides various key value storage engines
//!

use std::path::PathBuf;

use crate::Result;

/// Trait for a key value storage engine
pub trait KvsEngine: Clone + Send + Sync {
    /// Build a Kvstore from a database folder
    fn restore(folder: impl Into<PathBuf>) -> Result<Self>
    where
        Self: Sized;

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

    /// Find a collection of key values.
    ///
    /// # Errors
    ///
    /// Return an error if we failed to complete the read of the keys
    fn find(&self, like: Vec<u8>) -> Result<Vec<Vec<u8>>>;
}

/// kvs is this libraries implementation of a key value store
pub mod kvs;

/// kvs store that keeps all data in a library
pub mod memory;

/// sled is a already implemented library in rust
pub mod sled;

pub use self::kvs::KvStore;
pub use self::memory::KvInMemoryStore;
pub use self::sled::SledKvsEngine;
