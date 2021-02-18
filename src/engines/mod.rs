//! This module provides various key value storage engines
//!

use crate::Result;

/// Trait for a key value storage engine
pub trait KvsEngine {
    /// Sets the value of a string key to a string.
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is not written successfully
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Gets the string value of a given string key.
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// Return an error if the value is not read successfullly
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// Return an error if the key does not exist or value failed to be read
    fn remove(&mut self, key: String) -> Result<()>;
}

/// kvs is this libraries implementation of a key value store
pub mod kvs;

/// sled is a already implemented library in rust
pub mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
