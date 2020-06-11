#![deny(missing_docs)]
//! A simple key/value store.

pub use kv::KvError;
pub use kv::KvStore;
pub use kv::Result;

mod kv;
