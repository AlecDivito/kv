#![deny(missing_docs)]
//! A simple key/value store.
//!

#[macro_use]
extern crate log;

pub use client::KvClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{GenericError, KvError, Result};
pub use server::KvServer;

mod client;
mod common;
mod engines;
mod error;
mod server;

/// a simple thread pool
pub mod thread_pool;
