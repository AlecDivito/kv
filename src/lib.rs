#![deny(missing_docs)]
//! A simple key/value store.
//!

#[macro_use]
extern crate tracing;

pub use client::KvClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{GenericError, KvError, Result};
pub use server::run as listen_with;

mod client;
mod command;
mod common;
mod connection;
mod datastructures;
mod engines;
mod error;
mod server;
mod shutdown;

/// a simple thread pool
pub mod thread_pool;
