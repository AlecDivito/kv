#![deny(missing_docs)]
//! A simple key/value store.
//!

#[macro_use]
extern crate log;

pub use client::KvClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{ KvError, GenericError, Result};
pub use server::KvServer;

mod client;
mod common;
mod engines;
mod error;
mod server;
