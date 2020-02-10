#![deny(missing_docs)]
//! A simple key/value store.
#[macro_use] extern crate log;

pub use kvs_error::{KvsError, Result};
pub use engine::{KvsEngine, KvStore, SledStore};
pub use kv_server::KvServer;
pub use kv_client::KvClient;

mod kvs_error;
mod engine;
mod protocol;
mod kv_server;
mod kv_client;

/// Support various kinds of thread pool implementation.
pub mod thread_pool;