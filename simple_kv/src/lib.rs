#![deny(missing_docs)]
//! A simple key/value store.
#[macro_use]
extern crate log;

pub use engine::{KvStore, KvsEngine, SledStore};
pub use kv_client::KvClient;
pub use kv_server::KvServer;
pub use kvs_error::{KvsError, Result};

mod engine;
mod kv_client;
mod kv_server;
mod kvs_error;
mod protocol;

/// Support various kinds of thread pool implementation.
pub mod thread_pool;
