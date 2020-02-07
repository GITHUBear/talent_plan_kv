#![deny(missing_docs)]
//! A simple key/value store.

pub use kv::{KvStore};
pub use kvs_error::{KvsError, Result};

mod kvs_engine;
mod kvs_error;
mod kv;
