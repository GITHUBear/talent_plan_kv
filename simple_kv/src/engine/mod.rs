use crate::{ Result };

mod kvs;
mod sleds;

pub use kvs::KvStore;
pub use sleds::SledStore;

/// trait `KvsEngine` defines the abstract of `KvStore`
pub trait KvsEngine: Clone + Send + 'static {
    /// Set the value of a string key to a string.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Get the string value of a given string key.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Remove a given string key.
    fn remove(&self, key: String) -> Result<()>;
}