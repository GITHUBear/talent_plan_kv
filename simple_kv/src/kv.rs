use std::collections::HashMap;

/// `KvStore` is an in-memory key-value pairs storage.
///
/// Simply use `HashMap` to store key-value in memory without persistence.
///
/// Example:
/// ```rust
/// use simple_kv::KvStore;
/// let mut kvs = KvStore::new();
/// kvs.set("Hello".to_string(), "world".to_string());
/// let val = kvs.get("Hello".to_string());
/// assert_eq!(val, Some("world".to_string()));
/// kvs.remove("Hello".to_string());
/// let val = kvs.get("Hello".to_string());
/// assert_eq!(val, None);
/// ```

pub struct KvStore {
    kv_map: HashMap<String, String>,
}

impl KvStore {
    /// Create a `KvStore`.
    pub fn new() -> Self {
        KvStore {
            kv_map: HashMap::new(),
        }
    }

    /// Set the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) {
        self.kv_map.insert(key, value);
    }

    /// Get the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Option<String> {
        self.kv_map.get(&*key).cloned()
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) {
        self.kv_map.remove(&*key);
    }
}
