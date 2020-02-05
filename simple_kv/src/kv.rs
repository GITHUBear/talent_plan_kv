use std::collections::HashMap;

pub struct KvStore {
    kv_map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            kv_map: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.kv_map.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.kv_map.get(&*key).cloned()
    }

    pub fn remove(&mut self, key: String) {
        self.kv_map.remove(&*key);
    }
}
