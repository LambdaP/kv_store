use std::collections::HashMap;

struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    fn insert(&mut self, key: String, value: String) -> Option<String> {
        self.store.insert(key, value)
    }

    fn get(&self, key: &str) -> Option<String> {
        self.store.get(key).cloned()
    }

    fn remove(&mut self, key: &str) -> Option<String> {
        self.store.remove(key)
    }
}

fn main() {
    println!("Hello, world!");
}
