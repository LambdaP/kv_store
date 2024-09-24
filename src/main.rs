use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use tokio::sync::mpsc;

use axum::{
    routing::{delete, get, post, put},
    Router,
};

use tower_governor::{governor::GovernorConfig, GovernorLayer};

use bytes::Bytes;

use tracing::{error, info};

use crate::store_interface::KvStore;

const DEFAULT_PORT: u16 = 3000;
const MAX_BATCH_SIZE: usize = 1024;

mod store_interface {
    use super::inner_map;
    use axum::{
        http::StatusCode,
        response::{IntoResponse, Response},
    };
    use bytes::Bytes;
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, Instant},
    };
    use tokio::sync::{mpsc, RwLock};
    use tracing::{debug, info};

    #[derive(Debug, Default)]
    struct AU64Counter(AtomicU64);

    impl AU64Counter {
        fn increment(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn load(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    // TODO use a BTreeMap to store keys ordered by expiry date
    #[derive(Debug)]
    pub struct KvStore {
        pub data: RwLock<inner_map::InnerMap<String, Bytes>>,
        pub metrics: Metrics,
        keys_removal_tx: mpsc::Sender<String>,
    }

    #[derive(Clone, Debug)]
    pub enum KvStoreResponse {
        Success,
        SuccessBody(Bytes),
        Created,
        NotFound,
        CasTestFailed,
    }

    impl KvStoreResponse {
        pub fn into_status_body(self) -> (StatusCode, Option<Bytes>) {
            let status = match self {
                KvStoreResponse::Success => StatusCode::NO_CONTENT,
                KvStoreResponse::SuccessBody(_) => StatusCode::OK,
                KvStoreResponse::Created => StatusCode::CREATED,
                KvStoreResponse::NotFound => StatusCode::NOT_FOUND,
                KvStoreResponse::CasTestFailed => StatusCode::PRECONDITION_FAILED,
            };

            let body = match self {
                KvStoreResponse::SuccessBody(body) => Some(body),
                _ => None,
            };

            (status, body)
        }
    }

    #[derive(Debug)]
    pub struct Metrics {
        pub start_time: Instant,
        get_count: AU64Counter,
        put_count: AU64Counter,
        delete_count: AU64Counter,
        cas_success_count: AU64Counter,
        cas_failure_count: AU64Counter,
    }

    #[tracing::instrument(level = "trace", skip(n))]
    fn increment_au64(n: &AtomicU64) {
        n.fetch_add(1, Ordering::Relaxed);
    }

    impl Metrics {
        #[tracing::instrument(level = "trace", skip())]
        fn new() -> Self {
            Metrics {
                start_time: Instant::now(),
                get_count: AU64Counter::default(),
                put_count: AU64Counter::default(),
                delete_count: AU64Counter::default(),
                cas_success_count: AU64Counter::default(),
                cas_failure_count: AU64Counter::default(),
            }
        }

        #[tracing::instrument(level = "trace", skip(self))]
        fn increment_get(&self) {
            self.get_count.increment();
        }

        #[tracing::instrument(level = "trace", skip(self))]
        fn increment_put(&self) {
            self.put_count.increment();
        }

        #[tracing::instrument(level = "trace", skip(self))]
        fn increment_delete(&self) {
            self.delete_count.increment();
        }

        #[tracing::instrument(level = "trace", skip(self))]
        fn increment_cas_success(&self) {
            self.cas_success_count.increment();
        }

        #[tracing::instrument(level = "trace", skip(self))]
        fn increment_cas_failure(&self) {
            self.cas_failure_count.increment();
        }

        #[tracing::instrument(level = "trace", skip(self))]
        pub fn total_get_ops(&self) -> u64 {
            self.get_count.load()
        }

        #[tracing::instrument(level = "trace", skip(self))]
        pub fn total_put_ops(&self) -> u64 {
            self.put_count.load()
        }

        #[tracing::instrument(level = "trace", skip(self))]
        pub fn total_delete_ops(&self) -> u64 {
            self.delete_count.load()
        }

        #[tracing::instrument(level = "trace", skip(self))]
        pub fn total_cas_success(&self) -> u64 {
            self.cas_success_count.load()
        }

        #[tracing::instrument(level = "trace", skip(self))]
        pub fn total_cas_failure(&self) -> u64 {
            self.cas_failure_count.load()
        }
    }

    impl IntoResponse for KvStoreResponse {
        fn into_response(self) -> Response {
            let (status, body) = self.into_status_body();
            (status, body.unwrap_or_default()).into_response()
        }
    }

    impl KvStore {
        #[tracing::instrument(level = "trace", skip(tx))]
        pub fn new(tx: mpsc::Sender<String>) -> KvStore {
            KvStore {
                data: RwLock::default(),
                metrics: Metrics::new(),
                keys_removal_tx: tx,
            }
        }

        #[tracing::instrument(level = "trace", skip(self, value))]
        pub async fn insert(
            &self,
            key: String,
            value: Bytes,
            ttl: Option<Duration>,
        ) -> KvStoreResponse {
            debug!("Inserting key: {}", key);
            self.metrics.increment_put();
            if self.data.write().await.insert(key, value, ttl) {
                KvStoreResponse::Success
            } else {
                KvStoreResponse::Created
            }
        }

        #[tracing::instrument(level = "trace", skip(self, key))]
        pub async fn get(&self, key: &str) -> KvStoreResponse {
            debug!("Getting key: {}", key);
            self.metrics.increment_get();

            let Some(inner_map::StoreEntry { value, expires }) = self.data.read().await.get(key)
            else {
                return KvStoreResponse::NotFound;
            };

            match expires {
                Some(expires_at) if expires_at < Instant::now() => {
                    self.mark_key_for_removal(key);
                    KvStoreResponse::NotFound
                }
                _ => KvStoreResponse::SuccessBody(value),
            }
        }

        #[tracing::instrument(level = "trace", skip(self, key))]
        pub async fn remove(&self, key: &str) -> KvStoreResponse {
            debug!("Removing key: {}", key);
            self.metrics.increment_delete();

            if self.data.write().await.remove(key) {
                KvStoreResponse::Success
            } else {
                KvStoreResponse::NotFound
            }
        }

        #[tracing::instrument(level = "trace", skip(self, key, expected, new))]
        pub async fn compare_and_swap(
            &self,
            key: &str,
            expected: &[u8],
            new: Bytes,
        ) -> KvStoreResponse {
            debug!("Compare-and-swap on key: {}", key);
            let Some(cas_result) = self.data.read().await.compare_and_swap(key, expected, new)
            else {
                self.metrics.increment_cas_failure();
                return KvStoreResponse::NotFound;
            };

            if cas_result {
                self.metrics.increment_cas_success();
                KvStoreResponse::Success
            } else {
                self.metrics.increment_cas_failure();
                KvStoreResponse::CasTestFailed
            }
        }

        #[tracing::instrument(level = "trace", skip(self))]
        fn mark_key_for_removal(&self, key: &str) -> bool {
            info!("Marking key {} for removal", key);
            self.keys_removal_tx.try_send(key.into()).is_ok()
        }

        #[tracing::instrument(level = "trace", skip(self, rx, buf))]
        pub async fn cleanup_received_keys(
            &self,
            rx: &mut mpsc::Receiver<String>,
            buf: &mut Vec<String>,
        ) {
            rx.recv_many(buf, rx.max_capacity()).await;
            let mut store = self.data.write().await;
            buf.drain(..).for_each(|key| {
                _ = store.remove_if_outdated(&key);
            });
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::sync::mpsc;

        async fn setup_kvstore() -> KvStore {
            let (tx, _rx) = mpsc::channel(64);
            KvStore::new(tx)
        }

        #[tokio::test]
        async fn test_insert_and_get() {
            let store = setup_kvstore().await;
            let key = "test_key".to_string();
            let value = Bytes::from("test_value");

            let insert_result = store.insert(key.clone(), value.clone(), None).await;
            assert!(matches!(insert_result, KvStoreResponse::Created));

            let get_result = store.get(&key).await;
            assert!(matches!(get_result, KvStoreResponse::SuccessBody(body) if body == value));
        }

        #[tokio::test]
        async fn test_insert_with_ttl() {
            let store = setup_kvstore().await;
            let key = "ttl_key".to_string();
            let value = Bytes::from("ttl_value");

            store
                .insert(key.clone(), value.clone(), Some(Duration::from_millis(10)))
                .await;

            // Value should exist immediately
            let get_result = store.get(&key).await;
            assert!(matches!(get_result, KvStoreResponse::SuccessBody(_)));

            // Wait for TTL to expire
            tokio::time::sleep(Duration::from_millis(20)).await;

            // Value should be gone now
            let get_result = store.get(&key).await;
            assert!(matches!(get_result, KvStoreResponse::NotFound));
        }

        #[tokio::test]
        async fn test_remove() {
            let store = setup_kvstore().await;
            let key = "remove_key".to_string();
            let value = Bytes::from("remove_value");

            store.insert(key.clone(), value, None).await;

            let remove_result = store.remove(&key).await;
            assert!(matches!(remove_result, KvStoreResponse::Success));

            let get_result = store.get(&key).await;
            assert!(matches!(get_result, KvStoreResponse::NotFound));
        }

        #[tokio::test]
        async fn test_compare_and_swap() {
            let store = setup_kvstore().await;
            let key = "cas_key".to_string();
            let initial_value = Bytes::from("initial_value");
            let new_value = Bytes::from("new_value");

            store.insert(key.clone(), initial_value.clone(), None).await;

            // Successful CAS
            let cas_result = store
                .compare_and_swap(&key, &initial_value, new_value.clone())
                .await;
            assert!(matches!(cas_result, KvStoreResponse::Success));

            // Failed CAS (value has changed)
            let failed_cas_result = store
                .compare_and_swap(&key, &initial_value, Bytes::from("another_value"))
                .await;
            assert!(matches!(failed_cas_result, KvStoreResponse::CasTestFailed));

            // Verify final value
            let get_result = store.get(&key).await;
            assert!(matches!(get_result, KvStoreResponse::SuccessBody(body) if body == new_value));
        }

        #[tokio::test]
        async fn test_metrics() {
            let store = setup_kvstore().await;
            let key = "metrics_key".to_string();
            let value = Bytes::from("metrics_value");

            store.insert(key.clone(), value.clone(), None).await;
            store.get(&key).await;
            store.remove(&key).await;
            store
                .compare_and_swap(&key, &value, Bytes::from("new_value"))
                .await;

            assert_eq!(store.metrics.total_put_ops(), 1);
            assert_eq!(store.metrics.total_get_ops(), 1);
            assert_eq!(store.metrics.total_delete_ops(), 1);
            assert_eq!(store.metrics.total_cas_failure(), 1);
            assert_eq!(store.metrics.total_cas_success(), 0);
        }

        #[tokio::test]
        async fn test_metrics_accuracy() {
            let (tx, _rx) = mpsc::channel(64);
            let store = Arc::new(KvStore::new(tx));

            // Perform a series of operations
            store
                .insert("key1".to_string(), Bytes::from("value1"), None)
                .await;
            store.get("key1").await;
            store.get("non_existent").await;
            store.remove("key1").await;
            store
                .compare_and_swap("key2", b"old", Bytes::from("new"))
                .await;

            // Check metrics
            assert_eq!(store.metrics.total_put_ops(), 1);
            assert_eq!(store.metrics.total_get_ops(), 2);
            assert_eq!(store.metrics.total_delete_ops(), 1);
            assert_eq!(store.metrics.total_cas_failure(), 1);
            assert_eq!(store.metrics.total_cas_success(), 0);
        }

        #[tokio::test]
        async fn test_cleanup_received_keys() {
            let (tx, mut rx) = mpsc::channel(64);
            let store = Arc::new(KvStore::new(tx));
            let key = "cleanup_key".to_string();
            let value = Bytes::from("cleanup_value");

            // Insert a key with a short TTL
            store
                .insert(key.clone(), value, Some(Duration::from_millis(10)))
                .await;

            // Wait for the TTL to expire
            tokio::time::sleep(Duration::from_millis(20)).await;

            // Trigger a get operation to mark the key for removal
            let _ = store.get(&key).await;

            // Run the cleanup
            let mut buf = Vec::new();
            store.cleanup_received_keys(&mut rx, &mut buf).await;

            // The key should now be removed
            let get_result = store.get(&key).await;
            assert!(matches!(get_result, KvStoreResponse::NotFound));
        }

        #[tokio::test]
        async fn test_concurrent_access() {
            let store = Arc::new(setup_kvstore().await);
            let key = Arc::new("concurrent_key".to_string());
            let mut handles = vec![];

            // Spawn 10 tasks to insert values concurrently
            for i in 0..10 {
                let store_clone = store.clone();
                let key_clone = key.clone();
                let handle = tokio::spawn(async move {
                    let value = Bytes::from(format!("value_{}", i));
                    store_clone.insert(key_clone.to_string(), value, None).await
                });
                handles.push(handle);
            }

            // Wait for all insertions to complete
            for handle in handles {
                let _ = handle.await;
            }

            // Verify that only one value was inserted
            let get_result = store.get(&key).await;
            assert!(matches!(get_result, KvStoreResponse::SuccessBody(_)));

            // Spawn 10 tasks to get the value concurrently
            let mut get_handles = vec![];
            for _ in 0..10 {
                let store_clone = store.clone();
                let key_clone = key.clone();
                let handle = tokio::spawn(async move { store_clone.get(&key_clone).await });
                get_handles.push(handle);
            }

            // Verify that all get operations succeed
            for handle in get_handles {
                let result = handle.await.unwrap();
                assert!(matches!(result, KvStoreResponse::SuccessBody(_)));
            }

            _ = store
                .insert(key.to_string(), Bytes::from(format!("value_0")), None)
                .await;

            // Test concurrent CAS operations
            let mut cas_handles = vec![];
            for i in 0..10 {
                let store_clone = store.clone();
                let key_clone = key.clone();
                let handle = tokio::spawn(async move {
                    let new_value = Bytes::from(format!("new_value_{}", i));
                    store_clone
                        .compare_and_swap(&key_clone, b"value_0", new_value)
                        .await
                });
                cas_handles.push(handle);
            }

            // Verify that only one CAS operation succeeds
            let mut success_count = 0;
            for handle in cas_handles {
                let result = handle.await.unwrap();
                if matches!(result, KvStoreResponse::Success) {
                    success_count += 1;
                }
            }
            assert_eq!(success_count, 1);
        }
    }
}

mod inner_map {
    use std::borrow::Borrow;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    #[derive(Debug, Default, Clone)]
    pub struct StoreEntry<V> {
        pub value: V,
        pub expires: Option<Instant>,
    }

    impl<V> StoreEntry<V> {
        #[tracing::instrument(level = "trace", skip(value, ttl))]
        fn new(value: V, ttl: Option<Duration>) -> Self {
            let expires = ttl.and_then(|dur| Instant::now().checked_add(dur));
            StoreEntry { value, expires }
        }

        #[tracing::instrument(level = "trace", skip(self, new))]
        fn update_value(&mut self, new: V) {
            self.value = new;
        }
    }

    // There is an inherent issue
    //   with using a Mutex<V>
    //   as the inner value
    //   (which is required for atomic compare-and-swap).
    // Reading the value
    //   in the get() method
    //   requires holding the lock
    //   while cloning the value,
    //   which possibly takes a long time to complete.
    // A std::sync::Mutex should only be locked
    //   for short periods of time,
    //   because the underlying thread is blocked,
    //   preventing progress on all tasks
    //   that are dispatched to that thread.
    // A possible solution
    //   would be to use a Mutex<Arc<V>>
    //   and return an Arc<V>,
    //   only locking the mutex
    //   while cloning the Arc<V>,
    //   which is fast.
    // As of right now
    //   this shouldn't be a problem
    //   since the inner value cannot be modified,
    //   only overwritten,
    //   thus at worse
    //   the returned pointer no longer points
    //   to the value held in the map.
    #[derive(Debug, Default)]
    pub struct InnerMap<K, V>(HashMap<K, Mutex<StoreEntry<V>>>);

    impl<K, V> InnerMap<K, V>
    where
        K: Hash + Eq,
        V: Clone,
    {
        #[tracing::instrument(level = "trace", skip(self))]
        pub fn len(&self) -> usize {
            self.0.len()
        }

        // Returns whether the key was present
        #[tracing::instrument(level = "trace", skip(self, key, value))]
        pub fn insert(&mut self, key: K, value: V, ttl: Option<Duration>) -> bool {
            let entry = StoreEntry::new(value, ttl);
            self.0.insert(key, entry.into()).is_some()
        }

        #[tracing::instrument(level = "trace", skip(self, key))]
        pub fn get<Q>(&self, key: &Q) -> Option<StoreEntry<V>>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            // TODO deal with a poisoned mutex
            self.0.get(key).map(|entry| (entry.lock().unwrap()).clone())
        }

        #[tracing::instrument(level = "trace", skip(self, key))]
        pub fn remove<Q>(&mut self, key: &Q) -> bool
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            self.0.remove(key).is_some()
        }

        #[tracing::instrument(level = "trace", skip(self, key, expected, new))]
        pub fn compare_and_swap<Q, E>(&self, key: &Q, expected: &E, new: V) -> Option<bool>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
            E: ?Sized,
            V: PartialEq<E>,
        {
            self.0.get(key).map(|locked_entry| {
                let mut entry = locked_entry.lock().unwrap();
                if entry.value == *expected {
                    entry.update_value(new);
                    true
                } else {
                    false
                }
            })
        }

        #[tracing::instrument(level = "trace", skip(self, key))]
        pub fn remove_if_outdated<Q>(&mut self, key: &Q) -> Option<()>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            let expires = { self.0.get(key)?.lock().unwrap().expires? };

            if expires < Instant::now() {
                self.0.remove(key);
            }

            None
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::time::Duration;

        #[test]
        fn test_insert_and_get() {
            let mut map = InnerMap::default();
            assert!(!map.insert("key1".to_string(), "value1".to_string(), None));
            assert_eq!(map.len(), 1);

            let entry = map.get("key1").unwrap();
            assert_eq!(entry.value, "value1");
            assert!(entry.expires.is_none());
        }

        #[test]
        fn test_insert_with_ttl() {
            let mut map = InnerMap::default();
            assert!(!map.insert(
                "key2".to_string(),
                "value2".to_string(),
                Some(Duration::from_secs(60))
            ));

            let entry = map.get("key2").unwrap();
            assert_eq!(entry.value, "value2");
            assert!(entry.expires.is_some());
        }

        #[test]
        fn test_remove() {
            let mut map = InnerMap::default();
            map.insert("key3".to_string(), "value3".to_string(), None);
            assert!(map.remove("key3"));
            assert_eq!(map.len(), 0);
            assert!(map.get("key3").is_none());
        }

        #[test]
        fn test_compare_and_swap_success() {
            let mut map = InnerMap::default();
            map.insert("key4".to_string(), "old_value".to_string(), None);

            assert_eq!(
                map.compare_and_swap("key4", "old_value", "new_value".to_string()),
                Some(true)
            );

            let entry = map.get("key4").unwrap();
            assert_eq!(entry.value, "new_value");
        }

        #[test]
        fn test_compare_and_swap_failure() {
            let mut map = InnerMap::default();
            map.insert("key5".to_string(), "current_value".to_string(), None);

            assert_eq!(
                map.compare_and_swap("key5", "wrong_value", "new_value".to_string()),
                Some(false)
            );

            let entry = map.get("key5").unwrap();
            assert_eq!(entry.value, "current_value");
        }

        #[test]
        fn test_remove_if_outdated() {
            let mut map = InnerMap::default();
            map.insert(
                "key6".to_string(),
                "value6".to_string(),
                Some(Duration::from_nanos(1)),
            );

            // Sleep to ensure the entry expires
            std::thread::sleep(Duration::from_millis(1));

            map.remove_if_outdated("key6");
            assert!(map.get("key6").is_none());
        }

        #[test]
        fn test_non_existent_key() {
            let mut map = InnerMap::<String, String>::default();
            assert!(map.get("non_existent").is_none());
            assert!(!map.remove("non_existent"));
            assert_eq!(
                map.compare_and_swap("non_existent", "any", "new".to_string()),
                None
            );
        }

        #[test]
        fn test_overwrite_existing_key() {
            let mut map = InnerMap::default();
            map.insert("key7".to_string(), "value7".to_string(), None);
            assert!(map.insert("key7".to_string(), "new_value7".to_string(), None));

            let entry = map.get("key7").unwrap();
            assert_eq!(entry.value, "new_value7");
        }

        #[test]
        fn test_len() {
            let mut map = InnerMap::default();
            assert_eq!(map.len(), 0);
            map.insert("key8".to_string(), "value8".to_string(), None);
            assert_eq!(map.len(), 1);
            map.insert("key9".to_string(), "value9".to_string(), None);
            assert_eq!(map.len(), 2);
            map.remove("key8");
            assert_eq!(map.len(), 1);
        }
    }
}

mod utf8_bytes {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer};
    use std::ops::Deref;
    use std::str::Utf8Error;

    #[derive(Clone, Debug, Default)]
    pub struct Utf8Bytes(Bytes);

    impl TryFrom<Bytes> for Utf8Bytes {
        type Error = Utf8Error;

        fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
            _ = std::str::from_utf8(&bytes)?;

            Ok(Utf8Bytes(bytes))
        }
    }

    impl From<String> for Utf8Bytes {
        fn from(s: String) -> Utf8Bytes {
            Utf8Bytes(s.into())
        }
    }

    impl Deref for Utf8Bytes {
        type Target = str;

        fn deref(&self) -> &str {
            // Safe because UTF-8 was validated at construction
            unsafe { std::str::from_utf8_unchecked(&self.0) }
        }
    }

    impl AsRef<Bytes> for Utf8Bytes {
        fn as_ref(&self) -> &Bytes {
            &self.0
        }
    }

    impl AsRef<[u8]> for Utf8Bytes {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    impl AsRef<str> for Utf8Bytes {
        fn as_ref(&self) -> &str {
            self
        }
    }

    impl<'de> Deserialize<'de> for Utf8Bytes {
        fn deserialize<D>(deserializer: D) -> Result<Utf8Bytes, D::Error>
        where
            D: Deserializer<'de>,
        {
            use serde::de::Error;
            Bytes::deserialize(deserializer)?
                .try_into()
                .map_err(Error::custom)
        }
    }
}

#[tracing::instrument(level = "trace", skip())]
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let port = env::var("KV_STORE_PORT")
        .map_err(|err| match err {
            env::VarError::NotPresent => {
                info!("KV_STORE_PORT environment variable not set. Using default port: {}", DEFAULT_PORT);
            }
            env::VarError::NotUnicode(_) => {
                error!("KV_STORE_PORT environment variable contains invalid UTF-8. Falling back to default port: {}", DEFAULT_PORT);
            }
        })
            .and_then(|port_str| {
            port_str.parse::<u16>().map_err(|_| {
                error!("Invalid KV_STORE_PORT value: '{}'. Falling back to default port: {}", port_str, DEFAULT_PORT);
            })
        })
        .inspect(|port| {
            info!("Successfully read KV_STORE_PORT from environment: {}", port);
        })
            .unwrap_or(DEFAULT_PORT);

    let socket_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);

    let (tx, mut rx) = mpsc::channel(64);

    let kv_store = Arc::new(KvStore::new(tx));
    let cleanup_store = kv_store.clone();

    tokio::spawn(async move {
        let mut buf = vec![];
        loop {
            cleanup_store.cleanup_received_keys(&mut rx, &mut buf).await;
        }
    });

    let governor_config = Arc::new(GovernorConfig::default());

    let app = Router::new()
        .route("/store/:key", get(routes::get_key))
        .route("/store/:key", put(routes::put_key_val))
        .route("/store/:key", delete(routes::delete_key))
        .route("/store/cas/:key", post(routes::compare_and_swap))
        .route("/batch", post(routes::batch_process))
        .route("/status", get(routes::status))
        .layer(GovernorLayer {
            config: governor_config,
        })
        .with_state(kv_store)
        // The following is required
        //   for the Governor layer to work properly.
        .into_make_service_with_connect_info::<std::net::SocketAddr>();

    let listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

mod routes {
    #[allow(clippy::wildcard_imports)]
    use super::*;
    use axum::{
        extract::{Json, Path, Query, State},
        http::StatusCode,
        response::IntoResponse,
    };
    use serde::{Deserialize, Serialize};
    use std::time::{Duration, Instant};
    use time::{ext::InstantExt, format_description::well_known::Iso8601, OffsetDateTime};

    #[derive(Debug, Default, Deserialize)]
    pub struct CasPayload {
        expected: Bytes,
        new: Bytes,
    }

    #[derive(Debug, Default, Deserialize)]
    pub struct PutRequestQueryParams {
        ttl: Option<u64>,
    }

    #[derive(Debug, Default, Deserialize, Serialize)]
    pub struct StatusResponse {
        status: String,
        uptime: String,
        total_keys: usize,
        total_operations: u64,
        get_operations: u64,
        put_operations: u64,
        delete_operations: u64,
        cas_operations: u64,
        successful_cas_operations: u64,
        failed_cas_operations: u64,
        server_time: String,
        version: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(tag = "method")]
    #[serde(rename_all = "UPPERCASE")]
    pub enum SimpleRequest {
        Get {
            key: String,
        },
        Put {
            key: String,
            value: Bytes,
            #[serde(skip_serializing_if = "Option::is_none")]
            ttl: Option<u64>,
        },
        Delete {
            key: String,
        },
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct SimpleResponse {
        status: u16,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Bytes>,
    }

    #[tracing::instrument(level = "trace", skip(kv_store))]
    pub async fn get_key(
        State(kv_store): State<Arc<KvStore>>,
        Path(key): Path<String>,
    ) -> impl IntoResponse {
        kv_store.get(&key).await
    }

    #[tracing::instrument(level = "trace", skip(kv_store, value))]
    pub async fn put_key_val(
        State(kv_store): State<Arc<KvStore>>,
        Path(key): Path<String>,
        Query(query): Query<PutRequestQueryParams>,
        value: Bytes,
    ) -> impl IntoResponse {
        let ttl = query.ttl.map(Duration::from_secs);
        kv_store.insert(key, value, ttl).await
    }

    #[tracing::instrument(level = "trace", skip(kv_store))]
    pub async fn delete_key(
        State(kv_store): State<Arc<KvStore>>,
        Path(key): Path<String>,
    ) -> impl IntoResponse {
        kv_store.remove(&key).await
    }

    #[tracing::instrument(level = "trace", skip(kv_store, cas_payload))]
    pub async fn compare_and_swap(
        State(kv_store): State<Arc<KvStore>>,
        Path(key): Path<String>,
        Json(cas_payload): Json<CasPayload>,
    ) -> impl IntoResponse {
        kv_store
            .compare_and_swap(&key, &cas_payload.expected, cas_payload.new)
            .await
    }

    #[tracing::instrument(level = "trace", skip(kv_store, requests))]
    pub async fn batch_process(
        State(kv_store): State<Arc<KvStore>>,
        Json(requests): Json<Vec<SimpleRequest>>,
    ) -> impl IntoResponse {
        if requests.len() > MAX_BATCH_SIZE {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }

        let handles = requests.into_iter().map(|request| {
            let kv_store = kv_store.clone();
            match request {
                SimpleRequest::Get { key } => {
                    tokio::task::spawn(async move { kv_store.get(&key).await.into_status_body() })
                }
                SimpleRequest::Put { key, value, ttl } => tokio::task::spawn(async move {
                    let ttl = ttl.map(Duration::from_secs);
                    kv_store.insert(key, value, ttl).await.into_status_body()
                }),
                SimpleRequest::Delete { key } => {
                    tokio::task::spawn(
                        async move { kv_store.remove(&key).await.into_status_body() },
                    )
                }
            }
        });

        let mut responses = vec![];

        for handle in handles {
            let (status, value) = handle
                .await
                .unwrap_or((StatusCode::INTERNAL_SERVER_ERROR, None));
            responses.push(SimpleResponse {
                status: status.as_u16(),
                value,
            });
        }

        Ok(Json(responses))
    }

    #[tracing::instrument(level = "trace", skip(kv_store))]
    pub async fn status(State(kv_store): State<Arc<KvStore>>) -> impl IntoResponse {
        (StatusCode::OK, Json(make_status(&kv_store).await))
    }

    #[tracing::instrument(level = "trace", skip(data, metrics))]
    async fn make_status(KvStore { data, metrics, .. }: &KvStore) -> StatusResponse {
        let total_keys = { data.read().await.len() };

        let get_operations = metrics.total_get_ops();
        let put_operations = metrics.total_put_ops();
        let delete_operations = metrics.total_delete_ops();
        let successful_cas_operations = metrics.total_cas_success();
        let failed_cas_operations = metrics.total_cas_failure();
        let cas_operations = successful_cas_operations + failed_cas_operations;
        let total_operations = get_operations + put_operations + delete_operations + cas_operations;

        let version = env!("CARGO_PKG_VERSION").into();

        let uptime = format!(
            "{:.3}",
            Instant::now().signed_duration_since(metrics.start_time)
        );

        let server_time = OffsetDateTime::now_local()
            .unwrap_or_else(|_| OffsetDateTime::now_utc())
            .format(&Iso8601::DATE_TIME_OFFSET)
            .unwrap_or_else(|_| "Error formatting server time".into());

        StatusResponse {
            status: "OK".into(),
            uptime,
            total_keys,
            get_operations,
            put_operations,
            delete_operations,
            successful_cas_operations,
            failed_cas_operations,
            cas_operations,
            total_operations,
            server_time,
            version,
        }
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        use axum::{
            body::Body,
            http::{Request, StatusCode},
            Router,
        };
        use std::sync::Arc;
        use tokio::sync::mpsc;
        use tower::ServiceExt; // for `oneshot`

        async fn setup_app() -> Router {
            let (tx, _rx) = mpsc::channel(64);
            let kv_store = Arc::new(KvStore::new(tx));
            Router::new()
                .route("/store/:key", get(get_key))
                .route("/store/:key", put(put_key_val))
                .route("/store/:key", delete(delete_key))
                .route("/store/cas/:key", post(compare_and_swap))
                .route("/batch", post(batch_process))
                .route("/status", get(status))
                .with_state(kv_store)
        }

        #[tokio::test]
        async fn test_get_key() {
            let app = setup_app().await;

            // Insert a key-value pair
            let put_request = Request::builder()
                .method("PUT")
                .uri("/store/test_key")
                .body(Body::from("test_value"))
                .unwrap();
            let response = app.clone().oneshot(put_request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);

            // Test GET request
            let get_request = Request::builder()
                .method("GET")
                .uri("/store/test_key")
                .body(Body::empty())
                .unwrap();
            let response = app.oneshot(get_request).await.unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(&body[..], b"test_value");
        }

        #[tokio::test]
        async fn test_put_key_val() {
            let app = setup_app().await;

            let request = Request::builder()
                .method("PUT")
                .uri("/store/new_key")
                .body(Body::from("new_value"))
                .unwrap();
            let response = app.oneshot(request).await.unwrap();

            assert_eq!(response.status(), StatusCode::CREATED);
        }

        #[tokio::test]
        async fn test_delete_key() {
            let app = setup_app().await;

            // Insert a key-value pair
            let put_request = Request::builder()
                .method("PUT")
                .uri("/store/delete_key")
                .body(Body::from("delete_value"))
                .unwrap();
            let response = app.clone().oneshot(put_request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);

            // Delete the key
            let delete_request = Request::builder()
                .method("DELETE")
                .uri("/store/delete_key")
                .body(Body::empty())
                .unwrap();
            let response = app.oneshot(delete_request).await.unwrap();

            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }

        #[tokio::test]
        async fn test_compare_and_swap() {
            let app = setup_app().await;

            // Insert a key-value pair
            let put_request = Request::builder()
                .method("PUT")
                .uri("/store/cas_key")
                .body(Body::from("initial_value"))
                .unwrap();
            let response = app.clone().oneshot(put_request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);

            // Perform CAS operation
            let cas_payload = serde_json::json!({
                "expected": "initial_value",
                "new": "new_value"
            });
            let cas_request = Request::builder()
                .method("POST")
                .uri("/store/cas/cas_key")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&cas_payload).unwrap()))
                .unwrap();
            let response = app.oneshot(cas_request).await.unwrap();

            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }

        #[tokio::test]
        async fn test_batch_process() {
            let app = setup_app().await;

            let batch_payload = serde_json::json!([
                {"method": "PUT", "key": "batch_key1", "value": "batch_value1"},
                {"method": "GET", "key": "batch_key1"},
                {"method": "DELETE", "key": "batch_key1"}
            ]);
            let batch_request = Request::builder()
                .method("POST")
                .uri("/batch")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&batch_payload).unwrap()))
                .unwrap();
            let response = app.oneshot(batch_request).await.unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let batch_response: Vec<SimpleResponse> = serde_json::from_slice(&body).unwrap();
            assert_eq!(batch_response.len(), 3);
            assert_eq!(batch_response[0].status, 201); // Created
            assert_eq!(batch_response[1].status, 200); // OK
            assert_eq!(batch_response[2].status, 204); // No Content
        }

        #[tokio::test]
        async fn test_status() {
            let app = setup_app().await;

            let request = Request::builder()
                .method("GET")
                .uri("/status")
                .body(Body::empty())
                .unwrap();
            let response = app.oneshot(request).await.unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let status_response: StatusResponse = serde_json::from_slice(&body).unwrap();
            assert_eq!(status_response.status, "OK");
        }

        #[tokio::test]
        async fn test_error_handling() {
            let app = setup_app().await;

            // Test invalid JSON for CAS operation
            let invalid_cas_request = Request::builder()
                .method("POST")
                .uri("/store/cas/some_key")
                .header("Content-Type", "application/json")
                .body(Body::from("{invalid_json}"))
                .unwrap();
            let response = app.oneshot(invalid_cas_request).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        #[tokio::test]
        async fn test_empty_key_value() {
            let app = setup_app().await;

            // Test putting an empty value
            let put_request = Request::builder()
                .method("PUT")
                .uri("/store/empty_key")
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(put_request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);

            // Test getting the empty value
            let get_request = Request::builder()
                .method("GET")
                .uri("/store/empty_key")
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(get_request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(body.is_empty());
        }
    }
}
