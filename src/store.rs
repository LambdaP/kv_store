// use super::inner_map;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tower_http::metrics::in_flight_requests::InFlightRequestsCounter;
use tracing::{debug, info};

use self::inner_map::CasError;
use crate::utf8_bytes::Utf8Bytes;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "method")]
#[serde(rename_all = "UPPERCASE")]
pub enum SimpleRequest {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: Utf8Bytes,
        #[serde(skip_serializing_if = "Option::is_none")]
        ttl: Option<u64>,
    },
    Delete {
        key: String,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SimpleResponse {
    pub status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Utf8Bytes>,
}

impl SimpleResponse {
    pub fn new(status: StatusCode, value: Option<Utf8Bytes>) -> Self {
        Self {
            status: status.as_u16(),
            value,
        }
    }
}

// Commands that can mutate the store
#[non_exhaustive]
#[derive(Debug, Copy, Clone)]
pub(crate) enum Cmd {
    Put(u64),
    Delete(u64),
    CompareAndSwap(u64),
    Expired(u64),
    Watch,
}

impl Cmd {
    pub(crate) fn get_counter(self) -> Option<u64> {
        match self {
            Cmd::Put(c) | Cmd::Delete(c) | Cmd::CompareAndSwap(c) | Cmd::Expired(c) => Some(c),
            _ => None,
        }
    }
}

impl std::fmt::Display for Cmd {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Cmd::Put(_) => write!(f, "PUT"),
            Cmd::Delete(_) => write!(f, "DELETE"),
            Cmd::CompareAndSwap(_) => write!(f, "COMPARE_AND_SWAP"),
            Cmd::Expired(_) => write!(f, "EXPIRED"),
            Cmd::Watch => write!(f, "WATCH"),
        }
    }
}

type PubHandle = broadcast::Sender<(Cmd, Utf8Bytes)>;

#[derive(Debug)]
pub(crate) struct AppRequestsCounters {
    pub watch: InFlightRequestsCounter,
    pub batch: InFlightRequestsCounter,
}

impl AppRequestsCounters {
    fn new() -> Self {
        Self {
            watch: InFlightRequestsCounter::new(),
            batch: InFlightRequestsCounter::new(),
        }
    }
}

#[derive(Debug)]
pub struct Metered {
    pub store: Arc<KvStore>,
    pub start_time: Instant,
    pub(crate) requests_counters: AppRequestsCounters,
    ops_counters: OpsCounters,
}

impl Default for Metered {
    fn default() -> Self {
        Self::new()
    }
}

impl Metered {
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: Arc::new(KvStore::new()),
            start_time: Instant::now(),
            ops_counters: OpsCounters::new(),
            requests_counters: AppRequestsCounters::new(),
        }
    }

    pub async fn keys_len(&self) -> usize {
        self.store.data.read().await.len()
    }

    pub fn uptime(&self) -> time::Duration {
        use time::ext::InstantExt;
        Instant::now().signed_duration_since(self.start_time)
    }

    pub fn get_ops_counters(&self) -> OpsMap<u64> {
        self.ops_counters.load_all()
    }

    pub fn get_watch_requests_count(&self) -> usize {
        self.requests_counters.watch.get()
    }

    pub fn get_batch_requests_count(&self) -> usize {
        self.requests_counters.batch.get()
    }

    pub async fn insert(
        &self,
        key: String,
        value: Utf8Bytes,
        ttl: Option<Duration>,
    ) -> KvStoreResponse {
        let response = self.store.insert(key, value, ttl).await;
        self.ops_counters.increment(CountOps::Put);
        response
    }

    pub async fn get(&self, key: &str) -> KvStoreResponse {
        let response = self.store.get(key).await;
        self.ops_counters.increment(CountOps::Get);
        response
    }

    pub async fn remove(&self, key: &str) -> KvStoreResponse {
        let response = self.store.remove(key).await;

        if let KvStoreResponse::Success = response {
            self.ops_counters.increment(CountOps::Delete);
        }

        response
    }

    pub(crate) async fn compare_and_swap<E>(
        &self,
        key: &str,
        expected: &E,
        new: Utf8Bytes,
    ) -> KvStoreResponse
    where
        E: ?Sized,
        Utf8Bytes: PartialEq<E>,
    {
        let response = self.store.compare_and_swap(key, expected, new).await;

        if let KvStoreResponse::Success = response {
            self.ops_counters.increment(CountOps::CasSuccess);
        } else {
            self.ops_counters.increment(CountOps::CasFailure);
        }

        response
    }

    pub(crate) async fn watch_key(
        &self,
        key: String,
    ) -> impl tokio_stream::Stream<Item = (String, Result<(Cmd, Utf8Bytes), BroadcastStreamRecvError>)>
    {
        self.ops_counters.increment(CountOps::WatchKey);
        self.store.watch_keys(vec![key]).await
    }

    pub(crate) async fn watch_multiple_keys(
        &self,
        keys: Vec<String>,
    ) -> impl tokio_stream::Stream<Item = (String, Result<(Cmd, Utf8Bytes), BroadcastStreamRecvError>)>
    {
        self.ops_counters.increment(CountOps::WatchMultipleKeys);
        self.store.watch_keys(keys).await
    }

    pub async fn batch_process(
        self: Arc<Self>,
        requests: Vec<SimpleRequest>,
    ) -> Vec<SimpleResponse> {
        self.ops_counters.increment(CountOps::Batch);
        self.store.clone().batch_process(requests).await
    }

    pub async fn cleanup_loop(self: Arc<Self>) {
        use tokio::time::{interval, MissedTickBehavior};

        let mut interval = interval(Duration::from_millis(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            self.store.cleanup_marked_keys().await;
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum KvStoreResponse {
    Success,
    SuccessBody(Utf8Bytes),
    Created,
    NotFound,
    CasTestFailed,
}

impl KvStoreResponse {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn into_status_body(self) -> (StatusCode, Option<Utf8Bytes>) {
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

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum CountOps {
    Get = 0,
    Put,
    Delete,
    CasSuccess,
    CasFailure,
    Batch,
    WatchKey,
    WatchMultipleKeys,
}

const N_COUNT_OPS: usize = CountOps::WatchMultipleKeys as usize + 1;
#[derive(Debug, Default, PartialEq, Eq)]
pub struct OpsMap<T>([T; N_COUNT_OPS]);

impl<T> std::ops::Index<CountOps> for OpsMap<T> {
    type Output = T;
    fn index(&self, ops: CountOps) -> &T {
        &self.0[ops as usize]
    }
}

impl<T> OpsMap<T>
where
    T: Default,
{
    fn new() -> Self {
        Self::default()
    }
}

impl<T> From<[T; N_COUNT_OPS]> for OpsMap<T> {
    fn from(arr: [T; N_COUNT_OPS]) -> OpsMap<T> {
        OpsMap(arr)
    }
}

type OpsCounters = OpsMap<AtomicU64>;

impl OpsMap<AtomicU64> {
    pub fn increment(&self, ops: CountOps) -> u64 {
        self[ops].fetch_add(1, Ordering::Relaxed)
    }

    pub fn load_all(&self) -> OpsMap<u64> {
        self.0.each_ref().map(|c| c.load(Ordering::Relaxed)).into()
    }
}

impl IntoResponse for KvStoreResponse {
    #[tracing::instrument(level = "trace", skip(self))]
    fn into_response(self) -> Response {
        let (status, body) = self.into_status_body();
        (status, body.unwrap_or_default()).into_response()
    }
}

// TODO use a BTreeMap to store keys ordered by expiry date
#[derive(Debug)]
pub struct KvStore {
    data: RwLock<inner_map::LockMap<String, Utf8Bytes>>,
    publish_handles: RwLock<HashMap<String, PubHandle>>,
    keys_to_remove: std::sync::Mutex<Vec<String>>,
}

impl KvStore {
    #[tracing::instrument(level = "trace", skip())]
    pub fn new() -> KvStore {
        KvStore {
            data: RwLock::default(),
            publish_handles: RwLock::default(),
            keys_to_remove: std::sync::Mutex::default(),
        }
    }

    pub(crate) async fn get_pub_tx(&self, key: &str) -> Option<PubHandle> {
        self.publish_handles.read().await.get(key).cloned()
    }

    pub(crate) async fn create_pub_tx(&self, key: String) -> PubHandle {
        self.publish_handles
            .write()
            .await
            .entry(key)
            .or_insert_with(|| broadcast::channel(1024).0)
            .clone()
    }

    // TODO Tokio prioritises writer access on RwStores
    //   to avoid the case of readers starving writers.
    // Here and in other places,
    //   the strategy of obtaining read access
    //   and relying on inner mutability
    //   to modify the store
    //   could similarly lead to readers starving writers
    //   on individual keys.
    #[tracing::instrument(level = "trace", skip(self, value))]
    pub async fn insert(
        &self,
        key: String,
        value: Utf8Bytes,
        ttl: Option<Duration>,
    ) -> KvStoreResponse {
        debug!("Inserting key: {}", key);
        let pub_tx = self.get_pub_tx(&key).await;

        // If the key is already present,
        //   acquire the lock with `read()`
        //   and update the entry using inner mutability.
        if let Some(cnt) = self.data.read().await.try_swap(&key, value.clone(), ttl) {
            if let Some(tx) = pub_tx {
                _ = tx.send((Cmd::Put(cnt), value));
            }
            return KvStoreResponse::Success;
        }

        let (cnt, key_exists) = self.data.write().await.insert(key, value.clone(), ttl);

        if let Some(tx) = pub_tx {
            // TODO consider using a different Cmd here to signal Update
            _ = tx.send((Cmd::Put(cnt), value));
        }

        if key_exists {
            KvStoreResponse::Success
        } else {
            KvStoreResponse::Created
        }
    }

    // TODO communicate TTL so that it can be part of the response
    #[tracing::instrument(level = "trace", skip(self, key))]
    pub async fn get(&self, key: &str) -> KvStoreResponse {
        debug!("Getting key: {}", key);

        let Some(inner_map::StoreEntry { value, expires }) = self.data.read().await.get(key) else {
            return KvStoreResponse::NotFound;
        };

        if let Some(expires) = expires {
            if expires < Instant::now() {
                self.mark_key_for_removal(key);
                return KvStoreResponse::NotFound;
            }
        }

        KvStoreResponse::SuccessBody(value)
    }

    #[tracing::instrument(level = "trace", skip(self, key))]
    pub async fn remove(&self, key: &str) -> KvStoreResponse {
        debug!("Removing key: {}", key);

        let Some(cnt) = self.data.write().await.remove(key) else {
            return KvStoreResponse::NotFound;
        };

        if let Some(tx) = self.get_pub_tx(key).await {
            _ = tx.send((Cmd::Delete(cnt), "".into()));
        }

        KvStoreResponse::Success
    }

    #[tracing::instrument(level = "trace", skip(self, key, expected, new))]
    pub async fn compare_and_swap<E>(
        &self,
        key: &str,
        expected: &E,
        new: Utf8Bytes,
    ) -> KvStoreResponse
    where
        E: ?Sized,
        Utf8Bytes: PartialEq<E>,
    {
        debug!("Compare-and-swap on key: {}", key);
        match self
            .data
            .read()
            .await
            .compare_and_swap(key, expected, new.clone())
        {
            Ok(cnt) => {
                if let Some(tx) = self.get_pub_tx(key).await {
                    _ = tx.send((Cmd::CompareAndSwap(cnt), new));
                }
                KvStoreResponse::Success
            }
            Err(CasError::ValuesDiffered) => KvStoreResponse::CasTestFailed,
            Err(CasError::NotFound) => KvStoreResponse::NotFound,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, keys))]
    pub async fn watch_keys(
        &self,
        keys: Vec<String>,
    ) -> impl tokio_stream::Stream<Item = (String, Result<(Cmd, Utf8Bytes), BroadcastStreamRecvError>)>
    {
        use tokio_stream::{wrappers::BroadcastStream, StreamExt, StreamMap};
        let mut handles = vec![];

        for key in &keys {
            let key_pub_tx = if let Some(tx) = self.get_pub_tx(key).await {
                tx
            } else {
                self.create_pub_tx(key.clone()).await
            };

            handles.push(key_pub_tx.subscribe());
        }

        let initial_values = {
            let guard = self.data.read().await;

            keys.iter().map(|key| guard.get(key)).collect::<Vec<_>>()
        };

        let initial_commands = initial_values
            .into_iter()
            .map(|val| (Cmd::Watch, val.map(|entry| entry.value).unwrap_or_default()));

        let streams = handles
            .into_iter()
            .zip(initial_commands)
            .map(|(rx, init)| tokio_stream::once(Ok(init)).chain(BroadcastStream::new(rx)));

        keys.into_iter().zip(streams).collect::<StreamMap<_, _>>()
    }

    pub async fn batch_process(
        self: Arc<Self>,
        requests: Vec<SimpleRequest>,
    ) -> Vec<SimpleResponse> {
        let db = self.clone();
        let handles = requests.into_iter().map(|request| {
            let db = db.clone();
            match request {
                SimpleRequest::Get { key } => {
                    tokio::task::spawn(async move { db.get(&key).await.into_status_body() })
                }
                SimpleRequest::Put { key, value, ttl } => tokio::task::spawn(async move {
                    let ttl = ttl.map(Duration::from_secs);
                    db.insert(key, value, ttl).await.into_status_body()
                }),
                SimpleRequest::Delete { key } => {
                    tokio::task::spawn(async move { db.remove(&key).await.into_status_body() })
                }
            }
        });

        let mut responses = vec![];

        for handle in handles {
            let (status, value) = handle
                .await
                .unwrap_or((StatusCode::INTERNAL_SERVER_ERROR, None));
            responses.push(SimpleResponse::new(status, value));
        }

        responses
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn mark_key_for_removal(&self, key: &str) -> bool {
        info!("Marking key {} for removal", key);
        if let Ok(ref mut buf) = self.keys_to_remove.try_lock() {
            buf.push(String::from(key));
            return true;
        }
        false
    }

    fn take_keys_to_remove(&self) -> Vec<String> {
        let mut guard = self.keys_to_remove.lock().unwrap();
        std::mem::take(&mut *guard)
    }

    pub async fn cleanup_marked_keys(&self) {
        let keys = self.take_keys_to_remove();

        if keys.is_empty() {
            return;
        }

        let removed: Vec<_> = {
            let mut guard = self.data.write().await;

            keys.into_iter()
                .filter_map(|key| {
                    if let Ok(cnt) = guard.remove_if_outdated(&key) {
                        Some((key, cnt))
                    } else {
                        None
                    }
                })
                .collect()
        };

        if removed.is_empty() {
            return;
        }

        let guard = self.publish_handles.read().await;

        for (key, cnt) in removed {
            if let Some(tx) = guard.get(&key) {
                _ = tx.send((Cmd::Expired(cnt), "".into()));
            }
        }
    }

    pub async fn close_publish_handles(&self) {
        // TODO maybe send a termination message
        let mut guard = self.publish_handles.write().await;
        _ = std::mem::take(&mut *guard);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tracing::instrument(level = "trace", skip())]
    async fn setup_kv_store() -> KvStore {
        KvStore::new()
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let store = Metered::new();
        let key = "test_key";
        let value = "test_value";

        let insert_result = store.insert(key.into(), value.into(), None).await;
        assert!(matches!(insert_result, KvStoreResponse::Created));

        let get_result = store.get(&key).await;
        assert!(matches!(get_result, KvStoreResponse::SuccessBody(body) if body == value));
    }

    #[tokio::test]
    async fn test_insert_with_ttl() {
        let store = setup_kv_store().await;
        let key = "ttl_key";
        let value = "ttl_value";

        store
            .insert(key.into(), value.into(), Some(Duration::from_millis(10)))
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
        let store = setup_kv_store().await;
        let key = "remove_key";
        let value = "remove_value";

        store.insert(key.into(), value.into(), None).await;

        let remove_result = store.remove(&key).await;
        assert!(matches!(remove_result, KvStoreResponse::Success));

        let get_result = store.get(&key).await;
        assert!(matches!(get_result, KvStoreResponse::NotFound));
    }

    #[tokio::test]
    async fn test_compare_and_swap() {
        let store = setup_kv_store().await;
        let key = "cas_key";
        let initial_value = "initial_value";
        let new_value = "new_value";

        store.insert(key.into(), initial_value.into(), None).await;

        // Successful CAS
        let cas_result = store
            .compare_and_swap(&key, &initial_value, new_value.into())
            .await;
        assert!(matches!(cas_result, KvStoreResponse::Success));

        // Failed CAS (value has changed)
        let failed_cas_result = store
            .compare_and_swap(&key, &initial_value, "another_value".into())
            .await;
        assert!(matches!(failed_cas_result, KvStoreResponse::CasTestFailed));

        // Verify final value
        let get_result = store.get(&key).await;
        assert!(matches!(get_result, KvStoreResponse::SuccessBody(body) if body == new_value));
    }

    #[tokio::test]
    async fn test_metrics() {
        let db = Metered::new();
        let key = "metrics_key";
        let value = "metrics_value";

        db.insert(key.into(), value.into(), None).await;
        db.get(&key).await;
        db.remove(&key).await;
        db.compare_and_swap(&key, &value, "new_value".into()).await;

        let counters = db.get_ops_counters();

        assert_eq!(counters[CountOps::Put], 1);
        assert_eq!(counters[CountOps::Get], 1);
        assert_eq!(counters[CountOps::Delete], 1);
        assert_eq!(counters[CountOps::CasFailure], 1);
        assert_eq!(counters[CountOps::CasSuccess], 0);
    }

    #[tokio::test]
    async fn test_metrics_accuracy() {
        let db = Metered::new();
        let key = "key1";

        // Perform a series of operations
        db.insert(key.into(), "value1".into(), None).await;
        db.get(&key).await;
        db.get("non_existent").await;
        db.remove(&key).await;
        db.compare_and_swap("key2", "old", "new".into()).await;

        let counters = db.get_ops_counters();

        // Check metrics
        assert_eq!(counters[CountOps::Put], 1);
        assert_eq!(counters[CountOps::Get], 2);
        assert_eq!(counters[CountOps::Delete], 1);
        assert_eq!(counters[CountOps::CasFailure], 1);
        assert_eq!(counters[CountOps::CasSuccess], 0);
    }

    #[tokio::test]
    async fn test_cleanup_marked_keys() {
        let db = Metered::new();
        let key = "cleanup_key";
        let value = "cleanup_value";

        // Insert a key with a short TTL
        db.insert(key.into(), value.into(), Some(Duration::from_millis(10)))
            .await;

        // Wait for the TTL to expire
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Trigger a get operation to mark the key for removal
        let _ = db.get(&key).await;

        // Run the cleanup
        db.store.cleanup_marked_keys().await;

        // The key should now be removed
        let get_result = db.get(&key).await;
        assert!(matches!(get_result, KvStoreResponse::NotFound));
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let db = Arc::new(Metered::new());
        let key = Arc::new("concurrent_key".to_string());
        let mut handles = vec![];

        // Spawn 10 tasks to insert values concurrently
        for i in 0..10 {
            let store_clone = db.clone();
            let key_clone = key.clone();
            let handle = tokio::spawn(async move {
                let value = format!("value_{}", i);
                store_clone
                    .insert(key_clone.to_string(), value.into(), None)
                    .await
            });
            handles.push(handle);
        }

        // Wait for all insertions to complete
        for handle in handles {
            let _ = handle.await;
        }

        // Verify that only one value was inserted
        let get_result = db.get(&key).await;
        assert!(matches!(get_result, KvStoreResponse::SuccessBody(_)));

        // Spawn 10 tasks to get the value concurrently
        let mut get_handles = vec![];
        for _ in 0..10 {
            let store_clone = db.clone();
            let key_clone = key.clone();
            let handle = tokio::spawn(async move { store_clone.get(&key_clone).await });
            get_handles.push(handle);
        }

        // Verify that all get operations succeed
        for handle in get_handles {
            let result = handle.await.unwrap();
            assert!(matches!(result, KvStoreResponse::SuccessBody(_)));
        }

        _ = db.insert(key.to_string(), "value_0".into(), None).await;

        // Test concurrent CAS operations
        let mut cas_handles = vec![];
        for i in 0..10 {
            let store_clone = db.clone();
            let key_clone = key.clone();
            let handle = tokio::spawn(async move {
                let new_value = format!("new_value_{}", i);
                store_clone
                    .compare_and_swap(&key_clone, "value_0", new_value.into())
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

mod inner_map {
    use std::{
        borrow::Borrow,
        collections::HashMap,
        hash::Hash,
        sync::{
            atomic::{AtomicU64, Ordering},
            Mutex,
        },
        time::{Duration, Instant},
    };

    #[derive(Debug, Default, Clone)]
    pub struct StoreEntry<V> {
        pub value: V,
        pub expires: Option<Instant>,
    }

    impl<V> StoreEntry<V> {
        fn new(value: V, ttl: Option<Duration>) -> Self {
            let expires = ttl.and_then(|dur| Instant::now().checked_add(dur));
            StoreEntry { value, expires }
        }

        fn update_value(&mut self, new: V) {
            self.value = new;
        }
    }

    #[derive(Debug, Default)]
    pub(crate) struct LockMap<K, V> {
        data: HashMap<K, Mutex<StoreEntry<V>>>,
        mut_count: AtomicU64,
    }

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    pub(crate) enum CasError {
        NotFound,
        ValuesDiffered,
    }

    impl<K, V> LockMap<K, V>
    where
        K: Hash + Eq,
        V: Clone,
    {
        #[tracing::instrument(level = "trace", skip(self))]
        pub fn len(&self) -> usize {
            self.data.len()
        }

        // Returns whether the key was present
        #[tracing::instrument(level = "trace", skip(self, key, value))]
        pub fn insert(&mut self, key: K, value: V, ttl: Option<Duration>) -> (u64, bool) {
            use std::collections::hash_map::Entry;

            let new_entry = StoreEntry::new(value, ttl);
            match self.data.entry(key) {
                Entry::Occupied(o) => {
                    let mut guard = o.get().lock().unwrap();
                    let op_rank = self.mut_count.fetch_add(1, Ordering::Relaxed);
                    *guard = new_entry;
                    (op_rank, true)
                }
                Entry::Vacant(o) => {
                    let op_rank = self.mut_count.fetch_add(1, Ordering::Relaxed);
                    o.insert(Mutex::new(new_entry));
                    (op_rank, false)
                }
            }
        }

        #[tracing::instrument(level = "trace", skip(self, key, value, ttl))]
        pub fn try_swap<Q>(&self, key: &Q, value: V, ttl: Option<Duration>) -> Option<u64>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            let locked_entry = self.data.get(key)?;

            let new_entry = StoreEntry::new(value, ttl);

            let mut guard = locked_entry.lock().unwrap();
            let op_rank = self.mut_count.fetch_add(1, Ordering::Relaxed);
            *guard = new_entry;
            Some(op_rank)
        }

        #[tracing::instrument(level = "trace", skip(self, key))]
        pub fn get<Q>(&self, key: &Q) -> Option<StoreEntry<V>>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            let locked_entry = self.data.get(key)?;
            Some(locked_entry.lock().unwrap().clone())
        }

        #[tracing::instrument(level = "trace", skip(self, key))]
        pub fn remove<Q>(&mut self, key: &Q) -> Option<u64>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            self.data.remove(key)?;

            let op_rank = self.mut_count.fetch_add(1, Ordering::Relaxed);
            Some(op_rank)
        }

        // TODO ttl?
        #[tracing::instrument(level = "trace", skip(self, key, expected, new))]
        pub fn compare_and_swap<Q, E>(&self, key: &Q, expected: &E, new: V) -> Result<u64, CasError>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
            E: ?Sized,
            V: PartialEq<E>,
        {
            let locked_entry = self.data.get(key).ok_or(CasError::NotFound)?;

            let mut guard = locked_entry.lock().unwrap();

            if guard.value != *expected {
                return Err(CasError::ValuesDiffered);
            }

            guard.update_value(new);
            let op_rank = self.mut_count.fetch_add(1, Ordering::Relaxed);

            Ok(op_rank)
        }

        // TODO replace bool with enum to clarify API
        #[tracing::instrument(level = "trace", skip(self, key))]
        pub fn remove_if_outdated<Q>(&mut self, key: &Q) -> Result<u64, bool>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            let expires = {
                let locked_entry = self.data.get(key).ok_or(false)?;
                let guard = locked_entry.lock().unwrap();
                guard.expires.ok_or(true)?
            };

            if expires > Instant::now() {
                return Err(true);
            }

            self.data.remove(key);
            let op_rank = self.mut_count.fetch_add(1, Ordering::Relaxed);
            Ok(op_rank)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::utf8_bytes::Utf8Bytes;
        use std::time::Duration;

        #[test]
        fn test_insert_and_get() {
            let mut map = LockMap::<String, Utf8Bytes>::default();
            let key = "key1";
            let value = "value1";

            assert!(!map.insert(key.into(), value.into(), None).1);
            assert_eq!(map.len(), 1);

            let entry = map.get(key).unwrap();
            assert_eq!(entry.value, value);
            assert!(entry.expires.is_none());
        }

        #[test]
        fn test_insert_with_ttl() {
            let mut map = LockMap::<String, Utf8Bytes>::default();
            let key = "key2";
            let value = "value2";
            assert!(
                !map.insert(key.into(), value.into(), Some(Duration::from_secs(60)))
                    .1
            );

            let entry = map.get(key).unwrap();
            assert_eq!(entry.value, value);
            assert!(entry.expires.is_some());
        }

        #[test]
        fn test_remove() {
            let mut map = LockMap::<String, Utf8Bytes>::default();
            map.insert("key3".into(), "value3".into(), None);
            assert!(map.remove("key3").is_some());
            assert_eq!(map.len(), 0);
            assert!(map.get("key3").is_none());
        }

        #[test]
        fn test_compare_and_swap_success() {
            let mut map = LockMap::<String, Utf8Bytes>::default();
            let key = "key4";
            let old_value = "old_value";
            let new_value = "new_value";
            map.insert(key.into(), old_value.into(), None);

            assert!(map
                .compare_and_swap(key, old_value, new_value.into())
                .is_ok());

            let entry = map.get(key).unwrap();
            assert_eq!(entry.value, new_value);
        }

        #[test]
        fn test_compare_and_swap_failure() {
            let mut map = LockMap::<String, Utf8Bytes>::default();
            map.insert("key5".into(), "current_value".into(), None);

            assert_eq!(
                map.compare_and_swap("key5", "wrong_value", "new_value".into()),
                Err(CasError::ValuesDiffered)
            );

            let entry = map.get("key5").unwrap();
            assert_eq!(entry.value, "current_value");
        }

        #[test]
        fn test_remove_if_outdated() {
            let mut map = LockMap::<String, Utf8Bytes>::default();
            map.insert(
                "key6".into(),
                "value6".into(),
                Some(Duration::from_nanos(1)),
            );

            // Sleep to ensure the entry expires
            std::thread::sleep(Duration::from_millis(1));

            _ = map.remove_if_outdated("key6");
            assert!(map.get("key6").is_none());
        }

        #[test]
        fn test_non_existent_key() {
            let mut map = LockMap::<String, Utf8Bytes>::default();
            assert!(map.get("non_existent").is_none());
            assert!(map.remove("non_existent").is_none());
            assert_eq!(
                map.compare_and_swap("non_existent", "any", "new".into()),
                Err(CasError::NotFound)
            );
        }

        #[test]
        fn test_overwrite_existing_key() {
            let mut map = LockMap::<String, Utf8Bytes>::default();
            map.insert("key7".to_string(), "value7".into(), None);
            assert!(map.insert("key7".into(), "new_value7".into(), None).1);

            let entry = map.get("key7").unwrap();
            assert_eq!(entry.value, "new_value7");
        }

        #[test]
        fn test_len() {
            let mut map = LockMap::<String, Utf8Bytes>::default();
            assert_eq!(map.len(), 0);
            map.insert("key8".into(), "value8".into(), None);
            assert_eq!(map.len(), 1);
            map.insert("key9".into(), "value9".into(), None);
            assert_eq!(map.len(), 2);
            map.remove("key8");
            assert_eq!(map.len(), 1);
        }
    }
}
