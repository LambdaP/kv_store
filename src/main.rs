use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, RwLock};

use axum::{
    routing::{delete, get, post, put},
    Router,
};

use tower_governor::{governor::GovernorConfig, GovernorLayer};

use bytes::Bytes;

use tracing::{debug, error, info};

const DEFAULT_PORT: u16 = 3000;
const MAX_BATCH_SIZE: usize = 1024;

#[derive(Debug)]
struct Metrics {
    start_time: Instant,
    get_count: AtomicU64,
    put_count: AtomicU64,
    delete_count: AtomicU64,
    cas_success_count: AtomicU64,
    cas_failure_count: AtomicU64,
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
            get_count: AtomicU64::default(),
            put_count: AtomicU64::default(),
            delete_count: AtomicU64::default(),
            cas_success_count: AtomicU64::default(),
            cas_failure_count: AtomicU64::default(),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn increment_get(&self) {
        increment_au64(&self.get_count);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn increment_put(&self) {
        increment_au64(&self.put_count);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn increment_delete(&self) {
        increment_au64(&self.delete_count);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn increment_cas_success(&self) {
        increment_au64(&self.cas_success_count);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn increment_cas_failure(&self) {
        increment_au64(&self.cas_failure_count);
    }
}

// TODO use a BTreeMap to store keys ordered by expiry date
#[derive(Debug)]
struct KvStore {
    data: RwLock<inner_map::InnerMap<String, Bytes>>,
    metrics: Metrics,
    keys_removal_tx: mpsc::Sender<String>,
}

impl KvStore {
    #[tracing::instrument(level = "trace", skip(tx))]
    fn new(tx: mpsc::Sender<String>) -> KvStore {
        KvStore {
            data: RwLock::default(),
            metrics: Metrics::new(),
            keys_removal_tx: tx,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, value))]
    async fn insert(&self, key: String, value: Bytes, ttl: Option<Duration>) -> bool {
        debug!("Inserting key: {}", key);
        self.metrics.increment_put();
        self.data.write().await.insert(key, value, ttl)
    }

    #[tracing::instrument(level = "trace", skip(self, key))]
    async fn get(&self, key: &str) -> Option<Bytes> {
        debug!("Getting key: {}", key);
        self.metrics.increment_get();

        let inner_map::StoreEntry { value, expires } = self.data.read().await.get(key)?;

        match expires {
            Some(expires_at) if expires_at < Instant::now() => {
                self.mark_key_for_removal(key);
                None
            }
            _ => Some(value),
        }
    }

    #[tracing::instrument(level = "trace", skip(self, key))]
    async fn remove(&self, key: &str) -> bool {
        debug!("Removing key: {}", key);
        self.metrics.increment_delete();
        self.data.write().await.remove(key)
    }

    #[tracing::instrument(level = "trace", skip(self, key, expected, new))]
    async fn compare_and_swap(&self, key: &str, expected: &[u8], new: Bytes) -> Option<bool> {
        debug!("Compare-and-swap on key: {}", key);
        let res = self.data.read().await.compare_and_swap(key, expected, new);

        if res == Some(true) {
            self.metrics.increment_cas_success();
        } else {
            self.metrics.increment_cas_failure();
        }

        res
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn mark_key_for_removal(&self, key: &str) -> bool {
        info!("Marking key {} for removal", key);
        self.keys_removal_tx.try_send(key.into()).is_ok()
    }

    #[tracing::instrument(level = "trace", skip(self, rx, buf))]
    async fn cleanup_received_keys(&self, rx: &mut mpsc::Receiver<String>, buf: &mut Vec<String>) {
        rx.recv_many(buf, rx.max_capacity()).await;
        let mut store = self.data.write().await;
        buf.drain(..).for_each(|key| {
            _ = store.remove_if_outdated(&key);
        });
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
    axum::serve(
        listener,
        app,
    )
    .await
    .unwrap();
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

    #[derive(Debug, Default, Serialize)]
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
        if let Some(value) = kv_store.get(&key).await {
            info!("Key found: {}", key);
            debug!("Value: {:?}", value);
            Ok(value)
        } else {
            info!("Key not found: {}", key);
            Err(StatusCode::NOT_FOUND)
        }
    }

    #[tracing::instrument(level = "trace", skip(kv_store, value))]
    pub async fn put_key_val(
        State(kv_store): State<Arc<KvStore>>,
        Path(key): Path<String>,
        Query(query): Query<PutRequestQueryParams>,
        value: Bytes,
    ) -> impl IntoResponse {
        let ttl = query.ttl.map(Duration::from_secs);
        if kv_store.insert(key, value, ttl).await {
            info!("Key updated");
            StatusCode::NO_CONTENT
        } else {
            info!("Key inserted");
            StatusCode::CREATED
        }
    }

    #[tracing::instrument(level = "trace", skip(kv_store))]
    pub async fn delete_key(
        State(kv_store): State<Arc<KvStore>>,
        Path(key): Path<String>,
    ) -> impl IntoResponse {
        if kv_store.remove(&key).await {
            info!("Key deleted: {}", key);
            Ok(StatusCode::NO_CONTENT)
        } else {
            info!("Key not found for deletion: {}", key);
            Err(StatusCode::NOT_FOUND)
        }
    }

    #[tracing::instrument(level = "trace", skip(kv_store, cas_payload))]
    pub async fn compare_and_swap(
        State(kv_store): State<Arc<KvStore>>,
        Path(key): Path<String>,
        Json(cas_payload): Json<CasPayload>,
    ) -> impl IntoResponse {
        if let Some(cas_success) = kv_store
            .compare_and_swap(&key, &cas_payload.expected, cas_payload.new)
            .await
        {
            if cas_success {
                info!("Compare-and-swap on key{}: values match", key);
                Ok(StatusCode::NO_CONTENT)
            } else {
                info!("Compare-and-swap on key{}: values do not match", key);
                Err(StatusCode::PRECONDITION_FAILED)
            }
        } else {
            info!("Key not found for compare-and-swap: {}", key);
            Err(StatusCode::NOT_FOUND)
        }
    }

    // TODO this duplicates API logic with other route handlers (get_key etc.)
    //   obviously this is outrageous
    //   and I should do something better
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
                SimpleRequest::Get { key } => tokio::task::spawn(async move {
                    if let Some(value) = kv_store.get(&key).await {
                        (StatusCode::OK, Some(value))
                    } else {
                        (StatusCode::NOT_FOUND, None)
                    }
                }),
                SimpleRequest::Put { key, value, ttl } => tokio::task::spawn(async move {
                    let ttl = ttl.map(Duration::from_secs);
                    let status = if kv_store.insert(key, value, ttl).await {
                        StatusCode::NO_CONTENT
                    } else {
                        StatusCode::CREATED
                    };
                    (status, None)
                }),
                SimpleRequest::Delete { key } => tokio::task::spawn(async move {
                    let status = if kv_store.remove(&key).await {
                        StatusCode::NO_CONTENT
                    } else {
                        StatusCode::NOT_FOUND
                    };
                    (status, None)
                }),
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

        let get_operations = metrics.get_count.load(Ordering::Relaxed);
        let put_operations = metrics.put_count.load(Ordering::Relaxed);
        let delete_operations = metrics.delete_count.load(Ordering::Relaxed);
        let successful_cas_operations = metrics.cas_success_count.load(Ordering::Relaxed);
        let failed_cas_operations = metrics.cas_failure_count.load(Ordering::Relaxed);
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
}
