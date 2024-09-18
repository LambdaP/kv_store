use std::borrow::Borrow;
use std::collections::HashMap;
use std::env;
use std::hash::Hash;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, RwLock};

use axum::{
    extract::{Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Router,
};

use tracing::{debug, error, info};

use serde::Deserialize;

const DEFAULT_PORT: u16 = 3000;

// TODO use a BTreeMap to store keys ordered by expiry date
#[derive(Debug)]
struct KvStore {
    data: RwLock<InnerMap<String, String>>,
    keys_removal_tx: mpsc::Sender<String>,
}

impl KvStore {
    #[tracing::instrument(level = "trace", skip(tx))]
    fn new(tx: mpsc::Sender<String>) -> KvStore {
        KvStore {
            data: RwLock::default(),
            keys_removal_tx: tx,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, value))]
    async fn insert(&self, key: String, value: String, ttl: Option<Duration>) -> bool {
        debug!("Inserting key: {}", key);
        self.data.write().await.insert(key, value, ttl)
    }

    #[tracing::instrument(level = "trace", skip(self, key))]
    async fn get(&self, key: &str) -> Option<String> {
        debug!("Getting key: {}", key);

        let StoreEntry { value, expires } = self.data.read().await.get(key)?;

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
        self.data.write().await.remove(key)
    }

    #[tracing::instrument(level = "trace", skip(self, key, expected, new))]
    async fn compare_and_swap(&self, key: &str, expected: &str, new: String) -> Option<bool> {
        debug!("Compare-and-swap on key: {}", key);
        self.data.read().await.compare_and_swap(key, expected, new)
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
struct InnerMap<K, V>(HashMap<K, Mutex<StoreEntry<V>>>);

impl<K, V> InnerMap<K, V>
where
    K: Hash + Eq,
    V: Clone,
{
    // Returns whether the key was present
    #[tracing::instrument(level = "trace", skip(self, key, value))]
    fn insert(&mut self, key: K, value: V, ttl: Option<Duration>) -> bool {
        let entry = StoreEntry::new(value, ttl);
        self.0.insert(key, entry.into()).is_some()
    }

    #[tracing::instrument(level = "trace", skip(self, key))]
    fn get<Q>(&self, key: &Q) -> Option<StoreEntry<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // TODO deal with a poisoned mutex
        self.0.get(key).map(|entry| (entry.lock().unwrap()).clone())
    }

    #[tracing::instrument(level = "trace", skip(self, key))]
    fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.remove(key).is_some()
    }

    #[tracing::instrument(level = "trace", skip(self, key, expected, new))]
    fn compare_and_swap<Q, E>(&self, key: &Q, expected: &E, new: V) -> Option<bool>
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
    fn remove_if_outdated<Q>(&mut self, key: &Q) -> Option<()>
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

#[derive(Debug, Default, Clone)]
struct StoreEntry<V> {
    value: V,
    expires: Option<Instant>,
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

#[derive(Debug, Default, Deserialize)]
struct CasPayload {
    expected: String,
    new: String,
}

#[derive(Debug, Default, Deserialize)]
struct PutRequestQueryParams {
    ttl: Option<u64>,
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

    let app = Router::new()
        .route("/store/:key", get(get_key))
        .route("/store/:key", put(put_key_val))
        .route("/store/:key", delete(delete_key))
        .route("/store/cas/:key", post(compare_and_swap))
        .with_state(kv_store);

    let listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[tracing::instrument(level = "trace", skip(kv_store))]
async fn get_key(
    State(kv_store): State<Arc<KvStore>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    if let Some(value) = kv_store.get(&key).await {
        info!("Key found: {}", key);
        debug!("Value: {}", value);
        Ok(value)
    } else {
        info!("Key not found: {}", key);
        Err(StatusCode::NOT_FOUND)
    }
}

#[tracing::instrument(level = "trace", skip(kv_store, value))]
async fn put_key_val(
    State(kv_store): State<Arc<KvStore>>,
    Path(key): Path<String>,
    Query(query): Query<PutRequestQueryParams>,
    value: String,
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
async fn delete_key(
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
async fn compare_and_swap(
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
