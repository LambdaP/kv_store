use std::collections::HashMap;
use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use tokio::sync::RwLock;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, put},
    Router,
};

use tracing::{debug, error, info};

const DEFAULT_PORT: u16 = 3000;

#[derive(Debug, Default, Clone)]
struct KvStore(Arc<RwLock<HashMap<String, String>>>);

impl KvStore {
    #[tracing::instrument(level = "trace", skip())]
    fn new() -> KvStore {
        KvStore::default()
    }

    #[tracing::instrument(level = "trace", skip(self, key, value))]
    async fn insert(&mut self, key: String, value: String) -> Option<String> {
        debug!("Inserting key: {}", key);
        self.0.write().await.insert(key, value)
    }

    #[tracing::instrument(level = "trace", skip(self, key))]
    async fn get(&self, key: &str) -> Option<String> {
        debug!("Getting key: {}", key);
        self.0.read().await.get(key).cloned()
    }

    #[tracing::instrument(level = "trace", skip(self, key))]
    async fn remove(&mut self, key: &str) -> Option<String> {
        debug!("Removing key: {}", key);
        self.0.write().await.remove(key)
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

    let kv_store = KvStore::new();

    let app = Router::new()
        .route("/store/:key", get(get_key))
        .route("/store/:key", put(put_key_val))
        .route("/store/:key", delete(delete_key))
        .with_state(kv_store);

    let listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[tracing::instrument(level = "trace", skip(kv_store))]
async fn get_key(State(kv_store): State<KvStore>, Path(key): Path<String>) -> impl IntoResponse {
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
    State(mut kv_store): State<KvStore>,
    Path(key): Path<String>,
    value: String,
) -> impl IntoResponse {
    kv_store.insert(key, value).await;
    info!("Key inserted.");
    StatusCode::OK
}

#[tracing::instrument(level = "trace", skip(kv_store))]
async fn delete_key(
    State(mut kv_store): State<KvStore>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    if kv_store.remove(&key).await.is_some() {
        info!("Key deleted: {}", key);
        Ok(())
    } else {
        info!("Key not found for deletion: {}", key);
        Err(StatusCode::NOT_FOUND)
    }
}
