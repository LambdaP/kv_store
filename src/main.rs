use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, put},
    Router,
};

use tracing::{debug, info};

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

    let kv_store = KvStore::new();

    let app = Router::new()
        .route("/store/:key", get(get_key))
        .route("/store/:key", put(put_key_val))
        .route("/store/:key", delete(delete_key))
        .with_state(kv_store);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
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
