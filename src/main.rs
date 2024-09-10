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

#[derive(Debug, Default, Clone)]
struct KvStore(Arc<RwLock<HashMap<String, String>>>);

impl KvStore {
    fn new() -> KvStore {
        Default::default()
    }

    async fn insert(&mut self, key: String, value: String) -> Option<String> {
        self.0.write().await.insert(key, value)
    }

    async fn get(&self, key: &str) -> Option<String> {
        self.0.read().await.get(key).cloned()
    }

    async fn remove(&mut self, key: &str) -> Option<String> {
        self.0.write().await.remove(key)
    }
}


#[tokio::main]
async fn main() {
    let kv_store = KvStore::new();

    let app = Router::new()
        .route("/store/:key", get(get_key))
        .route("/store/:key", put(put_key_val))
        .route("/store/:key", delete(delete_key))
        .with_state(kv_store);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_key(State(kv_store): State<KvStore>, Path(key): Path<String>) -> impl IntoResponse {
    kv_store.get(&key).await.ok_or(StatusCode::NOT_FOUND)
}

async fn put_key_val(State(mut kv_store): State<KvStore>, Path(key): Path<String>, value: String) {
    kv_store.insert(key, value).await;
}

async fn delete_key(
    State(mut kv_store): State<KvStore>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    kv_store
        .remove(&key)
        .await
        .map(|_| ())
        .ok_or(StatusCode::NOT_FOUND)
}
