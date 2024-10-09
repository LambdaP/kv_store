use std::{env, sync::Arc, time::Duration};

use axum::{
    extract::{Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use time::{format_description::well_known::Iso8601, OffsetDateTime};

use crate::{
    store::{Metered, SimpleRequest},
    utf8_bytes::Utf8Bytes,
};

type Db = Metered;

const MAX_BATCH_SIZE: usize = 1024;

// TODO most responses here
//   should probably include "Cache-Control: no-cache"
//   as a header

#[derive(Debug, Default, Deserialize)]
pub struct CasPayload {
    expected: Utf8Bytes,
    new: Utf8Bytes,
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
    watch_requests_count: usize,
    batch_requests_count: usize,
    server_time: String,
    version: String,
}

#[tracing::instrument(level = "trace", skip(db))]
pub async fn get_key(State(db): State<Arc<Db>>, Path(key): Path<String>) -> impl IntoResponse {
    db.get(&key).await
}

#[tracing::instrument(level = "trace", skip(db, value))]
pub async fn put_key_val(
    State(db): State<Arc<Db>>,
    Path(key): Path<String>,
    Query(query): Query<PutRequestQueryParams>,
    value: Utf8Bytes,
) -> impl IntoResponse {
    let ttl = query.ttl.map(Duration::from_secs);
    db.insert(key, value, ttl).await
}

#[tracing::instrument(level = "trace", skip(db))]
pub async fn delete_key(State(db): State<Arc<Db>>, Path(key): Path<String>) -> impl IntoResponse {
    db.remove(&key).await
}

#[tracing::instrument(level = "trace", skip(db, cas_payload))]
pub async fn compare_and_swap(
    State(db): State<Arc<Db>>,
    Path(key): Path<String>,
    Json(cas_payload): Json<CasPayload>,
) -> impl IntoResponse {
    db.compare_and_swap(&key, &cas_payload.expected, cas_payload.new)
        .await
}

#[tracing::instrument(level = "trace", skip(db))]
pub async fn watch_key(State(db): State<Arc<Db>>, Path(key): Path<String>) -> impl IntoResponse {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use tokio_stream::StreamExt;

    tracing::trace!("Watching key {key:?}");

    let stream = db.watch_key(key).await;

    let response_stream = stream.map(|(key, result)| match result {
        Ok((cmd, body)) => {
            let event = Event::default()
                .event(format!("{cmd} key {key}"))
                .data(body);
            if let Some(cnt) = cmd.get_counter() {
                return Ok(event.id(cnt.to_string()));
            }
            Ok(event)
        }
        Err(e) => Err(format!("Stream error for key {key}: {e}")),
    });

    Sse::new(response_stream).keep_alive(KeepAlive::default())
}

#[tracing::instrument(level = "trace", skip(db))]
pub async fn watch_multiple_keys(
    State(db): State<Arc<Db>>,
    Json(keys): Json<Vec<String>>,
) -> impl IntoResponse {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use tokio_stream::StreamExt;

    tracing::trace!("Watching multiple keys {keys:?}");

    if keys.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let stream = db.watch_multiple_keys(keys).await;

    let response_stream = stream.map(|(key, result)| match result {
        Ok((cmd, body)) => {
            let event = Event::default()
                .event(format!("{cmd} key {key}"))
                .data(body);
            if let Some(cnt) = cmd.get_counter() {
                return Ok(event.id(cnt.to_string()));
            }
            Ok(event)
        }
        Err(e) => Err(format!("Stream error for key {key}: {e}")),
    });

    Ok(Sse::new(response_stream).keep_alive(KeepAlive::default()))
}

#[tracing::instrument(level = "trace", skip(db, requests))]
pub async fn batch_process(
    State(db): State<Arc<Db>>,
    Json(requests): Json<Vec<SimpleRequest>>,
) -> impl IntoResponse {
    if requests.len() > MAX_BATCH_SIZE {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    let responses = db.batch_process(requests).await;

    Ok(Json(responses))
}

#[tracing::instrument(level = "trace", skip(db))]
pub async fn status(State(db): State<Arc<Db>>) -> impl IntoResponse {
    (StatusCode::OK, Json(make_status(&db).await))
}

#[tracing::instrument(level = "trace", skip(db))]
async fn make_status(db: &Db) -> StatusResponse {
    use crate::store::CountOps;

    let total_keys = db.keys_len().await;
    let counters = db.get_ops_counters();

    let get_operations = counters[CountOps::Get];
    let put_operations = counters[CountOps::Put];
    let delete_operations = counters[CountOps::Delete];
    let successful_cas_operations = counters[CountOps::CasSuccess];
    let failed_cas_operations = counters[CountOps::CasFailure];
    // TODO this logic belongs to `Metered`
    let cas_operations = successful_cas_operations + failed_cas_operations;
    let total_operations = get_operations + put_operations + delete_operations + cas_operations;

    let watch_requests_count = db.get_watch_requests_count();
    let batch_requests_count = db.get_batch_requests_count();

    let version = env!("CARGO_PKG_VERSION").into();

    let uptime = format!("{:.3}", db.uptime());

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
        watch_requests_count,
        batch_requests_count,
        server_time,
        version,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        make_app,
        store::SimpleResponse
    };
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        Router,
    };
    use std::sync::Arc;
    use tower::ServiceExt; // for `oneshot`

    #[tracing::instrument(level = "trace", skip())]
    async fn setup_app() -> Router {
        let db = Arc::new(Db::new());
        make_app(db)
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

    #[tokio::test]
    async fn test_watch_single_key() {
        use tokio_stream::StreamExt;

        let app = setup_app().await;
        let key = "test_watch_key";

        // Start watching the key
        let watch_request = Request::builder()
            .method("GET")
            .uri(format!("/watch_key/{}", key))
            .body(Body::empty())
            .unwrap();

        let watch_response = app.clone().oneshot(watch_request).await.unwrap();
        assert_eq!(watch_response.status(), StatusCode::OK);

        // Convert the response body into a stream of events
        let mut body_stream = watch_response.into_body().into_data_stream();

        // Check the initial SSE
        let chunk = body_stream
            .next()
            .await
            .expect("Expected initial WATCH event, but none was received")
            .unwrap();
        let event = std::str::from_utf8(&chunk).unwrap();

        assert!(
            event.contains(&format!("event: WATCH key {}", key)),
            "Expected initial WATCH event, got: {}",
            event
        );

        // Perform operations on the key
        let operations = vec![
            ("PUT", "initial_value", StatusCode::CREATED),
            ("PUT", "updated_value", StatusCode::NO_CONTENT),
            ("DELETE", "", StatusCode::NO_CONTENT),
        ];

        for (operation, value, expected_code) in operations {
            let request = match operation {
                "PUT" => Request::builder()
                    .method("PUT")
                    .uri(format!("/store/{}", key))
                    .body(Body::from(value.to_string()))
                    .unwrap(),
                "DELETE" => Request::builder()
                    .method("DELETE")
                    .uri(format!("/store/{}", key))
                    .body(Body::empty())
                    .unwrap(),
                _ => panic!("Unsupported operation"),
            };

            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), expected_code);

            let chunk = body_stream.next().await.unwrap().unwrap();

            let event = std::str::from_utf8(&chunk).unwrap();
            assert!(event.contains(&format!("event: {} key {}", operation, key)));
            if operation == "PUT" {
                assert!(event.contains(value));
            }
        }
    }
}
