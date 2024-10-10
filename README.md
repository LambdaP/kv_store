# Concurrent Key-Value Store with Axum

A concurrent key-value store implemented in Rust using the Axum web framework.

## Implementation Overview

This project implements a in-memory key-value store with the following characteristics:

- Concurrent access using Tokio's asynchronous runtime
- RESTful API built with Axum
- Lock-free reads for improved concurrency
- Compare-and-swap (CAS) operations for atomic updates
- Time-to-live (TTL) support for key expiration
- Server-Sent Events (SSE) for key change notifications
- Batch operations for multiple key-value manipulations

## Key Components

### Data Structure

- `LockMap`: Custom concurrent map implementation using `tokio::sync::RwLock` for write operations and inner mutability (using `std::sync::Mutex`) for read operations
- `Utf8Bytes`: Wrapper around `bytes::Bytes` ensuring UTF-8 validity

### API Endpoints

- `GET /store/:key`: Retrieve a value
- `PUT /store/:key`: Insert or update a value
- `DELETE /store/:key`: Remove a key-value pair
- `POST /store/cas/:key`: Compare-and-swap operation
- `GET /watch_key/:key`: Watch a single key for changes
- `POST /watch`: Watch multiple keys for changes
- `POST /batch`: Perform multiple operations in a single request
- `GET /status`: Retrieve store metrics

### Concurrency Model

- Read operations use `RwLock::read()` with inner mutability for updates
- Write operations use `RwLock::write()` for exclusive access
- Compare-and-swap uses optimistic locking with `RwLock::read()`

### Testing

- Unit tests for individual components (`LockMap`, `Utf8Bytes`)
- Integration tests for API endpoints

## Usage

To run the server:

```bash
cargo run
```

API interaction examples:

```bash
# Insert a key-value pair
curl -X PUT http://localhost:3000/store/key1 -d "value1"

# Retrieve a value
curl http://localhost:3000/store/key1

# Delete a key-value pair
curl -X DELETE http://localhost:3000/store/key1

# Compare-and-swap
curl -X POST http://localhost:3000/store/cas/key1 -H "Content-Type: application/json" -d '{"expected":"old_value","new":"new_value"}'

# Watch a single key for changes
curl http://localhost:3000/watch_key/key1

# Watch multiple keys for changes
curl -X POST http://localhost:3000/watch -H "Content-Type: application/json" -d '["key1", "key2", "key3"]'
```

## Design Considerations

- Use of `bytes::Bytes` for efficient memory management of values
- Custom `Utf8Bytes` type to ensure UTF-8 validity while maintaining the efficiency of `Bytes`
- Separation of concerns between the storage layer (`LockMap`) and the API layer
- Use of `tower-governor` for rate limiting

## Limitations and Future Work

- Current implementation is in-memory only; no persistence
- No support for distributed operations or clustering
- Potential for reader starvation in high-contention scenarios due to the use of `RwLock`

## Dependencies

- `axum`: Web framework
- `tokio`: Asynchronous runtime
- `tower-governor`: Rate limiting
- `tracing`: Logging and instrumentation
- `serde`: Serialization and deserialization of JSON payloads
- `bytes`: Efficient byte buffer management

This project serves as a practical exploration of concurrent data structure design and RESTful API implementation in Rust.

## License

See the [LICENSE.md](LICENSE.md) file for license rights and limitations (AGPLv3)
