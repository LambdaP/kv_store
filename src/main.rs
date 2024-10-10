use std::{
    env,
    future::IntoFuture,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};

use tokio::{
    signal,
    time::{timeout, Duration},
};
use tokio_util::sync::CancellationToken;
use tower_governor::{governor::GovernorConfig, GovernorLayer};
use tracing::{error, info, warn};

use kv_store::{routes, Metered};

type Db = Metered;

const DEFAULT_PORT: u16 = 3000;

#[tracing::instrument(level = "trace", skip())]
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let port = get_port_from_env().unwrap_or(DEFAULT_PORT);

    let socket_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    let listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();

    let db = Arc::new(Db::new());

    let cancel_token = CancellationToken::new();

    let cleanup_db = db.clone();
    let cleanup_task = tokio::spawn({
        let cleanup_loop = cleanup_db.keys_cleanup_loop();
        let cleanup_token = cancel_token.child_token();

        async move {
            cleanup_token.run_until_cancelled(cleanup_loop).await;
            info!("Cleanup task was cancelled");
        }
    });

    let server_token = cancel_token.child_token();

    let server = axum::serve(
        listener,
        routes::make_app(db.clone())
            .layer(GovernorLayer {
                config: Arc::new(GovernorConfig::default()),
            })
            // The following is required
            //   for the Governor layer to work properly.
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(server_token.cancelled_owned());

    let server_task = tokio::spawn(server.into_future());

    let () = shutdown_signal().await;

    info!("Initiating server shutdown");

    cancel_token.cancel();

    let server_shutdown = tokio::spawn(async {
        match timeout(Duration::from_secs(30), server_task).await {
            Ok(Err(e)) => {
                error!("Server error: {}", e);
            }
            Ok(_) => {
                info!("Server task shut down normally");
            }
            _ => {
                warn!("Server shutdown timed out after 30 seconds");
            }
        }
    });

    let cleanup_shutdown = tokio::spawn(async {
        match timeout(Duration::from_secs(10), cleanup_task).await {
            Ok(Err(e)) => {
                error!("Cleanup task error: {}", e);
            }
            Ok(_) => {
                info!("Cleanup task shut down normally");
            }
            _ => {
                warn!("Cleanup task shutdown timed out after 10 seconds");
            }
        }
    });

    _ = tokio::join!(server_shutdown, cleanup_shutdown, db.cleanup_shutdown());

    info!("Server shutdown complete");
}

fn get_port_from_env() -> Option<u16> {
    env::var("KV_STORE_PORT")
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
            .ok()
}

async fn shutdown_signal() {
    let ctrl_c = async { signal::ctrl_c().await.unwrap() };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .unwrap()
            .recv()
            .await;
    };

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }

    info!("Shutdown signal received");
}
