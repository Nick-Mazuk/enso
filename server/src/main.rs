#![cfg_attr(test, allow(clippy::disallowed_methods))]
// Forbid unwrap() in production code to prevent panics from corrupt data.
// Test code is allowed to use unwrap() for convenience.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::any,
};
use prost::Message as ProstMessage;
use server::{
    ClientConnection, DatabaseRegistry, config::ServerConfig, proto, types::ProtoSerializable,
};
use tokio::sync::broadcast;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
#[allow(clippy::disallowed_methods)] // Arc::clone is safe and expected for shared state
struct AppState {
    /// Database registry - manages per-app databases.
    /// Each WebSocket connection creates its own `ClientConnection` that
    /// opens/creates the database based on the `app_api_key` in `ConnectRequest`.
    registry: Arc<DatabaseRegistry>,
    /// Server configuration.
    #[allow(dead_code)] // Will be used for admin API key validation
    config: Arc<ServerConfig>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "server=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration from environment variables
    let config = match ServerConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("Failed to load configuration: {e}");
            std::process::exit(1);
        }
    };

    tracing::info!(
        "Loaded configuration: database_directory={}, listen_port={}",
        config.database_directory.display(),
        config.listen_port
    );

    // Create the data directory for databases
    if let Err(e) = std::fs::create_dir_all(&config.database_directory) {
        tracing::error!("Failed to create data directory: {e}");
        std::process::exit(1);
    }

    // Extract fields before consuming config
    let listen_port = config.listen_port;
    let admin_app_api_key = config.admin_app_api_key;

    // Create the database registry - databases are opened on-demand per app_api_key
    // Registry takes ownership of the database directory path
    let registry = Arc::new(DatabaseRegistry::new(config.database_directory));

    // Create config for AppState (currently unused but reserved for admin API key validation)
    let config = Arc::new(ServerConfig {
        admin_app_api_key,
        database_directory: PathBuf::new(),
        listen_port,
    });
    let state = AppState { registry, config };

    let app = Router::new()
        .route("/ws", any(ws_handler))
        .with_state(state);

    // Connect to the websocket on ws://127.0.0.1:<port>/ws
    let addr = SocketAddr::from(([127, 0, 0, 1], listen_port));
    tracing::info!("listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Failed to bind: {e}");
            std::process::exit(1);
        });

    axum::serve(listener, app).await.unwrap_or_else(|e| {
        tracing::error!("Server error: {e}");
        std::process::exit(1);
    });
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    tracing::debug!("got a websocket connection");
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

#[allow(clippy::too_many_lines, clippy::disallowed_methods)]
async fn handle_socket(mut socket: WebSocket, state: AppState) {
    // Create a per-connection ClientConnection that awaits ConnectRequest
    let mut client_connection = ClientConnection::new_awaiting_connect(Arc::clone(&state.registry));

    // Change receiver - will be set up after ConnectRequest is processed
    let mut change_rx: Option<server::storage::FilteredChangeReceiver> = None;

    loop {
        tokio::select! {
            // Handle incoming WebSocket messages
            msg = socket.recv() => {
                let msg = match msg {
                    Some(Ok(msg)) => msg,
                    Some(Err(e)) => {
                        tracing::warn!("websocket receive error: {e}");
                        return;
                    }
                    None => {
                        tracing::debug!("client disconnected");
                        return;
                    }
                };

                // Only process binary messages (protobuf)
                let data = match msg {
                    Message::Binary(data) => data,
                    Message::Text(text) => {
                        tracing::debug!("received text message (ignoring): {text}");
                        continue;
                    }
                    Message::Ping(data) => {
                        if socket.send(Message::Pong(data)).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => {
                        tracing::debug!("client sent close");
                        return;
                    }
                };

                // Decode the ClientMessage
                let client_message = match proto::ClientMessage::decode(data.as_ref()) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!("failed to decode ClientMessage: {e}");
                        if send_error_response(&mut socket, None, &format!("Failed to decode message: {e}")).await.is_err() {
                            return;
                        }
                        continue;
                    }
                };

                tracing::debug!("received ClientMessage with request_id: {:?}", client_message.request_id);

                // Handle the message through ClientConnection
                let messages = client_connection.handle_message(client_message);
                for msg in messages {
                    let bytes = msg.encode_to_vec();
                    if socket.send(Message::Binary(bytes.into())).await.is_err() {
                        tracing::debug!("client disconnected");
                        return;
                    }
                }

                // If we just connected, set up the change receiver for subscriptions
                if change_rx.is_none() && client_connection.is_connected() {
                    match client_connection.subscribe_to_changes() {
                        Ok(rx) => {
                            change_rx = Some(rx);
                        }
                        Err(e) => {
                            tracing::error!("Failed to subscribe to changes: {e}");
                            return;
                        }
                    }
                }
            }

            // Handle broadcast notifications for subscriptions
            // (FilteredChangeReceiver automatically excludes this connection's own writes)
            // Only active after connection is established
            notification = async {
                match &mut change_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match notification {
                    Ok(change) => {
                        // Convert storage change records to proto format
                        let proto_changes: Vec<proto::ChangeRecord> =
                            change.changes.iter().map(ProtoSerializable::to_proto).collect();

                        // Forward changes to all matching subscriptions
                        for sub in client_connection.subscriptions() {
                            // Filter changes based on subscription's since_hlc if applicable
                            // For now, send all changes to all subscriptions
                            // (since_hlc filtering was already done during initial backfill)
                            let update = proto::SubscriptionUpdate {
                                subscription_id: sub.id,
                                changes: proto_changes.clone(),
                            };
                            let msg = proto::ServerMessage {
                                payload: Some(proto::server_message::Payload::SubscriptionUpdate(update)),
                            };
                            let bytes = msg.encode_to_vec();
                            if socket.send(Message::Binary(bytes.into())).await.is_err() {
                                tracing::debug!("client disconnected during subscription update");
                                return;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!("subscription receiver lagged by {count} messages");
                        // Continue processing - we may have missed some updates
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::debug!("broadcast channel closed");
                        return;
                    }
                }
            }
        }
    }
}

/// Send an error response to the client.
async fn send_error_response(
    socket: &mut WebSocket,
    request_id: Option<u32>,
    message: &str,
) -> Result<(), ()> {
    let error_response = proto::ServerMessage {
        payload: Some(proto::server_message::Payload::Response(
            proto::ServerResponse {
                request_id,
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::InvalidArgument.into(),
                    message: message.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    };
    let response_bytes = error_response.encode_to_vec();
    socket
        .send(Message::Binary(response_bytes.into()))
        .await
        .map_err(|_| ())
}
