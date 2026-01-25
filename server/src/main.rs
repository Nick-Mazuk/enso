#![cfg_attr(test, allow(clippy::disallowed_methods))]
// Forbid unwrap() in production code to prevent panics from corrupt data.
// Test code is allowed to use unwrap() for convenience.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
    ClientConnection, proto,
    storage::{ChangeRecord, ChangeType, Database},
    subscription::{
        ClientSubscriptions, create_subscription_update, log_record_to_change_record,
        proto_hlc_to_storage,
    },
};
use tokio::sync::broadcast;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Shared database for all connections.
type SharedDatabase = Arc<Mutex<Database>>;

#[derive(Clone)]
#[allow(clippy::disallowed_methods)] // Arc::clone is safe and expected for shared state
struct AppState {
    /// Shared database - each WebSocket connection creates its own `ClientConnection`
    /// using this shared database.
    shared_database: SharedDatabase,
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

    // Create or open the database
    let db_path = PathBuf::from("enso.db");
    let (database, recovery_result) = Database::open_or_create(&db_path).unwrap_or_else(|e| {
        tracing::error!("Failed to open database: {e}");
        std::process::exit(1);
    });

    // Log recovery info if recovery was performed
    if let Some(result) = recovery_result {
        tracing::info!(
            "Database recovery completed: {} records scanned, {} transactions replayed, {} discarded",
            result.records_scanned,
            result.transactions_replayed,
            result.transactions_discarded
        );
    }

    // Create a shared database that all connections will use
    let shared_database: SharedDatabase = Arc::new(Mutex::new(database));
    let state = AppState { shared_database };

    let app = Router::new()
        .route("/ws", any(ws_handler))
        .with_state(state);

    // Connect to the websocket on ws://127.0.0.1:3000/ws
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
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
    // Create a per-connection ClientConnection using the shared database
    let client_connection = ClientConnection::new_shared(Arc::clone(&state.shared_database));

    // Per-connection subscription tracking
    let mut subscriptions = ClientSubscriptions::new();

    // Subscribe to the broadcast channel for change notifications (from the database)
    let mut change_rx = match client_connection.subscribe_to_changes() {
        Ok(rx) => rx,
        Err(e) => {
            tracing::error!("Failed to subscribe to changes: {e}");
            return;
        }
    };

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

                let request_id = client_message.request_id;
                tracing::debug!("received ClientMessage with request_id: {:?}", request_id);

                // Handle subscribe/unsubscribe specially
                match &client_message.payload {
                    Some(proto::client_message::Payload::Subscribe(req)) => {
                        if let Err(e) = handle_subscribe(&mut socket, &client_connection, &mut subscriptions, request_id, req).await {
                            tracing::debug!("subscribe handling failed: {e}");
                            return;
                        }
                    }
                    Some(proto::client_message::Payload::Unsubscribe(req)) => {
                        if let Err(e) = handle_unsubscribe(&mut socket, &mut subscriptions, request_id, req).await {
                            tracing::debug!("unsubscribe handling failed: {e}");
                            return;
                        }
                    }
                    _ => {
                        // Handle other messages (query, update) through ClientConnection
                        let server_message = client_connection.handle_message(client_message).await;
                        let response_bytes = server_message.encode_to_vec();
                        if socket.send(Message::Binary(response_bytes.into())).await.is_err() {
                            tracing::debug!("client disconnected");
                            return;
                        }
                    }
                }
            }

            // Handle broadcast notifications for subscriptions
            // (FilteredChangeReceiver automatically excludes this connection's own writes)
            notification = change_rx.recv() => {
                match notification {
                    Ok(change) => {
                        // Convert storage change records to proto format
                        let proto_changes: Vec<proto::ChangeRecord> = change.changes.iter().map(storage_change_to_proto).collect();

                        // Forward changes to all matching subscriptions
                        for sub in subscriptions.iter() {
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

/// Send a success response to the client.
async fn send_ok_response(socket: &mut WebSocket, request_id: Option<u32>) -> Result<(), ()> {
    let response = proto::ServerMessage {
        payload: Some(proto::server_message::Payload::Response(
            proto::ServerResponse {
                request_id,
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Ok.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    };
    let response_bytes = response.encode_to_vec();
    socket
        .send(Message::Binary(response_bytes.into()))
        .await
        .map_err(|_| ())
}

/// Handle a subscribe request.
async fn handle_subscribe(
    socket: &mut WebSocket,
    client_connection: &ClientConnection,
    subscriptions: &mut ClientSubscriptions,
    request_id: Option<u32>,
    req: &proto::SubscribeRequest,
) -> Result<(), &'static str> {
    let subscription_id = req.subscription_id;
    let since_hlc = req.since_hlc.as_ref().map(proto_hlc_to_storage);

    // Add the subscription
    if let Err(e) = subscriptions.add(subscription_id, since_hlc) {
        let msg = format!("{e}");
        send_error_response(socket, request_id, &msg)
            .await
            .map_err(|()| "failed to send error response")?;
        return Ok(());
    }

    // If since_hlc was provided, send historical changes
    if let Some(hlc) = since_hlc {
        match client_connection.get_changes_since(hlc) {
            Ok(log_records) => {
                // Convert log records to change records
                let mut changes = Vec::new();
                for record in &log_records {
                    match log_record_to_change_record(record) {
                        Ok(Some(change)) => changes.push(change),
                        Ok(None) => {} // Skip non-change records
                        Err(e) => {
                            tracing::warn!("failed to convert log record: {e}");
                        }
                    }
                }

                if !changes.is_empty() {
                    // Send initial subscription update with historical changes
                    let update = create_subscription_update(subscription_id, &changes);
                    let msg = proto::ServerMessage {
                        payload: Some(proto::server_message::Payload::SubscriptionUpdate(update)),
                    };
                    let bytes = msg.encode_to_vec();
                    socket
                        .send(Message::Binary(bytes.into()))
                        .await
                        .map_err(|_| "failed to send initial subscription update")?;
                }
            }
            Err(e) => {
                tracing::warn!("failed to get changes since HLC: {e}");
                // Continue anyway - subscription is registered, just no backfill
            }
        }
    }

    // Send success response
    send_ok_response(socket, request_id)
        .await
        .map_err(|()| "failed to send ok response")?;

    tracing::debug!("subscription {} registered", subscription_id);
    Ok(())
}

/// Handle an unsubscribe request.
async fn handle_unsubscribe(
    socket: &mut WebSocket,
    subscriptions: &mut ClientSubscriptions,
    request_id: Option<u32>,
    req: &proto::UnsubscribeRequest,
) -> Result<(), &'static str> {
    let subscription_id = req.subscription_id;

    if let Err(e) = subscriptions.remove(subscription_id) {
        let msg = format!("{e}");
        send_error_response(socket, request_id, &msg)
            .await
            .map_err(|()| "failed to send error response")?;
        return Ok(());
    }

    send_ok_response(socket, request_id)
        .await
        .map_err(|()| "failed to send ok response")?;

    tracing::debug!("subscription {} removed", subscription_id);
    Ok(())
}

/// Convert a storage change record to a proto change record.
#[allow(clippy::disallowed_methods)] // Clone needed for String conversion
fn storage_change_to_proto(change: &ChangeRecord) -> proto::ChangeRecord {
    let change_type = match change.change_type {
        ChangeType::Insert => proto::ChangeType::Insert,
        ChangeType::Update => proto::ChangeType::Update,
        ChangeType::Delete => proto::ChangeType::Delete,
    };

    let value = change.value.as_ref().map(|v| {
        use server::storage::TripleValue;
        proto::TripleValue {
            value: Some(match v {
                TripleValue::Null => proto::triple_value::Value::String(String::new()), // Null not directly representable
                TripleValue::String(s) => proto::triple_value::Value::String(s.clone()),
                TripleValue::Number(n) => proto::triple_value::Value::Number(*n),
                TripleValue::Boolean(b) => proto::triple_value::Value::Boolean(*b),
            }),
        }
    });

    proto::ChangeRecord {
        change_type: change_type.into(),
        triple: Some(proto::Triple {
            entity_id: Some(change.entity_id.to_vec()),
            attribute_id: Some(change.attribute_id.to_vec()),
            value,
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: change.hlc.physical_time,
                logical_counter: change.hlc.logical_counter,
                node_id: change.hlc.node_id,
            }),
        }),
    }
}
