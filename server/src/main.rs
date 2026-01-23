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
use server::{ClientConnection, proto, storage::Database};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
#[allow(clippy::disallowed_methods)] // Arc::clone is safe and expected for shared state
struct AppState {
    client_connection: Arc<ClientConnection>,
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

    let client_connection = Arc::new(ClientConnection::new(database));
    let state = AppState { client_connection };

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

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    while let Some(msg) = socket.recv().await {
        let msg = match msg {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!("websocket receive error: {e}");
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
                // Send an error response
                let error_response = proto::ServerMessage {
                    response: Some(proto::ServerResponse {
                        request_id: None,
                        status: Some(proto::google::rpc::Status {
                            code: proto::google::rpc::Code::InvalidArgument.into(),
                            message: format!("Failed to decode message: {e}"),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                };
                let response_bytes = error_response.encode_to_vec();
                if socket
                    .send(Message::Binary(response_bytes.into()))
                    .await
                    .is_err()
                {
                    return;
                }
                continue;
            }
        };

        tracing::debug!(
            "received ClientMessage with request_id: {:?}",
            client_message.request_id
        );

        // Handle the message
        let server_message = state.client_connection.handle_message(client_message).await;

        // Encode and send the response
        let response_bytes = server_message.encode_to_vec();
        if socket
            .send(Message::Binary(response_bytes.into()))
            .await
            .is_err()
        {
            tracing::debug!("client disconnected");
            return;
        }
    }
}
