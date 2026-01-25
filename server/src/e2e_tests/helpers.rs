//! Common helpers for end-to-end tests.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::client_connection::ClientConnection;
use crate::proto;
use crate::storage::{Database, FilteredChangeReceiver};

/// Counter for generating unique test database IDs.
static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// RAII guard that cleans up the database file on drop.
pub struct TestClient {
    pub client: ClientConnection,
    pub runtime: tokio::runtime::Runtime,
    db_path: PathBuf,
    /// Shared database reference for creating sibling clients.
    shared_db: Arc<Mutex<Database>>,
}

impl TestClient {
    /// Create a new test client with a fresh database.
    #[must_use]
    pub fn new() -> Self {
        let temp_dir = std::env::temp_dir();
        let instance_id = TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!("e2e_test_{instance_id}.db"));

        // Remove if exists
        let _ = std::fs::remove_file(&db_path);

        #[allow(clippy::expect_used)]
        let database = Database::create(&db_path).expect("Failed to create test database");

        // Database now handles broadcast channel internally
        let client = ClientConnection::new(database);
        let shared_db = client.shared_database();

        #[allow(clippy::expect_used)]
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");

        Self {
            client,
            runtime,
            db_path,
            shared_db,
        }
    }

    /// Create a sibling client that shares the same database.
    ///
    /// This simulates multiple WebSocket connections to the same server,
    /// each with their own `ClientConnection` but sharing the same database.
    #[must_use]
    #[allow(clippy::disallowed_methods)] // Arc::clone is safe and expected
    pub fn create_sibling(&self) -> SiblingClient {
        let shared_db = Arc::clone(&self.shared_db);
        let client = ClientConnection::new_shared(shared_db);
        SiblingClient { client }
    }

    /// Send a message and return the response.
    pub fn handle_message(&self, message: proto::ClientMessage) -> proto::ServerResponse {
        let response = self
            .runtime
            .block_on(async { self.client.handle_message(message).await });

        #[allow(clippy::expect_used)]
        match response.payload.expect("Response should be present") {
            proto::server_message::Payload::Response(r) => r,
            proto::server_message::Payload::SubscriptionUpdate(_) => {
                panic!("Expected Response, got SubscriptionUpdate")
            }
        }
    }

    /// Subscribe to change notifications.
    ///
    /// Returns a receiver that will receive all change notifications broadcast
    /// after this call.
    ///
    /// # Panics
    ///
    /// The returned receiver automatically filters out notifications from
    /// this client's own writes.
    ///
    /// Panics if subscribing to the database fails.
    #[must_use]
    pub fn subscribe_to_changes(&self) -> FilteredChangeReceiver {
        #[allow(clippy::expect_used)]
        self.client
            .subscribe_to_changes()
            .expect("Failed to subscribe to changes")
    }

    /// Get changes since a given HLC timestamp.
    ///
    /// This is used for subscription backfill.
    pub fn get_changes_since(
        &self,
        hlc: crate::storage::HlcTimestamp,
    ) -> Result<Vec<crate::storage::LogRecord>, crate::storage::DatabaseError> {
        self.client.get_changes_since(hlc)
    }

    /// Get the connection ID for this client.
    #[must_use]
    pub fn connection_id(&self) -> crate::storage::ConnectionId {
        self.client.connection_id()
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.db_path);
    }
}

/// A sibling client that shares the same database as its parent `TestClient`.
///
/// This represents a separate WebSocket connection to the same server.
/// It does not own the database file and does not clean it up on drop.
pub struct SiblingClient {
    pub client: ClientConnection,
}

impl SiblingClient {
    /// Send a message and return the response.
    ///
    /// Uses a new runtime for each call since siblings don't own a runtime.
    pub fn handle_message(&self, message: proto::ClientMessage) -> proto::ServerResponse {
        #[allow(clippy::expect_used)]
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        let response = runtime.block_on(async { self.client.handle_message(message).await });

        #[allow(clippy::expect_used)]
        match response.payload.expect("Response should be present") {
            proto::server_message::Payload::Response(r) => r,
            proto::server_message::Payload::SubscriptionUpdate(_) => {
                panic!("Expected Response, got SubscriptionUpdate")
            }
        }
    }

    /// Subscribe to change notifications.
    ///
    /// The returned receiver automatically filters out notifications from
    /// this client's own writes.
    #[must_use]
    pub fn subscribe_to_changes(&self) -> FilteredChangeReceiver {
        #[allow(clippy::expect_used)]
        self.client
            .subscribe_to_changes()
            .expect("Failed to subscribe to changes")
    }

    /// Get the connection ID for this client.
    #[must_use]
    pub fn connection_id(&self) -> crate::storage::ConnectionId {
        self.client.connection_id()
    }
}

// =============================================================================
// ID Generation
// =============================================================================

/// Generate a deterministic entity ID from a seed value.
///
/// Each unique seed produces a unique, valid 16-byte entity ID.
#[must_use]
pub const fn new_entity_id(seed: u8) -> [u8; 16] {
    [
        0xE0 | (seed >> 4), // Entity marker + high nibble
        seed,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        seed ^ 0xFF, // Simple checksum
    ]
}

/// Generate a deterministic attribute ID from a seed value.
///
/// Each unique seed produces a unique, valid 16-byte attribute ID.
#[must_use]
pub const fn new_attribute_id(seed: u8) -> [u8; 16] {
    [
        0xA0 | (seed >> 4), // Attribute marker + high nibble
        seed,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        seed ^ 0xAA, // Simple checksum
    ]
}

// =============================================================================
// Response Helpers
// =============================================================================

/// Check if response has OK status.
#[must_use]
pub fn is_ok(response: &proto::ServerResponse) -> bool {
    response
        .status
        .as_ref()
        .is_some_and(|s| s.code == proto::google::rpc::Code::Ok as i32)
}

/// Get the status code from a response.
#[must_use]
pub fn status_code(response: &proto::ServerResponse) -> i32 {
    response.status.as_ref().map_or(-1, |s| s.code)
}

/// Extract the inner value from a query result row's first column.
#[must_use]
pub fn extract_value(response: &proto::ServerResponse, row: usize) -> Option<&proto::TripleValue> {
    response.rows.get(row).and_then(|r| {
        r.values.first().and_then(|v| match &v.value {
            Some(proto::query_result_value::Value::TripleValue(tv)) => Some(tv),
            _ => None,
        })
    })
}

/// Convenience to get a string value from response.
#[must_use]
pub fn get_string_value(response: &proto::ServerResponse, row: usize) -> Option<&str> {
    extract_value(response, row).and_then(|tv| match &tv.value {
        Some(proto::triple_value::Value::String(s)) => Some(s.as_str()),
        _ => None,
    })
}

/// Convenience to get a number value from response.
#[must_use]
pub fn get_number_value(response: &proto::ServerResponse, row: usize) -> Option<f64> {
    extract_value(response, row).and_then(|tv| match &tv.value {
        Some(proto::triple_value::Value::Number(n)) => Some(*n),
        _ => None,
    })
}

/// Convenience to get a boolean value from response.
#[must_use]
pub fn get_bool_value(response: &proto::ServerResponse, row: usize) -> Option<bool> {
    extract_value(response, row).and_then(|tv| match &tv.value {
        Some(proto::triple_value::Value::Boolean(b)) => Some(*b),
        _ => None,
    })
}

// =============================================================================
// HLC Helpers
// =============================================================================

/// Create a new HLC timestamp for testing.
///
/// Uses a simple pattern where the seed value is used to create a unique HLC.
#[must_use]
pub fn new_hlc(seed: u64) -> proto::HlcTimestamp {
    proto::HlcTimestamp {
        physical_time_ms: seed * 1000,
        logical_counter: 0,
        node_id: 1,
    }
}
