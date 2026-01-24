//! Common helpers for end-to-end tests.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::client_connection::ClientConnection;
use crate::proto;
use crate::storage::Database;

/// Counter for generating unique test database IDs.
static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// RAII guard that cleans up the database file on drop.
pub struct TestClient {
    pub client: ClientConnection,
    pub runtime: tokio::runtime::Runtime,
    db_path: PathBuf,
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
        let client = ClientConnection::new(database);

        #[allow(clippy::expect_used)]
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");

        Self {
            client,
            runtime,
            db_path,
        }
    }

    /// Send a message and return the response.
    pub fn handle_message(&self, message: proto::ClientMessage) -> proto::ServerResponse {
        let response = self
            .runtime
            .block_on(async { self.client.handle_message(message).await });

        #[allow(clippy::expect_used)]
        response.response.expect("Response should be present")
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.db_path);
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
