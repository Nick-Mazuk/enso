use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::{Database, DatabaseError};

static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Create a new test database using a temporary file.
///
/// Each call creates a unique database file in the system temp directory.
/// The database file is not automatically cleaned up; tests should be run
/// in a clean environment or clean up manually if needed.
pub fn new_test_database() -> Result<Database, DatabaseError> {
    let temp_dir = std::env::temp_dir();
    let counter = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
    let unique_name = format!(
        "enso_test_{}_{}.db",
        std::process::id(),
        counter
    );
    let path = temp_dir.join(unique_name);

    // Remove if it exists from a previous run
    let _ = std::fs::remove_file(&path);

    Database::create(&path)
}
