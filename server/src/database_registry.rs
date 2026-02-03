//! Registry of open databases, keyed by `app_api_key`.
//!
//! This module provides a thread-safe registry that manages database instances.
//! Multiple connections with the same `app_api_key` share a single `Database` instance,
//! enabling subscription broadcasting across connections.
//!
//! # Database Types
//!
//! - **Shared databases**: One per app, stored at `{app_api_key}.db`
//! - **User databases**: One per user per app, stored at `{app_api_key}/{user_id_hash}.db`
//!   where `user_id_hash` is the hex-encoded SHA-256 hash of the user ID
//!
//! # Thread Safety
//!
//! The registry uses `RwLock` instead of `Mutex` to allow concurrent database access:
//! - Multiple threads can read from the same database simultaneously
//! - Write operations acquire exclusive access
//!
//! # Invariants
//!
//! - Each database key maps to exactly one `Database` instance
//! - Database instances are never removed once created (for the lifetime of the registry)
//! - All `app_api_key` values are validated before use

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use sha2::{Digest, Sha256};

use crate::storage::buffer_pool::{BufferPool, DEFAULT_POOL_CAPACITY};
use crate::storage::gc::{GcConfig, spawn_gc_task};
use crate::storage::{Database, DatabaseError};

/// Maximum length for an `app_api_key`.
const MAX_API_KEY_LENGTH: usize = 256;

/// Registry of open databases, keyed by `app_api_key`.
///
/// Enables multiple connections with the same `app_api_key` to share one Database instance.
/// This is necessary for subscription broadcasting to work across connections.
pub struct DatabaseRegistry {
    /// Map from `app_api_key` to shared database instance.
    /// Uses `RwLock` to allow concurrent reads of the map.
    databases: RwLock<HashMap<String, Arc<RwLock<Database>>>>,
    /// Base directory where database files are stored.
    base_directory: PathBuf,
    /// Shared buffer pool for all databases.
    buffer_pool: Arc<BufferPool>,
}

impl DatabaseRegistry {
    /// Create a new database registry.
    ///
    /// # Arguments
    ///
    /// * `base_directory` - Directory where database files will be stored.
    ///   Each app's database will be at `{base_directory}/{app_api_key}.db`.
    ///
    /// Initializes a shared 2GB buffer pool for all databases.
    #[must_use]
    pub fn new(base_directory: PathBuf) -> Self {
        Self {
            databases: RwLock::new(HashMap::new()),
            base_directory,
            buffer_pool: BufferPool::new(DEFAULT_POOL_CAPACITY),
        }
    }

    /// Create a new database registry with a custom buffer pool capacity.
    ///
    /// # Arguments
    ///
    /// * `base_directory` - Directory where database files will be stored.
    /// * `pool_capacity` - Number of 8KB pages in the shared buffer pool.
    #[must_use]
    pub fn with_pool_capacity(base_directory: PathBuf, pool_capacity: usize) -> Self {
        Self {
            databases: RwLock::new(HashMap::new()),
            base_directory,
            buffer_pool: BufferPool::new(pool_capacity),
        }
    }

    /// Get or create a database for the given `app_api_key`.
    ///
    /// If a database for this key already exists, returns a reference to it.
    /// Otherwise, opens or creates a new database file and stores it in the registry.
    ///
    /// # Pre-conditions
    ///
    /// - `app_api_key` must be valid (non-empty, valid characters, reasonable length)
    ///   Use `validate_api_key` to check before calling.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The registry lock is poisoned
    /// - The database cannot be opened or created
    #[allow(clippy::disallowed_methods)] // Arc::clone is safe and expected
    #[allow(clippy::significant_drop_tightening)] // False positive - we need the lock held during insert
    pub fn get_or_create(&self, app_api_key: &str) -> Result<Arc<RwLock<Database>>, DatabaseError> {
        // Fast path: check if database already exists (read lock only)
        {
            let databases = self
                .databases
                .read()
                .map_err(|_| DatabaseError::LockPoisoned)?;
            if let Some(db) = databases.get(app_api_key) {
                return Ok(Arc::clone(db));
            }
        }

        // Slow path: need to create the database (write lock)
        let mut databases = self
            .databases
            .write()
            .map_err(|_| DatabaseError::LockPoisoned)?;

        // Double-check: another thread may have created it while we waited for the write lock
        if let Some(db) = databases.get(app_api_key) {
            return Ok(Arc::clone(db));
        }

        // Create the database
        let db_path = self.base_directory.join(format!("{app_api_key}.db"));
        let (database, recovery_result) =
            Database::open_or_create(&db_path, Arc::clone(&self.buffer_pool))?;

        if let Some(result) = recovery_result {
            tracing::info!(
                "Database recovery for '{}': {} records scanned, {} transactions replayed, {} discarded",
                app_api_key,
                result.records_scanned,
                result.transactions_replayed,
                result.transactions_discarded
            );
        }

        // Get the GC notify handle before wrapping in RwLock
        let gc_notify = database.gc_notify();

        let db_arc = Arc::new(RwLock::new(database));
        databases.insert(app_api_key.to_string(), Arc::clone(&db_arc));

        // Spawn background GC task with weak reference to prevent cycles
        // The task will exit cleanly when the database is dropped
        // Only spawn if we're inside a tokio runtime (may not be in some test contexts)
        if tokio::runtime::Handle::try_current().is_ok() {
            let weak_db = Arc::downgrade(&db_arc);
            let _gc_handle = spawn_gc_task(weak_db, gc_notify, GcConfig::default());
        }

        tracing::info!("Opened database for app '{}'", app_api_key);

        Ok(db_arc)
    }

    /// Get or create a shared database for the given `app_api_key`.
    ///
    /// Shared databases are stored at `{base_directory}/{app_api_key}.db` and are
    /// accessible by all users of the application.
    ///
    /// # Pre-conditions
    ///
    /// - `app_api_key` must be valid (non-empty, valid characters, reasonable length)
    ///   Use `validate_api_key` to check before calling.
    ///
    /// # Post-conditions
    ///
    /// - Returns a shared reference to the database
    /// - Database is created if it doesn't exist
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The registry lock is poisoned
    /// - The database cannot be opened or created
    pub fn get_shared_database(
        &self,
        app_api_key: &str,
    ) -> Result<Arc<RwLock<Database>>, DatabaseError> {
        self.get_or_create(app_api_key)
    }

    /// Get or create a user-specific database for the given `app_api_key` and `user_id`.
    ///
    /// User databases are stored at `{base_directory}/{app_api_key}/{user_id_hash}.db`
    /// where `user_id_hash` is the hex-encoded SHA-256 hash of the `user_id`.
    /// This ensures filesystem-safe paths regardless of the user ID format.
    ///
    /// # Pre-conditions
    ///
    /// - `app_api_key` must be valid (non-empty, valid characters, reasonable length)
    ///   Use `validate_api_key` to check before calling.
    /// - `user_id` must not be empty.
    ///
    /// # Post-conditions
    ///
    /// - Returns a reference to the user's database
    /// - Database is created if it doesn't exist
    /// - The user database directory is created if needed
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The registry lock is poisoned
    /// - The user database directory cannot be created
    /// - The database cannot be opened or created
    #[allow(clippy::disallowed_methods)] // Arc::clone is safe and expected
    #[allow(clippy::significant_drop_tightening)] // False positive - we need the lock held during insert
    pub fn get_user_database(
        &self,
        app_api_key: &str,
        user_id: &str,
    ) -> Result<Arc<RwLock<Database>>, DatabaseError> {
        assert!(!user_id.is_empty(), "user_id must not be empty");

        // Hash the user_id with SHA-256 to get a filesystem-safe path
        let user_id_hash = hash_user_id(user_id);
        let db_key = format!("{app_api_key}/{user_id_hash}");

        // Fast path: check if database already exists (read lock only)
        {
            let databases = self
                .databases
                .read()
                .map_err(|_| DatabaseError::LockPoisoned)?;
            if let Some(db) = databases.get(&db_key) {
                return Ok(Arc::clone(db));
            }
        }

        // Slow path: need to create the database (write lock)
        let mut databases = self
            .databases
            .write()
            .map_err(|_| DatabaseError::LockPoisoned)?;

        // Double-check: another thread may have created it while we waited for the write lock
        if let Some(db) = databases.get(&db_key) {
            return Ok(Arc::clone(db));
        }

        // Ensure the user database directory exists
        let user_db_directory = self.base_directory.join(app_api_key);
        std::fs::create_dir_all(&user_db_directory).map_err(DatabaseError::Io)?;

        // Create the database
        let db_path = user_db_directory.join(format!("{user_id_hash}.db"));
        let (database, recovery_result) =
            Database::open_or_create(&db_path, Arc::clone(&self.buffer_pool))?;

        if let Some(result) = recovery_result {
            tracing::info!(
                "Database recovery for user '{}' in app '{}': {} records scanned, {} transactions replayed, {} discarded",
                user_id_hash,
                app_api_key,
                result.records_scanned,
                result.transactions_replayed,
                result.transactions_discarded
            );
        }

        // Get the GC notify handle before wrapping in RwLock
        let gc_notify = database.gc_notify();

        let db_arc = Arc::new(RwLock::new(database));
        databases.insert(db_key, Arc::clone(&db_arc));

        // Spawn background GC task with weak reference to prevent cycles
        // The task will exit cleanly when the database is dropped
        // Only spawn if we're inside a tokio runtime (may not be in some test contexts)
        if tokio::runtime::Handle::try_current().is_ok() {
            let weak_db = Arc::downgrade(&db_arc);
            let _gc_handle = spawn_gc_task(weak_db, gc_notify, GcConfig::default());
        }

        tracing::info!(
            "Opened user database for app '{}', user hash '{}'",
            app_api_key,
            user_id_hash
        );

        Ok(db_arc)
    }
}

/// Hash a user ID using SHA-256 and return the hex-encoded result.
///
/// # Pre-conditions
///
/// - `user_id` should not be empty (though this function will still hash it).
///
/// # Post-conditions
///
/// - Returns a 64-character lowercase hex string (SHA-256 produces 32 bytes = 64 hex chars).
/// - The same `user_id` always produces the same hash.
/// - Result is filesystem-safe (only alphanumeric characters).
fn hash_user_id(user_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(user_id.as_bytes());
    let result = hasher.finalize();
    // Convert to lowercase hex string
    result.iter().map(|b| format!("{b:02x}")).collect()
}

/// Error returned when validating an `app_api_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiKeyValidationError {
    /// The key is empty.
    Empty,
    /// The key exceeds the maximum length.
    TooLong,
    /// The key contains invalid characters.
    InvalidCharacters,
}

impl std::fmt::Display for ApiKeyValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "app_api_key must not be empty"),
            Self::TooLong => write!(
                f,
                "app_api_key exceeds maximum length of {MAX_API_KEY_LENGTH} characters"
            ),
            Self::InvalidCharacters => write!(
                f,
                "app_api_key contains invalid characters; only alphanumeric, hyphens, and underscores are allowed"
            ),
        }
    }
}

/// Validate that an API key is well-formed.
///
/// Valid keys:
/// - Are non-empty
/// - Are at most 256 characters
/// - Contain only alphanumeric characters, hyphens, and underscores
///
/// This validation prevents path traversal attacks and ensures safe filenames.
///
/// # Examples
///
/// ```
/// use server::database_registry::validate_api_key;
///
/// assert!(validate_api_key("my-app-123").is_ok());
/// assert!(validate_api_key("my_app").is_ok());
/// assert!(validate_api_key("").is_err());
/// assert!(validate_api_key("../evil").is_err());
/// ```
pub fn validate_api_key(key: &str) -> Result<(), ApiKeyValidationError> {
    if key.is_empty() {
        return Err(ApiKeyValidationError::Empty);
    }

    if key.len() > MAX_API_KEY_LENGTH {
        return Err(ApiKeyValidationError::TooLong);
    }

    if !key
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(ApiKeyValidationError::InvalidCharacters);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_api_key_valid() {
        assert!(validate_api_key("my-app").is_ok());
        assert!(validate_api_key("my_app").is_ok());
        assert!(validate_api_key("myApp123").is_ok());
        assert!(validate_api_key("a").is_ok());
        assert!(validate_api_key("test-app-v2").is_ok());
    }

    #[test]
    fn test_validate_api_key_empty() {
        assert_eq!(validate_api_key(""), Err(ApiKeyValidationError::Empty));
    }

    #[test]
    fn test_validate_api_key_too_long() {
        let long_key = "a".repeat(MAX_API_KEY_LENGTH + 1);
        assert_eq!(
            validate_api_key(&long_key),
            Err(ApiKeyValidationError::TooLong)
        );

        // Exactly at limit should be OK
        let at_limit = "a".repeat(MAX_API_KEY_LENGTH);
        assert!(validate_api_key(&at_limit).is_ok());
    }

    #[test]
    fn test_validate_api_key_invalid_characters() {
        assert_eq!(
            validate_api_key("../evil"),
            Err(ApiKeyValidationError::InvalidCharacters)
        );
        assert_eq!(
            validate_api_key("my app"),
            Err(ApiKeyValidationError::InvalidCharacters)
        );
        assert_eq!(
            validate_api_key("app.name"),
            Err(ApiKeyValidationError::InvalidCharacters)
        );
        assert_eq!(
            validate_api_key("app/name"),
            Err(ApiKeyValidationError::InvalidCharacters)
        );
        assert_eq!(
            validate_api_key("app\\name"),
            Err(ApiKeyValidationError::InvalidCharacters)
        );
    }

    #[test]
    fn test_hash_user_id_produces_64_char_hex() {
        let hash = hash_user_id("test-user");
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_hash_user_id_is_deterministic() {
        let hash1 = hash_user_id("user123");
        let hash2 = hash_user_id("user123");
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_user_id_different_inputs_produce_different_hashes() {
        let hash1 = hash_user_id("user1");
        let hash2 = hash_user_id("user2");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_user_id_known_value() {
        // SHA-256 of "test" is known
        let hash = hash_user_id("test");
        assert_eq!(
            hash,
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }

    #[test]
    fn test_hash_user_id_handles_special_characters() {
        // Should work with any UTF-8 string including special chars
        let hash = hash_user_id("user@example.com");
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_get_shared_database_creates_database() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let registry = DatabaseRegistry::new(temp_dir.path().to_path_buf());

        let result = registry.get_shared_database("test-app");
        assert!(result.is_ok());

        // Database file should exist
        let db_path = temp_dir.path().join("test-app.db");
        assert!(db_path.exists());
    }

    #[test]
    fn test_get_shared_database_returns_same_instance() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let registry = DatabaseRegistry::new(temp_dir.path().to_path_buf());

        let db1 = registry.get_shared_database("test-app").expect("First call");
        let db2 = registry
            .get_shared_database("test-app")
            .expect("Second call");

        // Both should point to the same Arc
        assert!(Arc::ptr_eq(&db1, &db2));
    }

    #[test]
    fn test_get_user_database_creates_database_and_directory() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let registry = DatabaseRegistry::new(temp_dir.path().to_path_buf());

        let result = registry.get_user_database("test-app", "user123");
        assert!(result.is_ok());

        // User database directory should exist
        let user_dir = temp_dir.path().join("test-app");
        assert!(user_dir.exists());
        assert!(user_dir.is_dir());

        // Database file should exist with hashed name
        let user_hash = hash_user_id("user123");
        let db_path = user_dir.join(format!("{user_hash}.db"));
        assert!(db_path.exists());
    }

    #[test]
    fn test_get_user_database_returns_same_instance() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let registry = DatabaseRegistry::new(temp_dir.path().to_path_buf());

        let db1 = registry
            .get_user_database("test-app", "user123")
            .expect("First call");
        let db2 = registry
            .get_user_database("test-app", "user123")
            .expect("Second call");

        // Both should point to the same Arc
        assert!(Arc::ptr_eq(&db1, &db2));
    }

    #[test]
    fn test_get_user_database_different_users_get_different_databases() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let registry = DatabaseRegistry::new(temp_dir.path().to_path_buf());

        let db1 = registry
            .get_user_database("test-app", "user1")
            .expect("User 1");
        let db2 = registry
            .get_user_database("test-app", "user2")
            .expect("User 2");

        // Different users should have different databases
        assert!(!Arc::ptr_eq(&db1, &db2));
    }

    #[test]
    fn test_get_user_database_different_apps_get_different_databases() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let registry = DatabaseRegistry::new(temp_dir.path().to_path_buf());

        let db1 = registry
            .get_user_database("app1", "user123")
            .expect("App 1");
        let db2 = registry
            .get_user_database("app2", "user123")
            .expect("App 2");

        // Different apps should have different databases even for the same user
        assert!(!Arc::ptr_eq(&db1, &db2));
    }

    #[test]
    fn test_shared_and_user_databases_are_different() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let registry = DatabaseRegistry::new(temp_dir.path().to_path_buf());

        let shared_db = registry.get_shared_database("test-app").expect("Shared");
        let user_db = registry
            .get_user_database("test-app", "user123")
            .expect("User");

        // Shared and user databases should be different
        assert!(!Arc::ptr_eq(&shared_db, &user_db));
    }

    #[test]
    #[should_panic(expected = "user_id must not be empty")]
    fn test_get_user_database_panics_on_empty_user_id() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let registry = DatabaseRegistry::new(temp_dir.path().to_path_buf());

        let _ = registry.get_user_database("test-app", "");
    }
}
