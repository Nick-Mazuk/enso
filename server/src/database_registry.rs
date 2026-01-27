//! Registry of open databases, keyed by `app_api_key`.
//!
//! This module provides a thread-safe registry that manages database instances.
//! Multiple connections with the same `app_api_key` share a single `Database` instance,
//! enabling subscription broadcasting across connections.
//!
//! # Thread Safety
//!
//! The registry uses `RwLock` instead of `Mutex` to allow concurrent database access:
//! - Multiple threads can read from the same database simultaneously
//! - Write operations acquire exclusive access
//!
//! # Invariants
//!
//! - Each `app_api_key` maps to exactly one `Database` instance
//! - Database instances are never removed once created (for the lifetime of the registry)
//! - All `app_api_key` values are validated before use

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::storage::buffer_pool::{BufferPool, DEFAULT_POOL_CAPACITY};
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

        let db_arc = Arc::new(RwLock::new(database));
        databases.insert(app_api_key.to_string(), Arc::clone(&db_arc));

        tracing::info!("Opened database for app '{}'", app_api_key);

        Ok(db_arc)
    }
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
}
