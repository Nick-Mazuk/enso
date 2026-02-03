//! Application configuration registry.
//!
//! Provides a registry for loading and caching `AppConfig` instances from the admin database.
//! Each application's configuration includes its JWT settings for token verification.
//!
//! # Pre-conditions
//! - The admin database must be initialized before querying configurations.
//! - App API keys used for lookup must be valid (non-empty strings).
//!
//! # Post-conditions
//! - Loaded configurations are cached until explicitly invalidated.
//! - Cache lookups are thread-safe and non-blocking for reads.
//!
//! # Invariants
//! - The cache always contains valid `AppConfig` instances.
//! - Cache entries are never partially constructed.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::auth::{AppConfig, JwtConfig};
use crate::storage::{Database, DatabaseError};
use crate::types::{AttributeId, EntityId, TripleValue};

/// Well-known attribute IDs for app configuration in the admin database.
mod attributes {
    use super::AttributeId;

    /// The JWT algorithm attribute (values: "HS256", "RS256").
    pub fn jwt_algorithm() -> AttributeId {
        AttributeId::from_string("jwt_algorithm")
    }

    /// The JWT secret for HS256 (stored as a UTF-8 string).
    pub fn jwt_secret() -> AttributeId {
        AttributeId::from_string("jwt_secret")
    }

    /// The JWT public key for RS256 (PEM-encoded string).
    pub fn jwt_public_key() -> AttributeId {
        AttributeId::from_string("jwt_public_key")
    }
}

/// Errors that can occur when loading app configuration.
#[derive(Debug)]
pub enum ConfigRegistryError {
    /// The database operation failed.
    Database(DatabaseError),
    /// The app was not found in the admin database.
    AppNotFound(String),
    /// The configuration data is invalid or malformed.
    InvalidConfig {
        /// The app API key with invalid config.
        app_api_key: String,
        /// Description of what is invalid.
        reason: String,
    },
}

impl std::fmt::Display for ConfigRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(e) => write!(f, "database error: {e}"),
            Self::AppNotFound(key) => write!(f, "app not found: {key}"),
            Self::InvalidConfig { app_api_key, reason } => {
                write!(f, "invalid config for app '{app_api_key}': {reason}")
            }
        }
    }
}

impl std::error::Error for ConfigRegistryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(e) => Some(e),
            Self::AppNotFound(_) | Self::InvalidConfig { .. } => None,
        }
    }
}

impl From<DatabaseError> for ConfigRegistryError {
    fn from(e: DatabaseError) -> Self {
        Self::Database(e)
    }
}

/// A registry for loading and caching application configurations.
///
/// The registry reads `AppConfig` from the admin database and caches them
/// in memory for fast repeated lookups. The cache can be invalidated
/// when configurations change.
///
/// # Thread Safety
///
/// The registry uses `RwLock` for its cache, allowing multiple concurrent
/// readers with exclusive write access for cache updates.
///
/// # Invariants
///
/// - Cache entries are always complete and valid `AppConfig` instances.
/// - The admin database reference remains valid for the registry's lifetime.
pub struct ConfigRegistry {
    /// Cache of loaded app configurations.
    cache: RwLock<HashMap<String, AppConfig>>,
}

impl ConfigRegistry {
    /// Create a new configuration registry.
    ///
    /// # Post-conditions
    /// - The registry is initialized with an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Get the app configuration for the given API key.
    ///
    /// First checks the cache; if not found, loads from the admin database
    /// and caches the result.
    ///
    /// # Pre-conditions
    /// - `app_api_key` must be a non-empty string.
    /// - The admin database must be readable.
    ///
    /// # Post-conditions
    /// - On success, the configuration is cached for future lookups.
    ///
    /// # Errors
    /// Returns `ConfigRegistryError::Database` if the database operation fails.
    /// Returns `ConfigRegistryError::AppNotFound` if no configuration exists for the key.
    /// Returns `ConfigRegistryError::InvalidConfig` if the stored data is malformed.
    pub fn get_config(
        &self,
        app_api_key: &str,
        admin_db: &RwLock<Database>,
    ) -> Result<AppConfig, ConfigRegistryError> {
        // Fast path: check cache with read lock
        {
            let cache = self
                .cache
                .read()
                .map_err(|_| ConfigRegistryError::InvalidConfig {
                    app_api_key: app_api_key.to_string(),
                    reason: "cache lock poisoned".to_string(),
                })?;
            if let Some(config) = cache.get(app_api_key) {
                return Ok(config.clone());
            }
        }

        // Slow path: load from database and update cache
        let config = self.load_from_database(app_api_key, admin_db)?;

        // Update cache with write lock
        {
            let mut cache = self
                .cache
                .write()
                .map_err(|_| ConfigRegistryError::InvalidConfig {
                    app_api_key: app_api_key.to_string(),
                    reason: "cache lock poisoned".to_string(),
                })?;
            cache.insert(app_api_key.to_string(), config.clone());
        }

        Ok(config)
    }

    /// Get the JWT configuration for the given API key.
    ///
    /// This is a convenience method that extracts just the JWT config
    /// from the full app configuration.
    ///
    /// # Pre-conditions
    /// - `app_api_key` must be a non-empty string.
    /// - The admin database must be readable.
    ///
    /// # Errors
    /// Returns an error if the app config cannot be loaded.
    /// Returns `Ok(None)` if the app has no JWT configuration.
    pub fn get_jwt_config(
        &self,
        app_api_key: &str,
        admin_db: &RwLock<Database>,
    ) -> Result<Option<JwtConfig>, ConfigRegistryError> {
        let config = self.get_config(app_api_key, admin_db)?;
        Ok(config.jwt_config)
    }

    /// Invalidate the cached configuration for the given API key.
    ///
    /// The next call to `get_config` will reload from the database.
    ///
    /// # Pre-conditions
    /// - `app_api_key` must be a non-empty string.
    ///
    /// # Post-conditions
    /// - The cache entry for this key is removed (if it existed).
    pub fn invalidate(&self, app_api_key: &str) {
        if let Ok(mut cache) = self.cache.write() {
            cache.remove(app_api_key);
        }
    }

    /// Invalidate all cached configurations.
    ///
    /// # Post-conditions
    /// - The cache is empty.
    pub fn invalidate_all(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    /// Load app configuration from the admin database.
    ///
    /// The configuration is stored as triples with:
    /// - Entity ID: the app's API key
    /// - Attributes: jwt_algorithm, jwt_secret, jwt_public_key
    ///
    /// # Pre-conditions
    /// - The admin database must be readable.
    ///
    /// # Errors
    /// Returns `ConfigRegistryError::Database` if the database operation fails.
    /// Returns `ConfigRegistryError::AppNotFound` if no triples exist for the entity.
    /// Returns `ConfigRegistryError::InvalidConfig` if the data is malformed.
    fn load_from_database(
        &self,
        app_api_key: &str,
        admin_db: &RwLock<Database>,
    ) -> Result<AppConfig, ConfigRegistryError> {
        let db = admin_db.read().map_err(|_| ConfigRegistryError::Database(
            DatabaseError::LockPoisoned,
        ))?;

        let snapshot = db.begin_readonly();
        let entity_id = EntityId::from_string(app_api_key);

        // Read all attributes for this app
        let records = snapshot.scan_entity(&entity_id)?;

        // Release the snapshot
        let txn_id = snapshot.close();
        db.release_snapshot(txn_id);

        // If no records exist, the app is not configured
        if records.is_empty() {
            return Err(ConfigRegistryError::AppNotFound(app_api_key.to_string()));
        }

        // Parse the configuration from triples
        let jwt_config = self.parse_jwt_config(app_api_key, &records)?;

        Ok(AppConfig {
            app_api_key: app_api_key.to_string(),
            jwt_config,
        })
    }

    /// Parse JWT configuration from triple records.
    ///
    /// # Returns
    /// - `Ok(Some(JwtConfig))` if valid JWT configuration is found.
    /// - `Ok(None)` if no JWT configuration is present.
    /// - `Err` if the configuration is invalid or incomplete.
    fn parse_jwt_config(
        &self,
        app_api_key: &str,
        records: &[crate::types::TripleRecord],
    ) -> Result<Option<JwtConfig>, ConfigRegistryError> {
        let algorithm_attr = attributes::jwt_algorithm();
        let secret_attr = attributes::jwt_secret();
        let public_key_attr = attributes::jwt_public_key();

        // Find the algorithm attribute
        let algorithm = records
            .iter()
            .find(|r| r.attribute_id == algorithm_attr)
            .and_then(|r| match &r.value {
                TripleValue::String(s) => Some(s.as_str()),
                _ => None,
            });

        // No algorithm means no JWT config (app uses API key auth only)
        let Some(algorithm) = algorithm else {
            return Ok(None);
        };

        match algorithm {
            "HS256" => {
                let secret = records
                    .iter()
                    .find(|r| r.attribute_id == secret_attr)
                    .and_then(|r| match &r.value {
                        TripleValue::String(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .ok_or_else(|| ConfigRegistryError::InvalidConfig {
                        app_api_key: app_api_key.to_string(),
                        reason: "HS256 algorithm requires jwt_secret".to_string(),
                    })?;

                if secret.is_empty() {
                    return Err(ConfigRegistryError::InvalidConfig {
                        app_api_key: app_api_key.to_string(),
                        reason: "jwt_secret must not be empty".to_string(),
                    });
                }

                // Store secret as UTF-8 bytes
                Ok(Some(JwtConfig::Hs256 {
                    secret: secret.as_bytes().to_vec(),
                }))
            }
            "RS256" => {
                let public_key = records
                    .iter()
                    .find(|r| r.attribute_id == public_key_attr)
                    .and_then(|r| match &r.value {
                        TripleValue::String(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .ok_or_else(|| ConfigRegistryError::InvalidConfig {
                        app_api_key: app_api_key.to_string(),
                        reason: "RS256 algorithm requires jwt_public_key".to_string(),
                    })?;

                let jwt_config = JwtConfig::new_rs256(public_key.to_string()).map_err(|e| {
                    ConfigRegistryError::InvalidConfig {
                        app_api_key: app_api_key.to_string(),
                        reason: e.to_string(),
                    }
                })?;

                Ok(Some(jwt_config))
            }
            other => Err(ConfigRegistryError::InvalidConfig {
                app_api_key: app_api_key.to_string(),
                reason: format!("unsupported jwt_algorithm: {other}"),
            }),
        }
    }
}

impl Default for ConfigRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_registry_new() {
        let registry = ConfigRegistry::new();
        // Cache should be empty
        let cache = registry.cache.read().expect("cache readable");
        assert!(cache.is_empty());
    }

    #[test]
    fn test_config_registry_default() {
        let registry = ConfigRegistry::default();
        let cache = registry.cache.read().expect("cache readable");
        assert!(cache.is_empty());
    }

    #[test]
    fn test_invalidate_removes_entry() {
        let registry = ConfigRegistry::new();

        // Manually insert a config into the cache
        {
            let mut cache = registry.cache.write().expect("cache writable");
            cache.insert(
                "test-app".to_string(),
                AppConfig {
                    app_api_key: "test-app".to_string(),
                    jwt_config: None,
                },
            );
        }

        // Verify it's in the cache
        {
            let cache = registry.cache.read().expect("cache readable");
            assert!(cache.contains_key("test-app"));
        }

        // Invalidate
        registry.invalidate("test-app");

        // Verify it's gone
        {
            let cache = registry.cache.read().expect("cache readable");
            assert!(!cache.contains_key("test-app"));
        }
    }

    #[test]
    fn test_invalidate_all_clears_cache() {
        let registry = ConfigRegistry::new();

        // Manually insert multiple configs
        {
            let mut cache = registry.cache.write().expect("cache writable");
            cache.insert(
                "app1".to_string(),
                AppConfig {
                    app_api_key: "app1".to_string(),
                    jwt_config: None,
                },
            );
            cache.insert(
                "app2".to_string(),
                AppConfig {
                    app_api_key: "app2".to_string(),
                    jwt_config: None,
                },
            );
        }

        // Verify they're in the cache
        {
            let cache = registry.cache.read().expect("cache readable");
            assert_eq!(cache.len(), 2);
        }

        // Invalidate all
        registry.invalidate_all();

        // Verify cache is empty
        {
            let cache = registry.cache.read().expect("cache readable");
            assert!(cache.is_empty());
        }
    }

    #[test]
    fn test_config_registry_error_display() {
        let db_err = ConfigRegistryError::Database(DatabaseError::LockPoisoned);
        assert!(db_err.to_string().contains("database error"));

        let not_found = ConfigRegistryError::AppNotFound("my-app".to_string());
        assert_eq!(not_found.to_string(), "app not found: my-app");

        let invalid = ConfigRegistryError::InvalidConfig {
            app_api_key: "bad-app".to_string(),
            reason: "missing secret".to_string(),
        };
        assert!(invalid.to_string().contains("invalid config"));
        assert!(invalid.to_string().contains("bad-app"));
        assert!(invalid.to_string().contains("missing secret"));
    }

    #[test]
    fn test_attributes() {
        // Verify attribute IDs are correctly created
        let alg = attributes::jwt_algorithm();
        let secret = attributes::jwt_secret();
        let pk = attributes::jwt_public_key();

        // They should all be different
        assert_ne!(alg, secret);
        assert_ne!(alg, pk);
        assert_ne!(secret, pk);
    }
}
