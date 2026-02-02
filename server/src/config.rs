//! Server configuration module.
//!
//! This module provides configuration loading for the Enso server from
//! environment variables.
//!
//! # Environment Variables
//!
//! - `ENSO_ADMIN_APP_API_KEY`: API key for admin operations (required for admin endpoints)
//! - `ENSO_DATABASE_DIRECTORY`: Directory where database files are stored (default: `./data`)
//! - `ENSO_LISTEN_PORT`: Port to listen on (default: `3000`)
//!
//! # Invariants
//!
//! - `database_directory` is always a valid path (may not exist yet)
//! - `listen_port` is always a valid port number (1-65535)
//! - `admin_app_api_key` follows the same validation rules as regular API keys

use std::path::PathBuf;

/// Server configuration.
///
/// Contains all configuration parameters needed to run the Enso server.
///
/// # Pre-conditions
///
/// When constructed via `from_env()`:
/// - All required environment variables must be set
/// - All values must be valid for their respective types
///
/// # Post-conditions
///
/// - `listen_port` is always in the valid range (1-65535)
/// - `database_directory` is a valid path
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// API key for admin operations.
    /// Used to authenticate requests to admin endpoints.
    pub admin_app_api_key: String,
    /// Directory where database files are stored.
    /// Each app's database will be at `{database_directory}/{app_api_key}.db`.
    pub database_directory: PathBuf,
    /// Port to listen on for WebSocket connections.
    pub listen_port: u16,
}

/// Error returned when loading configuration fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// An environment variable is missing.
    MissingEnvVar(String),
    /// An environment variable has an invalid value.
    InvalidValue { name: String, message: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEnvVar(name) => {
                write!(f, "missing required environment variable: {name}")
            }
            Self::InvalidValue { name, message } => {
                write!(f, "invalid value for {name}: {message}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl ServerConfig {
    /// Default port for the server.
    pub const DEFAULT_PORT: u16 = 3000;
    /// Default database directory.
    pub const DEFAULT_DATABASE_DIRECTORY: &'static str = "./data";

    /// Load configuration from environment variables.
    ///
    /// # Environment Variables
    ///
    /// - `ENSO_ADMIN_APP_API_KEY`: Admin API key (required)
    /// - `ENSO_DATABASE_DIRECTORY`: Database directory (default: `./data`)
    /// - `ENSO_LISTEN_PORT`: Listen port (default: `3000`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `ENSO_ADMIN_APP_API_KEY` is not set or is empty
    /// - `ENSO_LISTEN_PORT` is set but not a valid port number
    pub fn from_env() -> Result<Self, ConfigError> {
        let admin_app_api_key = Self::load_admin_api_key()?;
        let database_directory = Self::load_database_directory();
        let listen_port = Self::load_listen_port()?;

        Ok(Self {
            admin_app_api_key,
            database_directory,
            listen_port,
        })
    }

    /// Load the admin API key from environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment variable is not set or is empty.
    fn load_admin_api_key() -> Result<String, ConfigError> {
        let key =
            std::env::var("ENSO_ADMIN_APP_API_KEY").map_err(|_| ConfigError::MissingEnvVar("ENSO_ADMIN_APP_API_KEY".to_string()))?;

        if key.is_empty() {
            return Err(ConfigError::InvalidValue {
                name: "ENSO_ADMIN_APP_API_KEY".to_string(),
                message: "must not be empty".to_string(),
            });
        }

        Ok(key)
    }

    /// Load the database directory from environment.
    ///
    /// Returns the default if not set.
    fn load_database_directory() -> PathBuf {
        std::env::var("ENSO_DATABASE_DIRECTORY")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(Self::DEFAULT_DATABASE_DIRECTORY))
    }

    /// Load the listen port from environment.
    ///
    /// Returns the default if not set.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is set but not a valid port number.
    fn load_listen_port() -> Result<u16, ConfigError> {
        match std::env::var("ENSO_LISTEN_PORT") {
            Ok(value) => value.parse::<u16>().map_err(|_| ConfigError::InvalidValue {
                name: "ENSO_LISTEN_PORT".to_string(),
                message: format!("'{value}' is not a valid port number (must be 1-65535)"),
            }),
            Err(_) => Ok(Self::DEFAULT_PORT),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        assert_eq!(ServerConfig::DEFAULT_PORT, 3000);
        assert_eq!(ServerConfig::DEFAULT_DATABASE_DIRECTORY, "./data");
    }

    #[test]
    fn test_config_error_display_missing() {
        let error = ConfigError::MissingEnvVar("TEST_VAR".to_string());
        assert_eq!(
            error.to_string(),
            "missing required environment variable: TEST_VAR"
        );
    }

    #[test]
    fn test_config_error_display_invalid() {
        let error = ConfigError::InvalidValue {
            name: "TEST_VAR".to_string(),
            message: "bad value".to_string(),
        };
        assert_eq!(error.to_string(), "invalid value for TEST_VAR: bad value");
    }
}
