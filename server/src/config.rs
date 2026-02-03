//! Server configuration module.
//!
//! # Pre-conditions
//! - Environment variables must be valid UTF-8 if set.
//!
//! # Post-conditions
//! - `ServerConfig` contains valid configuration values.
//! - `listen_port` defaults to 3000 if not specified.
//! - `database_directory` defaults to "./data" if not specified.
//!
//! # Invariants
//! - `admin_app_api_key` is always a non-empty string.
//! - `database_directory` is a valid path.

use std::path::PathBuf;

/// Server configuration loaded from environment variables.
///
/// # Environment Variables
/// - `ENSO_ADMIN_APP_API_KEY`: Required. The API key for admin app access.
/// - `ENSO_DATABASE_DIRECTORY`: Optional. Path to the database directory. Defaults to "./data".
/// - `ENSO_LISTEN_PORT`: Optional. Port to listen on. Defaults to 3000.
#[derive(Debug)]
pub struct ServerConfig {
    /// API key for admin app access.
    pub admin_app_api_key: String,
    /// Directory where databases are stored.
    pub database_directory: PathBuf,
    /// Port the server listens on.
    pub listen_port: u16,
}

/// Error returned when configuration loading fails.
#[derive(Debug)]
pub enum ConfigError {
    /// A required environment variable is missing.
    MissingEnvVar(&'static str),
    /// An environment variable has an invalid value.
    InvalidValue {
        /// Name of the environment variable.
        name: &'static str,
        /// The invalid value that was provided.
        value: String,
        /// Description of why the value is invalid.
        reason: &'static str,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEnvVar(name) => {
                write!(f, "missing required environment variable: {name}")
            }
            Self::InvalidValue {
                name,
                value,
                reason,
            } => {
                write!(
                    f,
                    "invalid value for environment variable {name}='{value}': {reason}"
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl ServerConfig {
    /// Default port if `ENSO_LISTEN_PORT` is not set.
    const DEFAULT_PORT: u16 = 3000;
    /// Default database directory if `ENSO_DATABASE_DIRECTORY` is not set.
    const DEFAULT_DATABASE_DIRECTORY: &'static str = "./data";

    /// Load configuration from environment variables.
    ///
    /// # Errors
    /// Returns `ConfigError::MissingEnvVar` if `ENSO_ADMIN_APP_API_KEY` is not set.
    /// Returns `ConfigError::InvalidValue` if `ENSO_LISTEN_PORT` is not a valid u16.
    pub fn from_env() -> Result<Self, ConfigError> {
        let admin_app_api_key = std::env::var("ENSO_ADMIN_APP_API_KEY")
            .map_err(|_| ConfigError::MissingEnvVar("ENSO_ADMIN_APP_API_KEY"))?;

        if admin_app_api_key.is_empty() {
            return Err(ConfigError::InvalidValue {
                name: "ENSO_ADMIN_APP_API_KEY",
                value: String::new(),
                reason: "must not be empty",
            });
        }

        let database_directory = std::env::var("ENSO_DATABASE_DIRECTORY").map_or_else(
            |_| PathBuf::from(Self::DEFAULT_DATABASE_DIRECTORY),
            PathBuf::from,
        );

        let listen_port = match std::env::var("ENSO_LISTEN_PORT") {
            Ok(port_str) => port_str
                .parse::<u16>()
                .map_err(|_| ConfigError::InvalidValue {
                    name: "ENSO_LISTEN_PORT",
                    value: port_str,
                    reason: "must be a valid port number (0-65535)",
                })?,
            Err(_) => Self::DEFAULT_PORT,
        };

        Ok(Self {
            admin_app_api_key,
            database_directory,
            listen_port,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_config_error_display() {
        let missing = ConfigError::MissingEnvVar("TEST_VAR");
        assert_eq!(
            missing.to_string(),
            "missing required environment variable: TEST_VAR"
        );

        let invalid = ConfigError::InvalidValue {
            name: "TEST_VAR",
            value: "bad".to_string(),
            reason: "must be good",
        };
        assert_eq!(
            invalid.to_string(),
            "invalid value for environment variable TEST_VAR='bad': must be good"
        );
    }
}
