//! Server configuration module.
//!
//! Loads configuration from environment variables with sensible defaults.
//! No external configuration files are needed - all settings come from the environment.
//!
//! # Environment Variables
//!
//! - `ENSO_ADMIN_APP_API_KEY`: The admin API key for privileged operations (required)
//! - `ENSO_DATABASE_DIRECTORY`: Directory where database files are stored (default: `./data`)
//! - `ENSO_LISTEN_PORT`: Port to listen on (default: `3000`)
//!
//! # Invariants
//!
//! - `admin_app_api_key` is always a valid, non-empty string
//! - `database_directory` is a valid `PathBuf`
//! - `listen_port` is a valid port number (1-65535)

use std::path::PathBuf;

/// Server configuration loaded from environment variables.
///
/// # Pre-conditions
///
/// All configuration values have been validated during construction via `load()`.
///
/// # Post-conditions
///
/// Once constructed, all fields contain valid, usable values.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Admin API key for privileged operations.
    /// This key grants elevated access and should be kept secure.
    pub admin_app_api_key: String,

    /// Directory where database files are stored.
    /// Each app's database will be at `{database_directory}/{app_api_key}.db`.
    pub database_directory: PathBuf,

    /// Port to listen on for incoming connections.
    pub listen_port: u16,
}

/// Error type for configuration loading failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// A required environment variable is missing.
    MissingEnvVar(&'static str),

    /// An environment variable has an invalid value.
    InvalidValue {
        /// Name of the environment variable.
        var_name: &'static str,
        /// Description of why the value is invalid.
        reason: String,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEnvVar(var) => {
                write!(f, "missing required environment variable: {var}")
            }
            Self::InvalidValue { var_name, reason } => {
                write!(f, "invalid value for {var_name}: {reason}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Environment variable names.
const ENV_ADMIN_APP_API_KEY: &str = "ENSO_ADMIN_APP_API_KEY";
const ENV_DATABASE_DIRECTORY: &str = "ENSO_DATABASE_DIRECTORY";
const ENV_LISTEN_PORT: &str = "ENSO_LISTEN_PORT";

/// Default values.
const DEFAULT_DATABASE_DIRECTORY: &str = "./data";
const DEFAULT_LISTEN_PORT: u16 = 3000;

impl ServerConfig {
    /// Load configuration from environment variables.
    ///
    /// # Environment Variables
    ///
    /// - `ENSO_ADMIN_APP_API_KEY`: Required. The admin API key.
    /// - `ENSO_DATABASE_DIRECTORY`: Optional. Defaults to `./data`.
    /// - `ENSO_LISTEN_PORT`: Optional. Defaults to `3000`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `ENSO_ADMIN_APP_API_KEY` is not set or is empty
    /// - `ENSO_LISTEN_PORT` is set but cannot be parsed as a valid port number
    pub fn load() -> Result<Self, ConfigError> {
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
    /// Returns an error if the environment variable is missing or empty.
    fn load_admin_api_key() -> Result<String, ConfigError> {
        let key = std::env::var(ENV_ADMIN_APP_API_KEY)
            .map_err(|_| ConfigError::MissingEnvVar(ENV_ADMIN_APP_API_KEY))?;

        if key.is_empty() {
            return Err(ConfigError::InvalidValue {
                var_name: ENV_ADMIN_APP_API_KEY,
                reason: "must not be empty".to_string(),
            });
        }

        Ok(key)
    }

    /// Load the database directory from environment, using default if not set.
    fn load_database_directory() -> PathBuf {
        std::env::var(ENV_DATABASE_DIRECTORY)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_DATABASE_DIRECTORY))
    }

    /// Load the listen port from environment, using default if not set.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment variable is set but cannot be parsed as u16.
    fn load_listen_port() -> Result<u16, ConfigError> {
        match std::env::var(ENV_LISTEN_PORT) {
            Ok(port_str) => port_str.parse().map_err(|_| ConfigError::InvalidValue {
                var_name: ENV_LISTEN_PORT,
                reason: format!("'{port_str}' is not a valid port number (1-65535)"),
            }),
            Err(_) => Ok(DEFAULT_LISTEN_PORT),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    /// Helper to run a test with specific environment variables set.
    /// Restores original environment after the test.
    fn with_env<F, R>(vars: &[(&str, Option<&str>)], test_fn: F) -> R
    where
        F: FnOnce() -> R,
    {
        // Save original values
        let originals: Vec<_> = vars
            .iter()
            .map(|(key, _)| (*key, env::var(key).ok()))
            .collect();

        // Set test values
        for (key, value) in vars {
            match value {
                Some(v) => env::set_var(key, v),
                None => env::remove_var(key),
            }
        }

        let result = test_fn();

        // Restore original values
        for (key, original) in originals {
            match original {
                Some(v) => env::set_var(key, v),
                None => env::remove_var(key),
            }
        }

        result
    }

    #[test]
    fn test_load_with_all_env_vars() {
        with_env(
            &[
                (ENV_ADMIN_APP_API_KEY, Some("test-admin-key")),
                (ENV_DATABASE_DIRECTORY, Some("/custom/data")),
                (ENV_LISTEN_PORT, Some("8080")),
            ],
            || {
                let config = ServerConfig::load().unwrap();

                assert_eq!(config.admin_app_api_key, "test-admin-key");
                assert_eq!(config.database_directory, PathBuf::from("/custom/data"));
                assert_eq!(config.listen_port, 8080);
            },
        );
    }

    #[test]
    fn test_load_with_defaults() {
        with_env(
            &[
                (ENV_ADMIN_APP_API_KEY, Some("test-key")),
                (ENV_DATABASE_DIRECTORY, None),
                (ENV_LISTEN_PORT, None),
            ],
            || {
                let config = ServerConfig::load().unwrap();

                assert_eq!(config.admin_app_api_key, "test-key");
                assert_eq!(config.database_directory, PathBuf::from("./data"));
                assert_eq!(config.listen_port, 3000);
            },
        );
    }

    #[test]
    fn test_load_missing_admin_key() {
        with_env(
            &[
                (ENV_ADMIN_APP_API_KEY, None),
                (ENV_DATABASE_DIRECTORY, None),
                (ENV_LISTEN_PORT, None),
            ],
            || {
                let result = ServerConfig::load();

                assert_eq!(
                    result.err(),
                    Some(ConfigError::MissingEnvVar(ENV_ADMIN_APP_API_KEY))
                );
            },
        );
    }

    #[test]
    fn test_load_empty_admin_key() {
        with_env(
            &[
                (ENV_ADMIN_APP_API_KEY, Some("")),
                (ENV_DATABASE_DIRECTORY, None),
                (ENV_LISTEN_PORT, None),
            ],
            || {
                let result = ServerConfig::load();

                assert!(matches!(
                    result.err(),
                    Some(ConfigError::InvalidValue { var_name: ENV_ADMIN_APP_API_KEY, .. })
                ));
            },
        );
    }

    #[test]
    fn test_load_invalid_port() {
        with_env(
            &[
                (ENV_ADMIN_APP_API_KEY, Some("test-key")),
                (ENV_DATABASE_DIRECTORY, None),
                (ENV_LISTEN_PORT, Some("not-a-number")),
            ],
            || {
                let result = ServerConfig::load();

                assert!(matches!(
                    result.err(),
                    Some(ConfigError::InvalidValue { var_name: ENV_LISTEN_PORT, .. })
                ));
            },
        );
    }

    #[test]
    fn test_load_port_out_of_range() {
        with_env(
            &[
                (ENV_ADMIN_APP_API_KEY, Some("test-key")),
                (ENV_DATABASE_DIRECTORY, None),
                (ENV_LISTEN_PORT, Some("99999")),
            ],
            || {
                let result = ServerConfig::load();

                assert!(matches!(
                    result.err(),
                    Some(ConfigError::InvalidValue { var_name: ENV_LISTEN_PORT, .. })
                ));
            },
        );
    }

    #[test]
    fn test_config_error_display() {
        let missing = ConfigError::MissingEnvVar("TEST_VAR");
        assert_eq!(
            missing.to_string(),
            "missing required environment variable: TEST_VAR"
        );

        let invalid = ConfigError::InvalidValue {
            var_name: "TEST_VAR",
            reason: "must be positive".to_string(),
        };
        assert_eq!(
            invalid.to_string(),
            "invalid value for TEST_VAR: must be positive"
        );
    }
}
