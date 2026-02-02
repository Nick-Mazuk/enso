//! Server configuration module.
//!
//! Configuration is loaded from environment variables with sensible defaults.
//!
//! # Environment Variables
//!
//! - `ENSO_ADMIN_APP_API_KEY`: API key for admin operations (required)
//! - `ENSO_DATABASE_DIRECTORY`: Directory for database files (default: `./data`)
//! - `ENSO_LISTEN_PORT`: Port to listen on (default: `3000`)

use std::path::PathBuf;

/// Server configuration.
///
/// # Invariants
///
/// - `admin_app_api_key` is non-empty
/// - `listen_port` is a valid port number (enforced by u16 type)
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// API key for admin operations.
    ///
    /// Used to authenticate requests that perform administrative actions.
    pub admin_app_api_key: String,

    /// Directory where database files are stored.
    ///
    /// Each app's database will be stored in a subdirectory based on its API key.
    pub database_directory: PathBuf,

    /// Port on which the server listens for connections.
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
        /// Description of what was expected.
        expected: &'static str,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEnvVar(name) => {
                write!(formatter, "missing required environment variable: {name}")
            }
            Self::InvalidValue {
                name,
                value,
                expected,
            } => {
                write!(
                    formatter,
                    "invalid value for {name}: '{value}' (expected {expected})"
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl ServerConfig {
    /// Environment variable name for admin API key.
    const ENV_ADMIN_APP_API_KEY: &'static str = "ENSO_ADMIN_APP_API_KEY";

    /// Environment variable name for database directory.
    const ENV_DATABASE_DIRECTORY: &'static str = "ENSO_DATABASE_DIRECTORY";

    /// Environment variable name for listen port.
    const ENV_LISTEN_PORT: &'static str = "ENSO_LISTEN_PORT";

    /// Default database directory.
    const DEFAULT_DATABASE_DIRECTORY: &'static str = "./data";

    /// Default listen port.
    const DEFAULT_LISTEN_PORT: u16 = 3000;

    /// Loads configuration from environment variables.
    ///
    /// # Pre-conditions
    ///
    /// - `ENSO_ADMIN_APP_API_KEY` environment variable must be set and non-empty.
    ///
    /// # Post-conditions
    ///
    /// - Returns a valid `ServerConfig` with all fields populated.
    /// - `admin_app_api_key` is non-empty.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if:
    /// - `ENSO_ADMIN_APP_API_KEY` is not set or is empty.
    /// - `ENSO_LISTEN_PORT` is set but not a valid u16.
    pub fn from_env() -> Result<Self, ConfigError> {
        let admin_app_api_key = Self::load_required_string(Self::ENV_ADMIN_APP_API_KEY)?;
        let database_directory = Self::load_optional_path(
            Self::ENV_DATABASE_DIRECTORY,
            Self::DEFAULT_DATABASE_DIRECTORY,
        );
        let listen_port = Self::load_optional_port(Self::ENV_LISTEN_PORT, Self::DEFAULT_LISTEN_PORT)?;

        let config = Self {
            admin_app_api_key,
            database_directory,
            listen_port,
        };

        // Post-condition: admin_app_api_key is non-empty
        assert!(!config.admin_app_api_key.is_empty());

        Ok(config)
    }

    /// Loads a required string from an environment variable.
    ///
    /// # Pre-conditions
    ///
    /// - `name` is a valid environment variable name.
    ///
    /// # Post-conditions
    ///
    /// - Returns a non-empty string on success.
    fn load_required_string(name: &'static str) -> Result<String, ConfigError> {
        match std::env::var(name) {
            Ok(value) if !value.is_empty() => Ok(value),
            Ok(_) => Err(ConfigError::MissingEnvVar(name)),
            Err(_) => Err(ConfigError::MissingEnvVar(name)),
        }
    }

    /// Loads an optional path from an environment variable with a default.
    ///
    /// # Pre-conditions
    ///
    /// - `name` is a valid environment variable name.
    /// - `default` is a valid path string.
    ///
    /// # Post-conditions
    ///
    /// - Returns the environment variable value if set and non-empty.
    /// - Returns the default value otherwise.
    fn load_optional_path(name: &'static str, default: &str) -> PathBuf {
        std::env::var(name)
            .ok()
            .filter(|value| !value.is_empty())
            .map_or_else(|| PathBuf::from(default), PathBuf::from)
    }

    /// Loads an optional port from an environment variable with a default.
    ///
    /// # Pre-conditions
    ///
    /// - `name` is a valid environment variable name.
    ///
    /// # Post-conditions
    ///
    /// - Returns the parsed port if the environment variable is set and valid.
    /// - Returns the default value if the environment variable is not set.
    fn load_optional_port(name: &'static str, default: u16) -> Result<u16, ConfigError> {
        match std::env::var(name) {
            Ok(value) if !value.is_empty() => value.parse().map_err(|_| ConfigError::InvalidValue {
                name,
                value,
                expected: "a valid port number (0-65535)",
            }),
            _ => Ok(default),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper to run a test with temporary environment variables.
    /// Restores original values after the test.
    fn with_env_vars<F, R>(vars: &[(&str, Option<&str>)], test: F) -> R
    where
        F: FnOnce() -> R,
    {
        // Save original values
        let originals: Vec<_> = vars
            .iter()
            .map(|(name, _)| (*name, std::env::var(name).ok()))
            .collect();

        // Set test values
        for (name, value) in vars {
            match value {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }

        let result = test();

        // Restore original values
        for (name, original) in originals {
            match original {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }

        result
    }

    #[test]
    fn test_from_env_with_all_values() {
        with_env_vars(
            &[
                ("ENSO_ADMIN_APP_API_KEY", Some("test-api-key")),
                ("ENSO_DATABASE_DIRECTORY", Some("/custom/path")),
                ("ENSO_LISTEN_PORT", Some("8080")),
            ],
            || {
                let config = ServerConfig::from_env().unwrap();
                assert_eq!(config.admin_app_api_key, "test-api-key");
                assert_eq!(config.database_directory, PathBuf::from("/custom/path"));
                assert_eq!(config.listen_port, 8080);
            },
        );
    }

    #[test]
    fn test_from_env_with_defaults() {
        with_env_vars(
            &[
                ("ENSO_ADMIN_APP_API_KEY", Some("test-api-key")),
                ("ENSO_DATABASE_DIRECTORY", None),
                ("ENSO_LISTEN_PORT", None),
            ],
            || {
                let config = ServerConfig::from_env().unwrap();
                assert_eq!(config.admin_app_api_key, "test-api-key");
                assert_eq!(config.database_directory, PathBuf::from("./data"));
                assert_eq!(config.listen_port, 3000);
            },
        );
    }

    #[test]
    fn test_missing_admin_api_key() {
        with_env_vars(&[("ENSO_ADMIN_APP_API_KEY", None)], || {
            let result = ServerConfig::from_env();
            assert!(result.is_err());
            let error = result.unwrap_err();
            assert!(matches!(error, ConfigError::MissingEnvVar("ENSO_ADMIN_APP_API_KEY")));
        });
    }

    #[test]
    fn test_empty_admin_api_key() {
        with_env_vars(&[("ENSO_ADMIN_APP_API_KEY", Some(""))], || {
            let result = ServerConfig::from_env();
            assert!(result.is_err());
            let error = result.unwrap_err();
            assert!(matches!(error, ConfigError::MissingEnvVar("ENSO_ADMIN_APP_API_KEY")));
        });
    }

    #[test]
    fn test_invalid_port() {
        with_env_vars(
            &[
                ("ENSO_ADMIN_APP_API_KEY", Some("test-api-key")),
                ("ENSO_LISTEN_PORT", Some("not-a-number")),
            ],
            || {
                let result = ServerConfig::from_env();
                assert!(result.is_err());
                let error = result.unwrap_err();
                assert!(matches!(
                    error,
                    ConfigError::InvalidValue {
                        name: "ENSO_LISTEN_PORT",
                        ..
                    }
                ));
            },
        );
    }

    #[test]
    fn test_port_out_of_range() {
        with_env_vars(
            &[
                ("ENSO_ADMIN_APP_API_KEY", Some("test-api-key")),
                ("ENSO_LISTEN_PORT", Some("99999")),
            ],
            || {
                let result = ServerConfig::from_env();
                assert!(result.is_err());
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
            name: "TEST_VAR",
            value: "bad".to_string(),
            expected: "a number",
        };
        assert_eq!(
            invalid.to_string(),
            "invalid value for TEST_VAR: 'bad' (expected a number)"
        );
    }
}
