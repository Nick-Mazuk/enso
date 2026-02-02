use std::path::PathBuf;

/// Server configuration.
///
/// # Invariants
/// - `listen_port` must be a valid port number (1-65535 enforced by u16).
/// - `database_directory` should be a valid path (existence checked at runtime).
/// - `admin_app_api_key` should be non-empty when admin functionality is required.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// API key for admin application access.
    pub admin_app_api_key: String,
    /// Directory where databases are stored.
    pub database_directory: PathBuf,
    /// Port the server listens on.
    pub listen_port: u16,
}

/// Error type for configuration loading failures.
#[derive(Debug)]
pub enum ConfigError {
    /// A required environment variable is missing.
    MissingEnvVar(String),
    /// An environment variable has an invalid value.
    InvalidValue { key: String, message: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEnvVar(key) => {
                write!(formatter, "missing required environment variable: {key}")
            }
            Self::InvalidValue { key, message } => {
                write!(
                    formatter,
                    "invalid value for environment variable {key}: {message}"
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl ServerConfig {
    /// Default port for the server.
    pub const DEFAULT_PORT: u16 = 3000;
    /// Default database directory.
    pub const DEFAULT_DATABASE_DIRECTORY: &str = "./data";

    /// Load configuration from environment variables.
    ///
    /// # Environment Variables
    /// - `ENSO_ADMIN_APP_API_KEY`: Required. API key for admin access.
    /// - `ENSO_DATABASE_DIRECTORY`: Optional. Defaults to "./data".
    /// - `ENSO_LISTEN_PORT`: Optional. Defaults to 3000.
    ///
    /// # Errors
    /// Returns `ConfigError` if:
    /// - `ENSO_ADMIN_APP_API_KEY` is not set.
    /// - `ENSO_LISTEN_PORT` is set but not a valid u16.
    ///
    /// # Postconditions
    /// - Returns a valid `ServerConfig` with all fields populated.
    pub fn from_env() -> Result<Self, ConfigError> {
        let admin_app_api_key = std::env::var("ENSO_ADMIN_APP_API_KEY")
            .map_err(|_| ConfigError::MissingEnvVar("ENSO_ADMIN_APP_API_KEY".to_string()))?;

        let database_directory = std::env::var("ENSO_DATABASE_DIRECTORY")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(Self::DEFAULT_DATABASE_DIRECTORY));

        let listen_port = match std::env::var("ENSO_LISTEN_PORT") {
            Ok(port_str) => port_str.parse::<u16>().map_err(|_| ConfigError::InvalidValue {
                key: "ENSO_LISTEN_PORT".to_string(),
                message: format!("'{port_str}' is not a valid port number"),
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
    fn test_from_env_missing_admin_key() {
        // Clear any existing env vars
        std::env::remove_var("ENSO_ADMIN_APP_API_KEY");
        std::env::remove_var("ENSO_DATABASE_DIRECTORY");
        std::env::remove_var("ENSO_LISTEN_PORT");

        let result = ServerConfig::from_env();
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, ConfigError::MissingEnvVar(key) if key == "ENSO_ADMIN_APP_API_KEY"));
    }

    #[test]
    fn test_from_env_with_defaults() {
        std::env::set_var("ENSO_ADMIN_APP_API_KEY", "test-key");
        std::env::remove_var("ENSO_DATABASE_DIRECTORY");
        std::env::remove_var("ENSO_LISTEN_PORT");

        let config = ServerConfig::from_env().unwrap();
        assert_eq!(config.admin_app_api_key, "test-key");
        assert_eq!(config.database_directory, PathBuf::from("./data"));
        assert_eq!(config.listen_port, 3000);

        std::env::remove_var("ENSO_ADMIN_APP_API_KEY");
    }

    #[test]
    fn test_from_env_with_all_values() {
        std::env::set_var("ENSO_ADMIN_APP_API_KEY", "my-admin-key");
        std::env::set_var("ENSO_DATABASE_DIRECTORY", "/custom/path");
        std::env::set_var("ENSO_LISTEN_PORT", "8080");

        let config = ServerConfig::from_env().unwrap();
        assert_eq!(config.admin_app_api_key, "my-admin-key");
        assert_eq!(config.database_directory, PathBuf::from("/custom/path"));
        assert_eq!(config.listen_port, 8080);

        std::env::remove_var("ENSO_ADMIN_APP_API_KEY");
        std::env::remove_var("ENSO_DATABASE_DIRECTORY");
        std::env::remove_var("ENSO_LISTEN_PORT");
    }

    #[test]
    fn test_from_env_invalid_port() {
        std::env::set_var("ENSO_ADMIN_APP_API_KEY", "test-key");
        std::env::set_var("ENSO_LISTEN_PORT", "not-a-number");

        let result = ServerConfig::from_env();
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, ConfigError::InvalidValue { key, .. } if key == "ENSO_LISTEN_PORT"));

        std::env::remove_var("ENSO_ADMIN_APP_API_KEY");
        std::env::remove_var("ENSO_LISTEN_PORT");
    }

    #[test]
    fn test_from_env_port_out_of_range() {
        std::env::set_var("ENSO_ADMIN_APP_API_KEY", "test-key");
        std::env::set_var("ENSO_LISTEN_PORT", "99999");

        let result = ServerConfig::from_env();
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, ConfigError::InvalidValue { key, .. } if key == "ENSO_LISTEN_PORT"));

        std::env::remove_var("ENSO_ADMIN_APP_API_KEY");
        std::env::remove_var("ENSO_LISTEN_PORT");
    }

    #[test]
    fn test_config_error_display() {
        let missing = ConfigError::MissingEnvVar("TEST_VAR".to_string());
        assert_eq!(
            missing.to_string(),
            "missing required environment variable: TEST_VAR"
        );

        let invalid = ConfigError::InvalidValue {
            key: "TEST_VAR".to_string(),
            message: "bad value".to_string(),
        };
        assert_eq!(
            invalid.to_string(),
            "invalid value for environment variable TEST_VAR: bad value"
        );
    }
}
