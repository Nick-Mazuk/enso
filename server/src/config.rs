use std::path::PathBuf;

/// Server configuration.
///
/// # Pre-conditions
/// - None.
///
/// # Post-conditions
/// - Configuration is immutable after creation.
///
/// # Invariants
/// - `listen_port` is always a valid port number (0-65535).
/// - `database_directory` is a valid path.
#[derive(Debug)]
pub struct ServerConfig {
    /// API key for the admin application that stores configs for other apps.
    pub admin_app_api_key: String,
    /// Directory where databases are stored.
    pub database_directory: PathBuf,
    /// Port the server listens on.
    pub listen_port: u16,
}

impl ServerConfig {
    /// Loads server configuration from environment variables.
    ///
    /// # Pre-conditions
    /// - Environment variables `ENSO_ADMIN_APP_API_KEY`, `ENSO_DATABASE_DIRECTORY`,
    ///   and `ENSO_LISTEN_PORT` should be set for production use.
    ///
    /// # Post-conditions
    /// - Returns a fully initialized `ServerConfig`.
    /// - Uses default values for any missing environment variables.
    ///
    /// # Environment Variables
    /// - `ENSO_ADMIN_APP_API_KEY`: Admin app API key (default: "admin")
    /// - `ENSO_DATABASE_DIRECTORY`: Database directory path (default: "./data")
    /// - `ENSO_LISTEN_PORT`: Server listen port (default: 3000)
    #[must_use]
    pub fn from_env() -> Self {
        Self::from_env_reader(|key| std::env::var(key))
    }

    /// Loads server configuration using a custom environment reader function.
    ///
    /// # Pre-conditions
    /// - `env_reader` must return `Ok(value)` for set variables and `Err(_)` for unset ones.
    ///
    /// # Post-conditions
    /// - Returns a fully initialized `ServerConfig`.
    /// - Uses default values for any variables where `env_reader` returns Err.
    ///
    /// # Invariants
    /// - This function is pure with respect to the provided `env_reader`.
    #[must_use]
    pub fn from_env_reader<F, E>(env_reader: F) -> Self
    where
        F: Fn(&str) -> Result<String, E>,
    {
        let admin_app_api_key =
            env_reader("ENSO_ADMIN_APP_API_KEY").unwrap_or_else(|_| "admin".to_string());

        let database_directory = env_reader("ENSO_DATABASE_DIRECTORY")
            .map_or_else(|_| PathBuf::from("./data"), PathBuf::from);

        let listen_port = env_reader("ENSO_LISTEN_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3000);

        Self {
            admin_app_api_key,
            database_directory,
            listen_port,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_loads_from_environment_variables() {
        // Create a mock environment reader that returns our test values
        let mock_env_reader = |key: &str| -> Result<String, std::env::VarError> {
            match key {
                "ENSO_ADMIN_APP_API_KEY" => Ok("env-admin-key".to_string()),
                "ENSO_DATABASE_DIRECTORY" => Ok("/tmp/enso-test-db".to_string()),
                "ENSO_LISTEN_PORT" => Ok("8080".to_string()),
                _ => Err(std::env::VarError::NotPresent),
            }
        };

        let config = ServerConfig::from_env_reader(mock_env_reader);

        assert_eq!(config.admin_app_api_key, "env-admin-key");
        assert_eq!(config.database_directory, PathBuf::from("/tmp/enso-test-db"));
        assert_eq!(config.listen_port, 8080);
    }

    #[test]
    fn server_config_struct_exists_with_required_fields() {
        // Verify the ServerConfig struct can be constructed with all required fields
        let config = ServerConfig {
            admin_app_api_key: "test-admin-key".to_string(),
            database_directory: PathBuf::from("./data"),
            listen_port: 3000,
        };

        assert_eq!(config.admin_app_api_key, "test-admin-key");
        assert_eq!(config.database_directory, PathBuf::from("./data"));
        assert_eq!(config.listen_port, 3000);
    }

    #[test]
    fn server_config_is_debug() {
        let config = ServerConfig {
            admin_app_api_key: "debug-key".to_string(),
            database_directory: PathBuf::from("./db"),
            listen_port: 9000,
        };

        // Verify Debug is implemented by formatting
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("ServerConfig"));
        assert!(debug_str.contains("debug-key"));
    }
}
