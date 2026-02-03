//! Application configuration for authentication.
//!
//! # Pre-conditions
//! - `app_api_key` must be a valid, non-empty string.
//! - If `jwt_config` is `Some`, it must contain valid cryptographic material.
//!
//! # Post-conditions
//! - `AppConfig` instances are immutable once created.
//!
//! # Invariants
//! - `app_api_key` is never empty.
//! - `JwtConfig::Hs256` secrets must not be empty.
//! - `JwtConfig::Rs256` public keys must be valid PEM-encoded RSA public keys.

use jsonwebtoken::DecodingKey;

/// Error returned when JWT configuration is invalid.
#[derive(Debug)]
pub enum JwtConfigError {
    /// The HS256 secret is empty.
    EmptySecret,
    /// The RS256 public key is not a valid PEM-encoded RSA public key.
    InvalidRs256PublicKey(String),
}

impl std::fmt::Display for JwtConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptySecret => write!(f, "HS256 secret must not be empty"),
            Self::InvalidRs256PublicKey(reason) => {
                write!(f, "invalid RS256 public key: {reason}")
            }
        }
    }
}

impl std::error::Error for JwtConfigError {}

/// JWT signing/verification configuration.
///
/// Supports both symmetric (HS256) and asymmetric (RS256) algorithms.
#[derive(Debug, Clone)]
pub enum JwtConfig {
    /// HMAC-SHA256 symmetric signing.
    ///
    /// Uses a shared secret for both signing and verification.
    Hs256 {
        /// The shared secret used for HMAC-SHA256.
        secret: Vec<u8>,
    },
    /// RSA-SHA256 asymmetric signing.
    ///
    /// Uses an RSA public key for verification only.
    Rs256 {
        /// PEM-encoded RSA public key.
        public_key: String,
    },
}

impl JwtConfig {
    /// Create a new HS256 JWT configuration.
    ///
    /// # Pre-conditions
    /// - `secret` must not be empty.
    ///
    /// # Post-conditions
    /// - Returns a valid `JwtConfig::Hs256` variant.
    ///
    /// # Errors
    /// Returns `JwtConfigError::EmptySecret` if the secret is empty.
    pub fn new_hs256(secret: Vec<u8>) -> Result<Self, JwtConfigError> {
        if secret.is_empty() {
            return Err(JwtConfigError::EmptySecret);
        }
        Ok(Self::Hs256 { secret })
    }

    /// Create a new RS256 JWT configuration.
    ///
    /// # Pre-conditions
    /// - `public_key` must be a valid PEM-encoded RSA public key.
    ///
    /// # Post-conditions
    /// - Returns a valid `JwtConfig::Rs256` variant.
    ///
    /// # Errors
    /// Returns `JwtConfigError::InvalidRs256PublicKey` if the key is not a valid RS256 PEM key.
    pub fn new_rs256(public_key: String) -> Result<Self, JwtConfigError> {
        // Validate that the public key can be parsed as an RSA public key.
        // DecodingKey::from_rsa_pem validates the PEM format and RSA structure.
        DecodingKey::from_rsa_pem(public_key.as_bytes())
            .map_err(|e| JwtConfigError::InvalidRs256PublicKey(e.to_string()))?;

        Ok(Self::Rs256 { public_key })
    }
}

/// Configuration for an application's authentication settings.
///
/// Each application has an API key and optionally supports JWT authentication.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// API key used to authenticate requests from this application.
    pub app_api_key: String,
    /// Optional JWT configuration for token-based authentication.
    pub jwt_config: Option<JwtConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Valid RSA-2048 public key for testing (generated for test purposes only)
    const VALID_RS256_PUBLIC_KEY: &str = r"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0Z3VS5JJcds3xfn/ygWyF8PbnGy0AHB7
Wazz7gZvaP/H7F1V3dJQKl2qFwXjT3b9rj8veqAqgLpKmqxPXrV7leXMIRyYDj9nvN2w5hxPzF9K
jXVZ2B2ZmEx2/bKiPcj+lnL9aZDl/7TPxDHpvnVp3Q8xZdLGZ9lP6sXcVl6d8X+6J8wHHJ5KnN2F
0Vrp/bJD5fMv8KcdB4yg9gx4mTsLMZVqGhOKLVf3s3hsjhJKKlC0wcDhO0yW6Dv5VsOiVy5e3Q+8
M7L5R7bOJzHlLcB7h7cfksFMRv/bH2qGy7V7ra91c8K6vH8s5b8VRn/0c8mKewfYqfRqnxL7d2M3
a3aCXj5dawIDAQAB
-----END PUBLIC KEY-----";

    #[test]
    fn test_app_config_creation() {
        let config = AppConfig {
            app_api_key: "test-api-key".to_string(),
            jwt_config: None,
        };

        assert_eq!(config.app_api_key, "test-api-key");
        assert!(config.jwt_config.is_none());
    }

    #[test]
    fn test_new_hs256_valid() {
        let result = JwtConfig::new_hs256(b"my-secret-key".to_vec());
        assert!(result.is_ok());

        if let Ok(JwtConfig::Hs256 { secret }) = result {
            assert_eq!(secret, b"my-secret-key");
        } else {
            panic!("Expected Hs256 config");
        }
    }

    #[test]
    fn test_new_hs256_empty_secret() {
        let result = JwtConfig::new_hs256(Vec::new());
        assert!(result.is_err());
        assert!(matches!(result, Err(JwtConfigError::EmptySecret)));
    }

    #[test]
    fn test_new_rs256_valid() {
        let result = JwtConfig::new_rs256(VALID_RS256_PUBLIC_KEY.to_string());
        assert!(result.is_ok());

        if let Ok(JwtConfig::Rs256 { public_key }) = result {
            assert_eq!(public_key, VALID_RS256_PUBLIC_KEY);
        } else {
            panic!("Expected Rs256 config");
        }
    }

    #[test]
    fn test_new_rs256_invalid_pem() {
        let result = JwtConfig::new_rs256("not a valid pem key".to_string());
        assert!(result.is_err());
        assert!(matches!(result, Err(JwtConfigError::InvalidRs256PublicKey(_))));
    }

    #[test]
    fn test_new_rs256_truncated_key() {
        let result = JwtConfig::new_rs256(
            "-----BEGIN PUBLIC KEY-----\nMIIBIjAN...\n-----END PUBLIC KEY-----".to_string(),
        );
        assert!(result.is_err());
        assert!(matches!(result, Err(JwtConfigError::InvalidRs256PublicKey(_))));
    }

    #[test]
    fn test_new_rs256_empty_key() {
        let result = JwtConfig::new_rs256(String::new());
        assert!(result.is_err());
        assert!(matches!(result, Err(JwtConfigError::InvalidRs256PublicKey(_))));
    }

    #[test]
    fn test_app_config_with_hs256() {
        let jwt_config = JwtConfig::new_hs256(b"my-secret-key".to_vec()).expect("valid secret");
        let config = AppConfig {
            app_api_key: "test-api-key".to_string(),
            jwt_config: Some(jwt_config),
        };

        assert_eq!(config.app_api_key, "test-api-key");
        assert!(config.jwt_config.is_some());

        if let Some(JwtConfig::Hs256 { secret }) = &config.jwt_config {
            assert_eq!(secret, b"my-secret-key");
        } else {
            panic!("Expected Hs256 config");
        }
    }

    #[test]
    fn test_app_config_with_rs256() {
        let jwt_config =
            JwtConfig::new_rs256(VALID_RS256_PUBLIC_KEY.to_string()).expect("valid key");
        let config = AppConfig {
            app_api_key: "test-api-key".to_string(),
            jwt_config: Some(jwt_config),
        };

        assert_eq!(config.app_api_key, "test-api-key");
        assert!(config.jwt_config.is_some());

        if let Some(JwtConfig::Rs256 { public_key }) = &config.jwt_config {
            assert_eq!(public_key, VALID_RS256_PUBLIC_KEY);
        } else {
            panic!("Expected Rs256 config");
        }
    }

    #[test]
    fn test_new_rs256_validates_pem_key() {
        let invalid_key = "not-a-valid-pem-key";
        let result = JwtConfig::new_rs256(invalid_key.to_string());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, JwtConfigError::InvalidRs256PublicKey(_)));
    }

    #[test]
    fn test_new_rs256_rejects_truncated_pem() {
        let truncated_key = "-----BEGIN PUBLIC KEY-----\nMIIBIjAN...\n-----END PUBLIC KEY-----";
        let result = JwtConfig::new_rs256(truncated_key.to_string());

        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_config_clone() {
        let hs256 = JwtConfig::new_hs256(b"secret".to_vec()).expect("valid secret");
        let cloned = hs256.clone();

        if let (JwtConfig::Hs256 { secret: s1 }, JwtConfig::Hs256 { secret: s2 }) = (&hs256, &cloned)
        {
            assert_eq!(s1, s2);
        } else {
            panic!("Clone should preserve variant");
        }
    }

    #[test]
    fn test_app_config_clone() {
        let jwt_config = JwtConfig::new_hs256(b"secret".to_vec()).expect("valid secret");
        let original = AppConfig {
            app_api_key: "key".to_string(),
            jwt_config: Some(jwt_config),
        };
        let cloned = original.clone();

        assert_eq!(original.app_api_key, cloned.app_api_key);
        assert!(cloned.jwt_config.is_some());
    }

    #[test]
    fn test_jwt_config_error_display() {
        let empty_secret = JwtConfigError::EmptySecret;
        assert_eq!(empty_secret.to_string(), "HS256 secret must not be empty");

        let invalid_key = JwtConfigError::InvalidRs256PublicKey("bad format".to_string());
        assert_eq!(
            invalid_key.to_string(),
            "invalid RS256 public key: bad format"
        );
    }
}
