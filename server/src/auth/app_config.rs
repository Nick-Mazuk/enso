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
    fn test_app_config_with_hs256() {
        let config = AppConfig {
            app_api_key: "test-api-key".to_string(),
            jwt_config: Some(JwtConfig::Hs256 {
                secret: b"my-secret-key".to_vec(),
            }),
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
        let public_key = "-----BEGIN PUBLIC KEY-----\nMIIBIjAN...\n-----END PUBLIC KEY-----";
        let config = AppConfig {
            app_api_key: "test-api-key".to_string(),
            jwt_config: Some(JwtConfig::Rs256 {
                public_key: public_key.to_string(),
            }),
        };

        assert_eq!(config.app_api_key, "test-api-key");
        assert!(config.jwt_config.is_some());

        if let Some(JwtConfig::Rs256 {
            public_key: stored_key,
        }) = &config.jwt_config
        {
            assert_eq!(stored_key, public_key);
        } else {
            panic!("Expected Rs256 config");
        }
    }

    #[test]
    fn test_jwt_config_clone() {
        let hs256 = JwtConfig::Hs256 {
            secret: b"secret".to_vec(),
        };
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
        let original = AppConfig {
            app_api_key: "key".to_string(),
            jwt_config: Some(JwtConfig::Hs256 {
                secret: b"secret".to_vec(),
            }),
        };
        let cloned = original.clone();

        assert_eq!(original.app_api_key, cloned.app_api_key);
        assert!(cloned.jwt_config.is_some());
    }
}
