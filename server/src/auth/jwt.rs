//! JWT verification module.
//!
//! Provides functions to verify JSON Web Tokens using HS256 and RS256 algorithms.
//!
//! # Pre-conditions
//! - For HS256: The secret must be non-empty.
//! - For RS256: The public key must be a valid PEM-encoded RSA public key.
//! - The JWT must be a valid, properly formatted token.
//!
//! # Post-conditions
//! - On success, returns the user ID extracted from the 'sub' claim.
//! - On failure, returns a descriptive error indicating what went wrong.
//!
//! # Invariants
//! - Verification is stateless and does not modify any external state.
//! - The same inputs always produce the same outputs.

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};

use super::JwtConfig;

/// Claims extracted from a JWT.
///
/// The 'sub' (subject) claim is required and contains the user identifier.
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    /// Subject claim containing the user identifier.
    sub: String,
}

/// Error returned when JWT verification fails.
#[derive(Debug)]
pub enum JwtError {
    /// The JWT signature is invalid.
    InvalidSignature,
    /// The JWT has expired.
    TokenExpired,
    /// The JWT is malformed or cannot be parsed.
    MalformedToken,
    /// The 'sub' claim is missing from the JWT.
    MissingSubClaim,
    /// The decoding key could not be created from the provided configuration.
    InvalidKey(String),
}

impl std::fmt::Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSignature => write!(f, "invalid JWT signature"),
            Self::TokenExpired => write!(f, "JWT has expired"),
            Self::MalformedToken => write!(f, "malformed JWT"),
            Self::MissingSubClaim => write!(f, "missing 'sub' claim in JWT"),
            Self::InvalidKey(reason) => write!(f, "invalid key: {reason}"),
        }
    }
}

impl std::error::Error for JwtError {}

/// Verifies a JWT and extracts the user ID from the 'sub' claim.
///
/// # Arguments
/// * `token` - The JWT string to verify.
/// * `config` - The JWT configuration containing the key material.
///
/// # Returns
/// The user ID from the 'sub' claim on success.
///
/// # Errors
/// Returns `JwtError` if verification fails for any reason.
pub fn verify_token(token: &str, config: &JwtConfig) -> Result<String, JwtError> {
    match config {
        JwtConfig::Hs256 { secret } => verify_hs256(token, secret),
        JwtConfig::Rs256 { public_key } => verify_rs256(token, public_key),
    }
}

/// Verifies a JWT using the HS256 algorithm.
///
/// # Arguments
/// * `token` - The JWT string to verify.
/// * `secret` - The shared secret for HMAC-SHA256 verification.
///
/// # Returns
/// The user ID from the 'sub' claim on success.
///
/// # Errors
/// Returns `JwtError` if verification fails.
fn verify_hs256(token: &str, secret: &[u8]) -> Result<String, JwtError> {
    if secret.is_empty() {
        return Err(JwtError::InvalidKey("secret must be non-empty".to_string()));
    }

    let key = DecodingKey::from_secret(secret);
    let validation = Validation::new(Algorithm::HS256);

    decode_and_extract_sub(token, &key, &validation)
}

/// Verifies a JWT using the RS256 algorithm.
///
/// # Arguments
/// * `token` - The JWT string to verify.
/// * `public_key` - The PEM-encoded RSA public key for verification.
///
/// # Returns
/// The user ID from the 'sub' claim on success.
///
/// # Errors
/// Returns `JwtError` if verification fails or the key is invalid.
fn verify_rs256(token: &str, public_key: &str) -> Result<String, JwtError> {
    let key = DecodingKey::from_rsa_pem(public_key.as_bytes())
        .map_err(|e| JwtError::InvalidKey(e.to_string()))?;
    let validation = Validation::new(Algorithm::RS256);

    decode_and_extract_sub(token, &key, &validation)
}

/// Decodes a JWT and extracts the 'sub' claim.
///
/// # Arguments
/// * `token` - The JWT string to decode.
/// * `key` - The decoding key.
/// * `validation` - The validation configuration.
///
/// # Returns
/// The user ID from the 'sub' claim on success.
///
/// # Errors
/// Returns `JwtError` based on the type of failure.
fn decode_and_extract_sub(
    token: &str,
    key: &DecodingKey,
    validation: &Validation,
) -> Result<String, JwtError> {
    let token_data = decode::<Claims>(token, key, validation).map_err(map_jwt_error)?;

    let user_id = token_data.claims.sub;
    if user_id.is_empty() {
        return Err(JwtError::MissingSubClaim);
    }

    Ok(user_id)
}

/// Maps jsonwebtoken errors to our JwtError type.
fn map_jwt_error(error: jsonwebtoken::errors::Error) -> JwtError {
    use jsonwebtoken::errors::ErrorKind;

    match error.kind() {
        ErrorKind::InvalidSignature => JwtError::InvalidSignature,
        ErrorKind::ExpiredSignature => JwtError::TokenExpired,
        ErrorKind::InvalidToken
        | ErrorKind::InvalidAlgorithm
        | ErrorKind::Base64(_)
        | ErrorKind::Json(_)
        | ErrorKind::Utf8(_) => JwtError::MalformedToken,
        ErrorKind::MissingRequiredClaim(_) => JwtError::MissingSubClaim,
        _ => JwtError::MalformedToken,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    fn create_hs256_token(sub: &str, secret: &[u8]) -> String {
        let claims = Claims {
            sub: sub.to_string(),
        };
        let header = Header::new(Algorithm::HS256);
        encode(&header, &claims, &EncodingKey::from_secret(secret))
            .expect("failed to create test token")
    }

    #[test]
    fn test_verify_hs256_valid_token() {
        let secret = b"test-secret-key-that-is-long-enough";
        let token = create_hs256_token("user-123", secret);

        let config = JwtConfig::Hs256 {
            secret: secret.to_vec(),
        };
        let result = verify_token(&token, &config);

        assert!(result.is_ok());
        assert_eq!(result.expect("verified token"), "user-123");
    }

    #[test]
    fn test_verify_hs256_invalid_signature() {
        let secret = b"test-secret-key-that-is-long-enough";
        let wrong_secret = b"wrong-secret-key-that-is-different";
        let token = create_hs256_token("user-123", secret);

        let config = JwtConfig::Hs256 {
            secret: wrong_secret.to_vec(),
        };
        let result = verify_token(&token, &config);

        assert!(result.is_err());
        assert!(matches!(result, Err(JwtError::InvalidSignature)));
    }

    #[test]
    fn test_verify_malformed_token() {
        let config = JwtConfig::Hs256 {
            secret: b"secret".to_vec(),
        };
        let result = verify_token("not-a-valid-jwt", &config);

        assert!(result.is_err());
        assert!(matches!(result, Err(JwtError::MalformedToken)));
    }

    #[test]
    fn test_verify_empty_token() {
        let config = JwtConfig::Hs256 {
            secret: b"secret".to_vec(),
        };
        let result = verify_token("", &config);

        assert!(result.is_err());
        assert!(matches!(result, Err(JwtError::MalformedToken)));
    }

    #[test]
    fn test_verify_rs256_invalid_key() {
        let config = JwtConfig::Rs256 {
            public_key: "not-a-valid-pem-key".to_string(),
        };
        let result = verify_token("some.jwt.token", &config);

        assert!(result.is_err());
        assert!(matches!(result, Err(JwtError::InvalidKey(_))));
    }

    #[test]
    fn test_jwt_error_display() {
        assert_eq!(
            JwtError::InvalidSignature.to_string(),
            "invalid JWT signature"
        );
        assert_eq!(JwtError::TokenExpired.to_string(), "JWT has expired");
        assert_eq!(JwtError::MalformedToken.to_string(), "malformed JWT");
        assert_eq!(
            JwtError::MissingSubClaim.to_string(),
            "missing 'sub' claim in JWT"
        );
        assert_eq!(
            JwtError::InvalidKey("bad key".to_string()).to_string(),
            "invalid key: bad key"
        );
    }

    #[test]
    fn test_verify_hs256_empty_sub_claim() {
        let secret = b"test-secret-key-that-is-long-enough";
        let token = create_hs256_token("", secret);

        let config = JwtConfig::Hs256 {
            secret: secret.to_vec(),
        };
        let result = verify_token(&token, &config);

        assert!(result.is_err());
        assert!(matches!(result, Err(JwtError::MissingSubClaim)));
    }

    #[test]
    fn test_verify_hs256_empty_secret() {
        let config = JwtConfig::Hs256 { secret: vec![] };
        let result = verify_token("some.jwt.token", &config);

        assert!(result.is_err());
        match result {
            Err(JwtError::InvalidKey(message)) => {
                assert_eq!(message, "secret must be non-empty");
            }
            _ => panic!("expected InvalidKey error"),
        }
    }

    #[test]
    fn test_verify_token_with_different_users() {
        let secret = b"test-secret-key-that-is-long-enough";

        let token1 = create_hs256_token("alice", secret);
        let token2 = create_hs256_token("bob", secret);

        let config = JwtConfig::Hs256 {
            secret: secret.to_vec(),
        };

        let result1 = verify_token(&token1, &config);
        let result2 = verify_token(&token2, &config);

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(result1.expect("alice token"), "alice");
        assert_eq!(result2.expect("bob token"), "bob");
    }
}
