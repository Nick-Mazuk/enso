//! Authentication module.
//!
//! This module provides authentication configuration and utilities for the Enso server.
//!
//! # Pre-conditions
//! - Applications must be configured with valid API keys.
//!
//! # Post-conditions
//! - Authentication configuration is immutable once loaded.
//!
//! # Invariants
//! - All configured API keys are non-empty.

pub mod app_config;
pub mod config_registry;
pub mod jwt;

pub use app_config::{AppConfig, JwtConfig, JwtConfigError, Rs256PublicKey};
pub use config_registry::{ConfigRegistry, ConfigRegistryError};
pub use jwt::{JwtError, verify_token};
