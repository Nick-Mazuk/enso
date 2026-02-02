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

pub use app_config::{AppConfig, JwtConfig};
