//! Query engine for the triple store.
//!
//! This module provides query execution capabilities on top of the storage engine.
//! It handles the conversion between protocol types and storage types.

mod executor;

// QueryError and QueryExecutor will be used when QueryRequest is implemented
#[allow(unused_imports)]
pub use executor::{QueryError, QueryExecutor};
pub use executor::value_to_storage;
