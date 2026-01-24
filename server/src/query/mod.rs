//! Query engine for the triple store.
//!
//! This module provides query execution capabilities on top of the storage engine.
//! It supports:
//! - Pattern matching with variables
//! - WHERE clauses (conjunction of patterns)
//! - OPTIONAL clauses (left join semantics)
//! - WHERE-NOT clauses (anti-join / negation)
//! - Filters (predicate functions)
//!
//! # Datalog-style Query Example
//!
//! ```ignore
//! use storage::Database;
//! use query::{Query, Pattern, PatternElement, QueryEngine};
//!
//! let mut db = Database::open(path)?;
//! let mut snapshot = db.begin_readonly();
//! let mut engine = QueryEngine::new(&mut snapshot);
//!
//! let query = Query::new()
//!     .find("e")
//!     .find("name")
//!     .where_pattern(Pattern::new(
//!         PatternElement::var("e"),
//!         PatternElement::field("name"),
//!         PatternElement::var("name"),
//!     ));
//!
//! let result = engine.execute(&query)?;
//! for row in &result.rows {
//!     println!("{:?}", row);
//! }
//!
//! let txn_id = snapshot.close();
//! db.release_snapshot(txn_id);
//! ```

// Allow dead code - this module exports a public API that isn't yet integrated
// with the HTTP handlers. The code is tested via integration tests.
#![allow(dead_code)]
#![allow(unused_imports)]

pub mod context;
pub mod engine;
mod executor;
pub mod types;

// Datalog-style query engine
pub use context::QueryContext;
pub use engine::{
    QueryEngine, query_entity_to_storage, query_field_to_storage, query_value_to_storage,
};
pub use types::{
    Datom, EntityId, FieldId, Filter, Pattern, PatternElement, Query, QueryResult, QueryRow,
    Triple, Value, Variable,
};

// Legacy query executor (operates on storage transactions)
pub use executor::value_to_storage;
#[allow(unused_imports)]
pub use executor::{QueryError, QueryExecutor};
