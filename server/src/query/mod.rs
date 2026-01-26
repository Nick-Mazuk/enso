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
//! ```
//! // Build a query that finds all entities with a "name" attribute
//! // using the pattern matching API:
//! //
//! // let query = Query::new()
//! //     .find("e")
//! //     .find("name")
//! //     .where_pattern(Pattern::new(
//! //         PatternElement::var("e"),
//! //         PatternElement::field("name"),
//! //         PatternElement::var("name"),
//! //     ));
//! //
//! // The query can be executed against a database snapshot:
//! // let mut engine = QueryEngine::new(&mut snapshot);
//! // let result = engine.execute(&query)?;
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
pub use engine::QueryEngine;
pub use types::{
    Datom, EntityId, FieldId, Filter, Pattern, PatternElement, Query, QueryResult, QueryRow,
    Triple, Value, Variable,
};

// Legacy query executor (operates on storage transactions)
#[allow(unused_imports)]
pub use executor::{QueryError, QueryExecutor};
