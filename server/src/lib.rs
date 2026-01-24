#![cfg_attr(test, allow(clippy::disallowed_methods))]
// Forbid unwrap() in production code to prevent panics from corrupt data.
// Test code is allowed to use unwrap() for convenience.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
// Life of a request:
// 1. Protobuf comes in
// 2. Convert / validate proto into internal request format
// 3. For queries:
//     - Convert to SQL
//     - Query the SQL database
//     - Convert to triples
//     - Respond
//    For updates:
//     - Append to log
//     - If accepted, go to subscription pub-sub
//
// System components:
//  - SQL database
//  - Datalog to SQL query engine
//  - Pub-sub component

mod client_connection;
mod constants;
mod e2e_tests;
pub mod proto;
mod query;
pub mod simulation;
pub mod storage;
#[cfg(test)]
mod testing;
mod types;

pub use client_connection::ClientConnection;
