//! End-to-end tests at the proto request/response level.
//!
//! Each test file covers a specific scenario, using deterministic inputs
//! to verify the complete request/response cycle.

#![cfg(test)]

mod helpers;

mod test_columns;
mod test_determinism;
mod test_empty_triples;
mod test_insert_boolean;
mod test_insert_multiple_entities;
mod test_insert_multiple_triples;
mod test_insert_number;
mod test_insert_string;
mod test_invalid_attribute_id;
mod test_invalid_entity_id;
mod test_many_inserts;
mod test_missing_fields;
mod test_query_empty_database;
mod test_query_nonexistent;
mod test_request_id;
mod test_sequence;
mod test_string_limits;
mod test_update_changes_type;
mod test_update_overwrites;
