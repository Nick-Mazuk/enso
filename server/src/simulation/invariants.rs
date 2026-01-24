//! Invariant checking for deterministic simulation testing.
//!
//! This module provides infrastructure for verifying database invariants
//! after each operation, helping to detect bugs and data corruption.

// Simulation code legitimately needs cloning for test data
#![allow(clippy::disallowed_methods)]

use std::collections::HashMap;

use crate::proto;

/// A recorded operation in the simulation.
#[allow(dead_code)] // Fields used for debugging and future invariant checks
#[derive(Debug)]
pub enum Operation {
    /// An update request.
    Update {
        /// The request that was sent.
        request: proto::TripleUpdateRequest,
        /// Whether the operation succeeded.
        success: bool,
        /// Error message if failed.
        error: Option<String>,
    },
    /// A query request.
    Query {
        /// The query that was sent.
        request: proto::QueryRequest,
        /// Whether the operation succeeded.
        success: bool,
        /// Number of rows returned.
        row_count: usize,
        /// Error message if failed.
        error: Option<String>,
    },
}

/// Tracks the history of operations for invariant checking.
#[derive(Debug, Default)]
pub struct OperationHistory {
    /// All operations in order.
    operations: Vec<Operation>,
    /// Successfully written triples: (`entity_id`, `attribute_id`) -> value
    /// This tracks what we expect to be in the database.
    expected_state: HashMap<([u8; 16], [u8; 16]), ExpectedValue>,
    /// Number of successful updates.
    successful_updates: u64,
    /// Number of failed updates.
    failed_updates: u64,
    /// Number of successful queries.
    successful_queries: u64,
    /// Number of failed queries.
    failed_queries: u64,
}

/// An expected value in the database.
#[derive(Debug, Clone)]
pub struct ExpectedValue {
    /// The value itself.
    pub value: proto::TripleValue,
    /// When it was written (operation index).
    pub written_at: usize,
}

impl OperationHistory {
    /// Create a new empty history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an update operation.
    pub fn record_update(
        &mut self,
        request: proto::TripleUpdateRequest,
        response: &proto::ServerResponse,
    ) {
        let success = response
            .status
            .as_ref()
            .is_some_and(|s| s.code == proto::google::rpc::Code::Ok as i32);

        let error = if success {
            None
        } else {
            response.status.as_ref().map(|s| s.message.clone())
        };

        // If successful, update expected state
        if success {
            self.successful_updates += 1;
            for triple in &request.triples {
                if let (Some(entity_id), Some(attribute_id), Some(value)) =
                    (&triple.entity_id, &triple.attribute_id, &triple.value)
                {
                    if let (Ok(e), Ok(a)) = (
                        <[u8; 16]>::try_from(entity_id.as_slice()),
                        <[u8; 16]>::try_from(attribute_id.as_slice()),
                    ) {
                        self.expected_state.insert(
                            (e, a),
                            ExpectedValue {
                                value: value.clone(),
                                written_at: self.operations.len(),
                            },
                        );
                    }
                }
            }
        } else {
            self.failed_updates += 1;
        }

        self.operations.push(Operation::Update {
            request,
            success,
            error,
        });
    }

    /// Record a query operation.
    pub fn record_query(&mut self, request: proto::QueryRequest, response: &proto::ServerResponse) {
        let success = response
            .status
            .as_ref()
            .is_some_and(|s| s.code == proto::google::rpc::Code::Ok as i32);

        let error = if success {
            None
        } else {
            response.status.as_ref().map(|s| s.message.clone())
        };

        let row_count = response.rows.len();

        if success {
            self.successful_queries += 1;
        } else {
            self.failed_queries += 1;
        }

        self.operations.push(Operation::Query {
            request,
            success,
            row_count,
            error,
        });
    }

    /// Get the number of operations.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if history is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Get statistics.
    #[must_use]
    pub fn stats(&self) -> HistoryStats {
        HistoryStats {
            total_operations: self.operations.len(),
            successful_updates: self.successful_updates,
            failed_updates: self.failed_updates,
            successful_queries: self.successful_queries,
            failed_queries: self.failed_queries,
            unique_keys: self.expected_state.len(),
        }
    }

    /// Get the expected state (for verification).
    #[must_use]
    pub const fn expected_state(&self) -> &HashMap<([u8; 16], [u8; 16]), ExpectedValue> {
        &self.expected_state
    }
}

/// Statistics about the operation history.
#[derive(Debug, Clone)]
pub struct HistoryStats {
    /// Total number of operations.
    pub total_operations: usize,
    /// Number of successful updates.
    pub successful_updates: u64,
    /// Number of failed updates.
    pub failed_updates: u64,
    /// Number of successful queries.
    pub successful_queries: u64,
    /// Number of failed queries.
    pub failed_queries: u64,
    /// Number of unique keys in expected state.
    pub unique_keys: usize,
}

/// An invariant violation detected during simulation.
#[derive(Debug, Clone)]
pub struct InvariantViolation {
    /// Description of the violation.
    pub description: String,
    /// Operation index where it was detected.
    pub operation_index: usize,
    /// Additional context.
    pub context: String,
}

/// Checker for database invariants.
pub struct InvariantChecker {
    /// Detected violations.
    violations: Vec<InvariantViolation>,
}

impl Default for InvariantChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl InvariantChecker {
    /// Create a new invariant checker.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            violations: Vec::new(),
        }
    }

    /// Get all violations.
    #[must_use]
    pub fn violations(&self) -> &[InvariantViolation] {
        &self.violations
    }

    /// Check if any violations were detected.
    #[must_use]
    pub const fn has_violations(&self) -> bool {
        !self.violations.is_empty()
    }

    /// Clear all recorded violations.
    pub fn clear(&mut self) {
        self.violations.clear();
    }

    /// Add a violation.
    pub fn add_violation(&mut self, violation: InvariantViolation) {
        self.violations.push(violation);
    }

    /// Check that a response is valid (has proper structure).
    pub fn check_response_valid(
        &mut self,
        response: &proto::ServerResponse,
        operation_index: usize,
    ) {
        // Response must have a status
        if response.status.is_none() {
            self.violations.push(InvariantViolation {
                description: "Response missing status".to_string(),
                operation_index,
                context: String::new(),
            });
        }
    }

    /// Check that an error response has a proper error code.
    pub fn check_error_response(
        &mut self,
        response: &proto::ServerResponse,
        operation_index: usize,
    ) {
        if let Some(status) = &response.status {
            // If not OK, should have a message
            if status.code != proto::google::rpc::Code::Ok as i32 && status.message.is_empty() {
                self.violations.push(InvariantViolation {
                    description: "Error response missing message".to_string(),
                    operation_index,
                    context: format!("code: {}", status.code),
                });
            }
        }
    }

    /// Check that query results have consistent structure.
    pub fn check_query_result_structure(
        &mut self,
        response: &proto::ServerResponse,
        operation_index: usize,
    ) {
        let num_columns = response.columns.len();

        for (row_idx, row) in response.rows.iter().enumerate() {
            if row.values.len() != num_columns {
                self.violations.push(InvariantViolation {
                    description: "Row has wrong number of values".to_string(),
                    operation_index,
                    context: format!(
                        "row {} has {} values, expected {}",
                        row_idx,
                        row.values.len(),
                        num_columns
                    ),
                });
            }
        }
    }

    /// Run all checks on an update response.
    pub fn check_update_response(
        &mut self,
        _request: &proto::TripleUpdateRequest,
        response: &proto::ServerResponse,
        operation_index: usize,
    ) {
        self.check_response_valid(response, operation_index);
        self.check_error_response(response, operation_index);
    }

    /// Run all checks on a query response.
    pub fn check_query_response(
        &mut self,
        _request: &proto::QueryRequest,
        response: &proto::ServerResponse,
        operation_index: usize,
    ) {
        self.check_response_valid(response, operation_index);
        self.check_error_response(response, operation_index);
        self.check_query_result_structure(response, operation_index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_history_record_update() {
        let mut history = OperationHistory::new();

        let request = proto::TripleUpdateRequest {
            triples: vec![proto::Triple {
                entity_id: Some(vec![1u8; 16]),
                attribute_id: Some(vec![2u8; 16]),
                value: Some(proto::TripleValue {
                    value: Some(proto::triple_value::Value::String("test".to_string())),
                }),
            }],
        };

        let response = proto::ServerResponse {
            request_id: Some(1),
            status: Some(proto::google::rpc::Status {
                code: proto::google::rpc::Code::Ok as i32,
                ..Default::default()
            }),
            ..Default::default()
        };

        history.record_update(request, &response);

        assert_eq!(history.len(), 1);
        assert_eq!(history.stats().successful_updates, 1);
        assert_eq!(history.expected_state().len(), 1);
    }

    #[test]
    fn test_operation_history_record_failed_update() {
        let mut history = OperationHistory::new();

        let request = proto::TripleUpdateRequest { triples: vec![] };

        let response = proto::ServerResponse {
            request_id: Some(1),
            status: Some(proto::google::rpc::Status {
                code: proto::google::rpc::Code::InvalidArgument as i32,
                message: "Invalid request".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        history.record_update(request, &response);

        assert_eq!(history.len(), 1);
        assert_eq!(history.stats().failed_updates, 1);
        assert_eq!(history.expected_state().len(), 0);
    }

    #[test]
    fn test_invariant_checker_response_valid() {
        let mut checker = InvariantChecker::new();

        // Valid response
        let response = proto::ServerResponse {
            status: Some(proto::google::rpc::Status {
                code: proto::google::rpc::Code::Ok as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        checker.check_response_valid(&response, 0);
        assert!(!checker.has_violations());

        // Invalid response (no status)
        let response = proto::ServerResponse::default();
        checker.check_response_valid(&response, 1);
        assert!(checker.has_violations());
    }

    #[test]
    fn test_invariant_checker_query_result_structure() {
        let mut checker = InvariantChecker::new();

        // Valid structure
        let response = proto::ServerResponse {
            status: Some(proto::google::rpc::Status {
                code: proto::google::rpc::Code::Ok as i32,
                ..Default::default()
            }),
            columns: vec!["a".to_string(), "b".to_string()],
            rows: vec![proto::QueryResultRow {
                values: vec![
                    proto::QueryResultValue::default(),
                    proto::QueryResultValue::default(),
                ],
            }],
            ..Default::default()
        };
        checker.check_query_result_structure(&response, 0);
        assert!(!checker.has_violations());

        // Invalid structure (wrong number of values)
        let response = proto::ServerResponse {
            status: Some(proto::google::rpc::Status {
                code: proto::google::rpc::Code::Ok as i32,
                ..Default::default()
            }),
            columns: vec!["a".to_string(), "b".to_string()],
            rows: vec![proto::QueryResultRow {
                values: vec![proto::QueryResultValue::default()], // Only 1 value, expected 2
            }],
            ..Default::default()
        };
        checker.check_query_result_structure(&response, 1);
        assert!(checker.has_violations());
    }
}
