//! Main simulator harness for deterministic simulation testing.
//!
//! This module ties together all the simulation components to provide
//! a complete testing framework for the triple store server.

// Simulation code legitimately needs cloning for test data
#![allow(clippy::disallowed_methods)]

use std::sync::atomic::{AtomicU64, Ordering};

use crate::client_connection::ClientConnection;
use crate::proto;
use crate::storage::Database;

/// Counter for generating unique simulator instance IDs.
static SIMULATOR_COUNTER: AtomicU64 = AtomicU64::new(0);

use super::invariants::{InvariantChecker, InvariantViolation, OperationHistory};
use super::message_gen::{MessageGenConfig, MessageGenerator};
use super::storage::{FaultConfig, SimulatedStorage};
use super::time::SimulatedTimeSource;

/// Configuration for the simulator.
#[derive(Debug, Clone)]
pub struct SimulatorConfig {
    /// Random seed for reproducibility.
    pub seed: u64,
    /// Fault injection configuration.
    pub fault_config: FaultConfig,
    /// Message generation configuration.
    pub message_config: MessageGenConfig,
    /// Whether to advance time between operations.
    pub advance_time: bool,
    /// Milliseconds to advance time per operation.
    pub time_advance_ms: u64,
}

impl SimulatorConfig {
    /// Create a new simulator config with the given seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            fault_config: FaultConfig::default(),
            message_config: MessageGenConfig::default(),
            advance_time: true,
            time_advance_ms: 1,
        }
    }

    /// Set the fault configuration.
    #[must_use]
    pub const fn with_fault_config(mut self, config: FaultConfig) -> Self {
        self.fault_config = config;
        self
    }

    /// Set the message configuration.
    #[must_use]
    pub const fn with_message_config(mut self, config: MessageGenConfig) -> Self {
        self.message_config = config;
        self
    }

    /// Set the malformed message rate.
    #[must_use]
    pub const fn with_malformed_rate(mut self, rate: f64) -> Self {
        self.message_config.malformed_rate = rate;
        self
    }

    /// Disable time advancement (for faster testing).
    #[must_use]
    pub const fn without_time_advance(mut self) -> Self {
        self.advance_time = false;
        self
    }
}

/// Results from a simulation run.
#[derive(Debug)]
pub struct SimulationResult {
    /// The seed used for this simulation.
    pub seed: u64,
    /// Number of messages processed.
    pub messages_processed: u64,
    /// Number of successful operations.
    pub successful_operations: u64,
    /// Number of failed operations (expected failures like validation errors).
    pub failed_operations: u64,
    /// Invariant violations detected.
    pub invariant_violations: Vec<InvariantViolation>,
    /// Whether the simulation completed without panics.
    pub completed_successfully: bool,
    /// Error message if simulation failed.
    pub error: Option<String>,
}

impl SimulationResult {
    /// Check if the simulation passed (no invariant violations).
    #[must_use]
    pub const fn passed(&self) -> bool {
        self.completed_successfully && self.invariant_violations.is_empty()
    }
}

/// The main simulator harness.
///
/// This ties together all simulation components:
/// - Simulated storage with fault injection
/// - Simulated time source
/// - Message generator
/// - Invariant checker
/// - Client connection handling
pub struct Simulator {
    config: SimulatorConfig,
    message_generator: MessageGenerator,
    history: OperationHistory,
    checker: InvariantChecker,
    time_source: SimulatedTimeSource,
    messages_processed: u64,
    successful_operations: u64,
    failed_operations: u64,
}

impl Simulator {
    /// Create a new simulator with the given configuration.
    #[must_use]
    pub fn new(config: SimulatorConfig) -> Self {
        let message_generator =
            MessageGenerator::with_config(config.seed, config.message_config.clone());
        let time_source = SimulatedTimeSource::default_start();

        Self {
            config,
            message_generator,
            history: OperationHistory::new(),
            checker: InvariantChecker::new(),
            time_source,
            messages_processed: 0,
            successful_operations: 0,
            failed_operations: 0,
        }
    }

    /// Run the simulation for a given number of messages.
    ///
    /// This creates a fresh database, sends the specified number of messages,
    /// and checks invariants after each operation.
    pub fn run(&mut self, message_count: usize) -> SimulationResult {
        // Create simulated storage (not used yet - full DST would make Database generic)
        let _storage =
            SimulatedStorage::with_config(self.config.seed, self.config.fault_config.clone());

        // Create database with simulated storage
        // NOTE: For now we use the real Database which uses DatabaseFile internally.
        // A full DST implementation would make Database generic over Storage.
        // For this initial implementation, we'll test through the ClientConnection
        // interface using a real temp database.

        // Create a temporary database for testing with a unique ID
        let temp_dir = std::env::temp_dir();
        let instance_id = SIMULATOR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!("dst_sim_{}_{}.db", self.config.seed, instance_id));

        // Remove if exists
        let _ = std::fs::remove_file(&db_path);

        let database = match Database::create(&db_path) {
            Ok(db) => db,
            Err(e) => {
                return SimulationResult {
                    seed: self.config.seed,
                    messages_processed: 0,
                    successful_operations: 0,
                    failed_operations: 0,
                    invariant_violations: vec![],
                    completed_successfully: false,
                    error: Some(format!("Failed to create database: {e}")),
                };
            }
        };

        // Create client connection (database now handles broadcasting internally)
        let client_connection = ClientConnection::new(database);

        // Run the simulation
        let result = self.run_with_connection(&client_connection, message_count);

        // Cleanup
        let _ = std::fs::remove_file(&db_path);

        result
    }

    /// Run simulation with an existing client connection.
    fn run_with_connection(
        &mut self,
        client_connection: &ClientConnection,
        message_count: usize,
    ) -> SimulationResult {
        #[allow(clippy::expect_used)] // Runtime creation failure is fatal
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");

        for _ in 0..message_count {
            // Generate next message
            let message = self.message_generator.next_message();
            self.messages_processed += 1;

            // Process the message
            let response =
                runtime.block_on(async { client_connection.handle_message(message.clone()).await });

            // Extract the server response
            let Some(proto::server_message::Payload::Response(server_response)) = &response.payload
            else {
                self.checker.add_violation(InvariantViolation {
                    description: "No response returned".to_string(),
                    operation_index: self.history.len(),
                    context: String::new(),
                });
                continue;
            };

            // Check invariants and record operation
            match &message.payload {
                Some(proto::client_message::Payload::TripleUpdateRequest(req)) => {
                    self.checker
                        .check_update_response(req, server_response, self.history.len());
                    self.history.record_update(req.clone(), server_response);

                    if server_response
                        .status
                        .as_ref()
                        .is_some_and(|s| s.code == proto::google::rpc::Code::Ok as i32)
                    {
                        self.successful_operations += 1;
                    } else {
                        self.failed_operations += 1;
                    }
                }
                Some(proto::client_message::Payload::Query(req)) => {
                    self.checker
                        .check_query_response(req, server_response, self.history.len());
                    self.history.record_query(req.clone(), server_response);

                    if server_response
                        .status
                        .as_ref()
                        .is_some_and(|s| s.code == proto::google::rpc::Code::Ok as i32)
                    {
                        self.successful_operations += 1;
                    } else {
                        self.failed_operations += 1;
                    }
                }
                Some(
                    proto::client_message::Payload::Subscribe(_)
                    | proto::client_message::Payload::Unsubscribe(_),
                ) => {
                    // Subscriptions not supported in simulation yet
                    self.failed_operations += 1;
                }
                None => {
                    // Message with no payload - this is an error
                    self.failed_operations += 1;
                }
            }

            // Advance time if configured
            if self.config.advance_time {
                self.time_source.advance(self.config.time_advance_ms);
            }
        }

        SimulationResult {
            seed: self.config.seed,
            messages_processed: self.messages_processed,
            successful_operations: self.successful_operations,
            failed_operations: self.failed_operations,
            invariant_violations: self.checker.violations().to_vec(),
            completed_successfully: true,
            error: None,
        }
    }

    /// Get the operation history.
    #[must_use]
    pub const fn history(&self) -> &OperationHistory {
        &self.history
    }

    /// Get the invariant checker.
    #[must_use]
    pub const fn checker(&self) -> &InvariantChecker {
        &self.checker
    }

    /// Get statistics about the simulation.
    #[must_use]
    pub fn stats(&self) -> SimulatorStats {
        SimulatorStats {
            messages_processed: self.messages_processed,
            successful_operations: self.successful_operations,
            failed_operations: self.failed_operations,
            invariant_violations: self.checker.violations().len(),
        }
    }
}

/// Statistics about the simulation.
#[derive(Debug, Clone)]
pub struct SimulatorStats {
    /// Number of messages processed.
    pub messages_processed: u64,
    /// Number of successful operations.
    pub successful_operations: u64,
    /// Number of failed operations.
    pub failed_operations: u64,
    /// Number of invariant violations.
    pub invariant_violations: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulator_basic() {
        let config = SimulatorConfig::new(12345);
        let mut simulator = Simulator::new(config);

        let result = simulator.run(100);

        assert!(result.completed_successfully);
        assert_eq!(result.messages_processed, 100);
        // Some operations may fail due to validation, that's expected
        assert!(result.successful_operations + result.failed_operations == 100);
    }

    #[test]
    fn test_simulator_with_malformed_messages() {
        let config = SimulatorConfig::new(12345).with_malformed_rate(0.5);
        let mut simulator = Simulator::new(config);

        let result = simulator.run(100);

        assert!(result.completed_successfully);
        // With malformed messages, we expect more failures
        assert!(result.failed_operations > 0);
    }

    #[test]
    fn test_simulator_deterministic() {
        // Same seed should produce same results
        let config1 = SimulatorConfig::new(12345);
        let mut sim1 = Simulator::new(config1);
        let result1 = sim1.run(50);

        let config2 = SimulatorConfig::new(12345);
        let mut sim2 = Simulator::new(config2);
        let result2 = sim2.run(50);

        assert_eq!(result1.messages_processed, result2.messages_processed);
        assert_eq!(result1.successful_operations, result2.successful_operations);
        assert_eq!(result1.failed_operations, result2.failed_operations);
    }

    #[test]
    fn test_simulator_no_invariant_violations() {
        // A basic simulation should not have invariant violations
        let config = SimulatorConfig::new(54321);
        let mut simulator = Simulator::new(config);

        let result = simulator.run(200);

        assert!(
            result.passed(),
            "Simulation should pass: {:?}",
            result.invariant_violations
        );
    }

    #[test]
    #[ignore] // Long running test
    fn test_simulator_stress() {
        let config = SimulatorConfig::new(99999)
            .with_malformed_rate(0.1)
            .without_time_advance();
        let mut simulator = Simulator::new(config);

        let result = simulator.run(10_000);

        assert!(result.completed_successfully);
        assert!(result.passed());
    }
}
