//! Deterministic Simulation Testing (DST) infrastructure.
//!
//! This module provides tools for testing the database with:
//! - Controlled time (no real system time)
//! - In-memory storage with fault injection
//! - Reproducible random message generation
//! - Invariant checking after each operation
//!
//! # Design Principles
//!
//! Following patterns from `TigerBeetle` and `Turso`:
//! 1. All I/O is abstracted and can be simulated
//! 2. All randomness is seeded for reproducibility
//! 3. Time is controlled, not real
//! 4. Faults can be injected at any I/O boundary
//! 5. Given the same seed, execution is identical
//!
//! # Usage
//!
//! ```ignore
//! use simulation::{Simulator, SimulatorConfig};
//!
//! let config = SimulatorConfig::new(12345) // seed
//!     .with_fault_rate(0.01)
//!     .with_malformed_rate(0.1);
//!
//! let mut sim = Simulator::new(config);
//! let result = sim.run(1000); // Run 1000 messages
//!
//! assert!(result.invariant_violations.is_empty());
//! ```

mod invariants;
mod message_gen;
mod simulator;
mod storage;
mod time;

pub use invariants::{InvariantChecker, InvariantViolation, OperationHistory};
pub use message_gen::{MalformationType, MessageGenConfig, MessageGenerator};
pub use simulator::{SimulationResult, Simulator, SimulatorConfig};
pub use storage::{FaultConfig, SimulatedStorage};
pub use time::SimulatedTimeSource;
