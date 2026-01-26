//! Simulated time source for deterministic testing.
//!
//! This module provides a controlled time source that allows tests to
//! advance time explicitly, ensuring deterministic behavior.

use std::cell::Cell;

use crate::storage::time::TimeSource;

/// A simulated time source for deterministic testing.
///
/// Unlike [`SystemTimeSource`](crate::storage::time::SystemTimeSource), this
/// implementation does not use the real system clock. Instead, time only
/// advances when explicitly told to, making tests fully deterministic.
///
/// # Thread Safety
///
/// This implementation uses [`Cell`] for interior mutability, making it
/// single-threaded only. For DST, we run everything in a single thread
/// anyway, so this is fine.
///
/// # Example
///
/// ```
/// use server::simulation::SimulatedTimeSource;
/// use server::storage::time::TimeSource;
///
/// let time = SimulatedTimeSource::new(1000);
/// assert_eq!(time.now_ms(), 1000);
///
/// time.advance(100);
/// assert_eq!(time.now_ms(), 1100);
///
/// time.set(5000);
/// assert_eq!(time.now_ms(), 5000);
/// ```
#[derive(Debug)]
pub struct SimulatedTimeSource {
    /// Current simulated time in milliseconds since Unix epoch.
    current_time_ms: Cell<u64>,
}

impl SimulatedTimeSource {
    /// Create a new simulated time source with the given initial time.
    ///
    /// # Arguments
    ///
    /// * `initial_time_ms` - The initial time in milliseconds since Unix epoch.
    ///   A reasonable default is around `1_700_000_000_000` (late 2023).
    #[must_use]
    pub const fn new(initial_time_ms: u64) -> Self {
        Self {
            current_time_ms: Cell::new(initial_time_ms),
        }
    }

    /// Create a new simulated time source starting at a reasonable default time.
    ///
    /// Uses `1_700_000_000_000` (approximately November 2023) as the starting point.
    #[must_use]
    pub const fn default_start() -> Self {
        Self::new(1_700_000_000_000)
    }

    /// Advance time by the given number of milliseconds.
    ///
    /// Time saturates at `u64::MAX` if overflow would occur.
    pub fn advance(&self, ms: u64) {
        let current = self.current_time_ms.get();
        self.current_time_ms.set(current.saturating_add(ms));
    }

    /// Set the current time to a specific value.
    ///
    /// Note: This can move time backwards, which might cause issues
    /// with HLC if not used carefully. Prefer `advance` for normal testing.
    pub fn set(&self, time_ms: u64) {
        self.current_time_ms.set(time_ms);
    }

    /// Get the current simulated time without advancing it.
    #[must_use]
    pub fn current(&self) -> u64 {
        self.current_time_ms.get()
    }
}

impl TimeSource for SimulatedTimeSource {
    fn now_ms(&self) -> u64 {
        self.current_time_ms.get()
    }
}

impl Default for SimulatedTimeSource {
    fn default() -> Self {
        Self::default_start()
    }
}

impl Clone for SimulatedTimeSource {
    fn clone(&self) -> Self {
        Self {
            current_time_ms: Cell::new(self.current_time_ms.get()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulated_time_initial() {
        let time = SimulatedTimeSource::new(1000);
        assert_eq!(time.now_ms(), 1000);
        assert_eq!(time.current(), 1000);
    }

    #[test]
    fn test_simulated_time_advance() {
        let time = SimulatedTimeSource::new(1000);

        time.advance(100);
        assert_eq!(time.now_ms(), 1100);

        time.advance(50);
        assert_eq!(time.now_ms(), 1150);
    }

    #[test]
    fn test_simulated_time_set() {
        let time = SimulatedTimeSource::new(1000);

        time.set(5000);
        assert_eq!(time.now_ms(), 5000);

        // Can go backwards (careful with HLC!)
        time.set(3000);
        assert_eq!(time.now_ms(), 3000);
    }

    #[test]
    fn test_simulated_time_default() {
        let time = SimulatedTimeSource::default_start();
        assert_eq!(time.now_ms(), 1_700_000_000_000);
    }

    #[test]
    fn test_simulated_time_deterministic() {
        // Same starting conditions produce same results
        let time1 = SimulatedTimeSource::new(1000);
        let time2 = SimulatedTimeSource::new(1000);

        for _ in 0..100 {
            time1.advance(1);
            time2.advance(1);
        }

        assert_eq!(time1.now_ms(), time2.now_ms());
        assert_eq!(time1.now_ms(), 1100);
    }
}
