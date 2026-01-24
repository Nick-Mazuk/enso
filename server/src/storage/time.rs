//! Time source abstraction for deterministic simulation testing.
//!
//! This module provides a `TimeSource` trait that abstracts over time operations,
//! allowing the system to use real system time in production and simulated time
//! in tests.

use std::time::{SystemTime, UNIX_EPOCH};

/// Abstraction over time operations.
///
/// This trait allows swapping between real system time and simulated time
/// for deterministic testing.
pub trait TimeSource {
    /// Get the current time in milliseconds since Unix epoch.
    fn now_ms(&self) -> u64;
}

/// Real time source using system clock.
///
/// This is the default implementation used in production.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    #[allow(clippy::cast_possible_truncation)] // Milliseconds won't overflow u64 for billions of years
    fn now_ms(&self) -> u64 {
        // SystemTime::now() can't fail on any supported platform.
        // duration_since(UNIX_EPOCH) only fails if system time is before 1970.
        #[allow(clippy::expect_used)]
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before Unix epoch");
        duration.as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_time_source() {
        let source = SystemTimeSource;
        let t1 = source.now_ms();
        let t2 = source.now_ms();

        // Time should be reasonable (after 2020)
        assert!(t1 > 1_577_836_800_000); // 2020-01-01 00:00:00 UTC

        // Time should not go backwards
        assert!(t2 >= t1);
    }
}
