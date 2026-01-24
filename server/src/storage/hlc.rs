//! Hybrid Logical Clock (HLC) implementation.
//!
//! HLC provides a logical timestamp that combines:
//! - Physical wall clock time for rough ordering
//! - Logical counter for fine-grained ordering within the same physical time
//! - Node ID for distributed uniqueness
//!
//! # Usage
//!
//! ```ignore
//! use storage::hlc::Clock;
//!
//! // Create a clock for node 1
//! let mut clock = Clock::new(1);
//!
//! // Get timestamp for a local event
//! let ts1 = clock.tick();
//!
//! // Receive a timestamp from another node and merge
//! let ts2 = clock.receive(remote_timestamp);
//! ```
//!
//! # Guarantees
//!
//! - Monotonically increasing timestamps within a node
//! - Causally ordered: if A happens-before B, then ts(A) < ts(B)
//! - Bounded drift from physical time (configurable)

use std::time::{SystemTime, UNIX_EPOCH};

use crate::storage::superblock::HlcTimestamp;

/// Maximum allowed drift between physical time and HLC physical component.
/// If the clock drifts more than this, we'll wait or error.
const MAX_DRIFT_MS: u64 = 60_000; // 1 minute

/// A Hybrid Logical Clock.
///
/// The clock maintains state and generates timestamps that are:
/// - Monotonically increasing for local events
/// - Causally consistent when merged with remote timestamps
#[derive(Debug)]
pub struct Clock {
    /// The last timestamp issued by this clock.
    last: HlcTimestamp,
    /// This node's unique identifier.
    node_id: u32,
    /// Maximum allowed forward drift in milliseconds.
    max_drift_ms: u64,
}

impl Clock {
    /// Create a new clock for the given node ID.
    ///
    /// The clock starts at the current wall clock time.
    #[must_use]
    pub fn new(node_id: u32) -> Self {
        let now = Self::wall_clock_ms();
        Self {
            last: HlcTimestamp {
                physical_time: now,
                logical_counter: 0,
                node_id,
            },
            node_id,
            max_drift_ms: MAX_DRIFT_MS,
        }
    }

    /// Create a clock initialized from a previous timestamp.
    ///
    /// This is useful when reopening a database and restoring clock state.
    #[must_use]
    pub fn from_timestamp(node_id: u32, last: HlcTimestamp) -> Self {
        // Ensure we don't go backwards from the saved timestamp
        let now = Self::wall_clock_ms();
        let physical_time = now.max(last.physical_time);

        Self {
            last: HlcTimestamp {
                physical_time,
                logical_counter: if physical_time == last.physical_time {
                    last.logical_counter
                } else {
                    0
                },
                node_id,
            },
            node_id,
            max_drift_ms: MAX_DRIFT_MS,
        }
    }

    /// Set the maximum allowed forward drift.
    pub const fn set_max_drift_ms(&mut self, max_drift_ms: u64) {
        self.max_drift_ms = max_drift_ms;
    }

    /// Get the current wall clock time in milliseconds since Unix epoch.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Milliseconds won't overflow u64 for billions of years
    fn wall_clock_ms() -> u64 {
        // SystemTime::now() can't fail on any supported platform.
        // duration_since(UNIX_EPOCH) only fails if system time is before 1970.
        #[allow(clippy::expect_used)]
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before Unix epoch");
        duration.as_millis() as u64
    }

    /// Generate a new timestamp for a local event.
    ///
    /// This advances the clock and returns the new timestamp.
    /// The timestamp is guaranteed to be greater than any previously issued.
    pub fn tick(&mut self) -> HlcTimestamp {
        let now = Self::wall_clock_ms();

        if now > self.last.physical_time {
            // Physical time advanced, reset logical counter
            self.last = HlcTimestamp {
                physical_time: now,
                logical_counter: 0,
                node_id: self.node_id,
            };
        } else {
            // Physical time hasn't advanced (or went backwards), increment logical
            self.last.logical_counter = self.last.logical_counter.saturating_add(1);
        }

        self.last
    }

    /// Receive a timestamp from another node and merge with local clock.
    ///
    /// This implements the HLC receive rule:
    /// - Take the maximum of local physical time and received physical time
    /// - If equal, take maximum logical counter + 1
    /// - Otherwise, use appropriate logical counter
    ///
    /// Returns the merged timestamp and an error if the drift is too large.
    pub fn receive(&mut self, remote: HlcTimestamp) -> Result<HlcTimestamp, ClockError> {
        let now = Self::wall_clock_ms();

        // Check for excessive drift
        if remote.physical_time > now + self.max_drift_ms {
            return Err(ClockError::ExcessiveDrift {
                remote_time: remote.physical_time,
                local_time: now,
                drift_ms: remote.physical_time - now,
            });
        }

        let new_physical = now.max(self.last.physical_time).max(remote.physical_time);

        let new_logical = if new_physical == self.last.physical_time
            && new_physical == remote.physical_time
        {
            // All three are equal, take max logical + 1
            self.last
                .logical_counter
                .max(remote.logical_counter)
                .saturating_add(1)
        } else if new_physical == self.last.physical_time {
            // Local physical time is max, increment local logical
            self.last.logical_counter.saturating_add(1)
        } else if new_physical == remote.physical_time {
            // Remote physical time is max, increment remote logical
            remote.logical_counter.saturating_add(1)
        } else {
            // Wall clock is max, reset logical
            0
        };

        self.last = HlcTimestamp {
            physical_time: new_physical,
            logical_counter: new_logical,
            node_id: self.node_id,
        };

        Ok(self.last)
    }

    /// Get the last timestamp issued by this clock.
    #[must_use]
    pub const fn last(&self) -> HlcTimestamp {
        self.last
    }

    /// Get this clock's node ID.
    #[must_use]
    pub const fn node_id(&self) -> u32 {
        self.node_id
    }

    /// Check if one timestamp happened before another.
    ///
    /// Returns true if `a` is causally before `b`.
    #[must_use]
    pub const fn happens_before(a: HlcTimestamp, b: HlcTimestamp) -> bool {
        a.physical_time < b.physical_time
            || (a.physical_time == b.physical_time && a.logical_counter < b.logical_counter)
    }

    /// Compare two timestamps for ordering.
    ///
    /// Returns:
    /// - `Ordering::Less` if `a` happened before `b`
    /// - `Ordering::Greater` if `a` happened after `b`
    /// - `Ordering::Equal` if they are the same (including `node_id`)
    #[must_use]
    pub fn compare(a: HlcTimestamp, b: HlcTimestamp) -> std::cmp::Ordering {
        match a.physical_time.cmp(&b.physical_time) {
            std::cmp::Ordering::Equal => match a.logical_counter.cmp(&b.logical_counter) {
                std::cmp::Ordering::Equal => a.node_id.cmp(&b.node_id),
                other => other,
            },
            other => other,
        }
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Errors that can occur with clock operations.
#[derive(Debug)]
pub enum ClockError {
    /// The remote timestamp is too far in the future.
    ExcessiveDrift {
        remote_time: u64,
        local_time: u64,
        drift_ms: u64,
    },
}

impl std::fmt::Display for ClockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExcessiveDrift {
                remote_time,
                local_time,
                drift_ms,
            } => {
                write!(
                    f,
                    "excessive clock drift: remote={remote_time}, local={local_time}, drift={drift_ms}ms"
                )
            }
        }
    }
}

impl std::error::Error for ClockError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_clock_new() {
        let clock = Clock::new(42);
        assert_eq!(clock.node_id(), 42);
        assert!(clock.last().physical_time > 0);
        assert_eq!(clock.last().logical_counter, 0);
        assert_eq!(clock.last().node_id, 42);
    }

    #[test]
    fn test_clock_tick_monotonic() {
        let mut clock = Clock::new(1);

        let mut prev = clock.tick();
        for _ in 0..100 {
            let curr = clock.tick();
            assert!(
                Clock::happens_before(prev, curr),
                "timestamps should be monotonically increasing"
            );
            prev = curr;
        }
    }

    #[test]
    fn test_clock_tick_same_physical_time() {
        let mut clock = Clock::new(1);

        // Tick rapidly to stay in same physical millisecond
        let ts1 = clock.tick();
        let ts2 = clock.tick();
        let ts3 = clock.tick();

        // All should have same or increasing physical time
        assert!(ts1.physical_time <= ts2.physical_time);
        assert!(ts2.physical_time <= ts3.physical_time);

        // But logical counter should increase if physical time is same
        if ts1.physical_time == ts2.physical_time {
            assert!(ts2.logical_counter > ts1.logical_counter);
        }
    }

    #[test]
    fn test_clock_tick_physical_time_advances() {
        let mut clock = Clock::new(1);

        let ts1 = clock.tick();

        // Sleep to ensure physical time advances
        thread::sleep(Duration::from_millis(2));

        let ts2 = clock.tick();

        // Physical time should have advanced
        assert!(ts2.physical_time > ts1.physical_time);
        // Logical counter should reset
        assert_eq!(ts2.logical_counter, 0);
    }

    #[test]
    fn test_clock_receive_merge() {
        let mut clock = Clock::new(1);
        clock.tick();

        // Create a remote timestamp in the past
        let remote = HlcTimestamp {
            physical_time: clock.last().physical_time - 100,
            logical_counter: 5,
            node_id: 2,
        };

        let result = clock.receive(remote).expect("receive should succeed");

        // Should be greater than both local and remote
        assert!(Clock::happens_before(remote, result));
    }

    #[test]
    fn test_clock_receive_from_future() {
        let mut clock = Clock::new(1);
        clock.tick();

        // Create a remote timestamp slightly in the future
        let remote = HlcTimestamp {
            physical_time: clock.last().physical_time + 100,
            logical_counter: 5,
            node_id: 2,
        };

        let result = clock.receive(remote).expect("receive should succeed");

        // Local clock should advance to at least the remote time
        assert!(result.physical_time >= remote.physical_time);
    }

    #[test]
    fn test_clock_receive_excessive_drift() {
        let mut clock = Clock::new(1);
        clock.set_max_drift_ms(1000); // 1 second max drift

        // Create a remote timestamp far in the future
        let remote = HlcTimestamp {
            physical_time: Clock::wall_clock_ms() + 10_000, // 10 seconds in future
            logical_counter: 0,
            node_id: 2,
        };

        let result = clock.receive(remote);
        assert!(matches!(result, Err(ClockError::ExcessiveDrift { .. })));
    }

    #[test]
    fn test_clock_from_timestamp() {
        let saved = HlcTimestamp {
            physical_time: Clock::wall_clock_ms() - 1000, // 1 second ago
            logical_counter: 42,
            node_id: 1,
        };

        let clock = Clock::from_timestamp(1, saved);

        // Clock should be at current time (not in the past)
        assert!(clock.last().physical_time >= Clock::wall_clock_ms() - 10);
    }

    #[test]
    fn test_clock_from_future_timestamp() {
        let saved = HlcTimestamp {
            physical_time: Clock::wall_clock_ms() + 1000, // 1 second in future
            logical_counter: 42,
            node_id: 1,
        };

        let clock = Clock::from_timestamp(1, saved);

        // Clock should preserve the future timestamp
        assert!(clock.last().physical_time >= saved.physical_time);
        assert_eq!(clock.last().logical_counter, saved.logical_counter);
    }

    #[test]
    fn test_happens_before() {
        let a = HlcTimestamp {
            physical_time: 100,
            logical_counter: 5,
            node_id: 1,
        };

        let b = HlcTimestamp {
            physical_time: 100,
            logical_counter: 6,
            node_id: 1,
        };

        let c = HlcTimestamp {
            physical_time: 101,
            logical_counter: 0,
            node_id: 1,
        };

        assert!(Clock::happens_before(a, b));
        assert!(Clock::happens_before(b, c));
        assert!(Clock::happens_before(a, c));

        assert!(!Clock::happens_before(b, a));
        assert!(!Clock::happens_before(c, b));
        assert!(!Clock::happens_before(a, a));
    }

    #[test]
    fn test_compare() {
        use std::cmp::Ordering;

        let a = HlcTimestamp {
            physical_time: 100,
            logical_counter: 5,
            node_id: 1,
        };

        let b = HlcTimestamp {
            physical_time: 100,
            logical_counter: 5,
            node_id: 2,
        };

        let c = HlcTimestamp {
            physical_time: 100,
            logical_counter: 5,
            node_id: 1,
        };

        assert_eq!(Clock::compare(a, b), Ordering::Less); // same time, node_id decides
        assert_eq!(Clock::compare(a, c), Ordering::Equal); // exactly equal
        assert_eq!(Clock::compare(b, a), Ordering::Greater);
    }

    #[test]
    fn test_clock_concurrent_simulation() {
        // Simulate two nodes generating timestamps
        let mut clock1 = Clock::new(1);
        let mut clock2 = Clock::new(2);

        let ts1_1 = clock1.tick();
        let _ts2_1 = clock2.tick();

        // Node 2 receives from node 1
        let ts2_2 = clock2.receive(ts1_1).expect("receive");

        // Node 1 receives from node 2's latest
        let ts1_2 = clock1.receive(ts2_2).expect("receive");

        // All timestamps should be comparable
        // ts2_2 should be after ts1_1 (it was merged)
        assert!(Clock::happens_before(ts1_1, ts2_2) || ts1_1.physical_time == ts2_2.physical_time);

        // ts1_2 should be after ts2_2 (it was merged)
        assert!(Clock::happens_before(ts2_2, ts1_2) || ts2_2.physical_time == ts1_2.physical_time);
    }
}
