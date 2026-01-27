//! Background garbage collection for the storage engine.
//!
//! The GC task runs asynchronously and processes tombstones in batches,
//! removing records that are no longer visible to any active snapshot.
//!
//! # Design
//!
//! The GC task uses a `Weak<RwLock<Database>>` to prevent reference cycles:
//! - The `Database` owns the `gc_notify` signal
//! - The task holds a weak reference to the database
//! - When the database is dropped, `Weak::upgrade()` returns `None` and the task exits
//!
//! # Usage
//!
//! The GC task is spawned by the database registry when a database is opened.
//! It processes tombstones after each commit that contains deletes.

use std::sync::{Arc, RwLock, Weak};

use tokio::sync::Notify;

use crate::storage::Database;

/// Configuration for the garbage collector.
#[derive(Debug, Clone, Copy)]
pub struct GcConfig {
    /// Maximum number of tombstones to process per tick.
    pub batch_size: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self { batch_size: 100 }
    }
}

/// Spawn a background GC task for a database.
///
/// The task waits for signals on `notify` and processes tombstones in batches.
/// It exits cleanly when the database is dropped (weak reference becomes invalid).
///
/// # Arguments
/// * `database` - Weak reference to the database to perform GC on
/// * `notify` - Notification channel signaled when there's GC work to do
/// * `config` - GC configuration
///
/// # Returns
/// A `JoinHandle` that can be used to await the task or cancel it on shutdown.
///
/// # Invariants
/// - Uses `Weak` reference to prevent reference cycles
/// - Processes at most `config.batch_size` tombstones per tick
/// - Exits cleanly when database is dropped
pub fn spawn_gc_task(
    database: Weak<RwLock<Database>>,
    notify: Arc<Notify>,
    config: GcConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        gc_loop(database, notify, config).await;
    })
}

/// The main GC loop.
///
/// Runs until the database is dropped or the task is cancelled.
async fn gc_loop(database: Weak<RwLock<Database>>, notify: Arc<Notify>, config: GcConfig) {
    loop {
        // Wait for notification that there's work to do
        notify.notified().await;

        // Try to upgrade the weak reference
        let Some(db_arc) = database.upgrade() else {
            // Database was dropped, exit the task
            break;
        };

        // Process one batch of tombstones
        // We acquire the write lock, process, then release it to allow other operations
        let result = {
            let Ok(mut db) = db_arc.write() else {
                // Lock was poisoned, exit the task
                eprintln!("GC error: database lock poisoned");
                break;
            };
            db.gc_tick(config.batch_size)
        };

        match result {
            Ok(tick_result) => {
                // If there are more tombstones remaining, signal ourselves to continue
                if tick_result.tombstones_remaining > 0 && tick_result.records_removed > 0 {
                    notify.notify_one();
                }
            }
            Err(e) => {
                // Log the error but continue - GC errors shouldn't crash the task
                // In production, this would use proper logging
                eprintln!("GC error: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;
    use crate::storage::buffer_pool::BufferPool;
    use std::time::Duration;
    use tempfile::tempdir;

    fn test_pool() -> Arc<BufferPool> {
        BufferPool::new(100)
    }

    #[tokio::test]
    async fn test_gc_task_exits_when_database_dropped() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        let pool = test_pool();

        // Create database wrapped in Arc<RwLock>
        let db = Database::create(&path, pool).expect("create db");
        let db_arc = Arc::new(RwLock::new(db));

        // Get the notify handle and spawn GC task
        let notify = {
            let db = db_arc.read().expect("lock should not be poisoned");
            db.gc_notify()
        };
        let weak = Arc::downgrade(&db_arc);
        let handle = spawn_gc_task(weak, Arc::clone(&notify), GcConfig::default());

        // Drop the database
        drop(db_arc);

        // Signal the GC task to wake it up - it should see the weak reference is invalid and exit
        notify.notify_one();

        // The task should exit within a reasonable timeout
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;

        // The task should have completed (not timed out)
        assert!(
            result.is_ok(),
            "GC task should exit when database is dropped"
        );
    }

    #[tokio::test]
    async fn test_gc_config_default() {
        let config = GcConfig::default();
        assert_eq!(config.batch_size, 100);
    }
}
