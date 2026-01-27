//! Triple store storage engine.
//!
//! A single-file storage engine optimized for triple store workloads.
//!
//! # File Format
//!
//! The database is stored in a single file with 8KB pages:
//!
//! - Page 0: Superblock (metadata about the database)
//! - Pages 1-N: Allocation bitmap (tracks free/used pages)
//! - Remaining pages: B-tree nodes, overflow pages, etc.
//!
//! # Usage
//!
//! ```no_run
//! use server::storage::{DatabaseFile, Transaction};
//! use server::storage::buffer_pool::BufferPool;
//! use server::types::{EntityId, AttributeId, TripleValue};
//! use std::path::Path;
//!
//! // Create a buffer pool and new database
//! let pool = BufferPool::new(100);
//! let path = Path::new("/tmp/my_database");
//! let mut db = DatabaseFile::create(path, pool).unwrap();
//!
//! // Begin a transaction
//! let mut txn = Transaction::begin(&mut db).unwrap();
//!
//! // Insert a triple
//! let entity_id = EntityId([1u8; 16]);
//! let attribute_id = AttributeId([2u8; 16]);
//! txn.insert(entity_id, attribute_id, TripleValue::String("hello".into())).unwrap();
//!
//! // Commit with durability
//! txn.commit().unwrap();
//! ```

mod allocator;
pub mod btree;
pub mod buffer_pool;
pub mod checkpoint;
mod database;
mod file;
pub mod gc;
pub mod hlc;
pub mod indexes;
pub mod io;
pub mod overflow;
mod page;
pub mod recovery;
mod superblock;
pub mod time;
pub mod tombstone;
mod transaction;
pub mod wal;

pub use allocator::PageAllocator;
pub use buffer_pool::{BufferPool, DEFAULT_POOL_CAPACITY};
pub use checkpoint::{
    CheckpointConfig, CheckpointError, CheckpointResult, CheckpointState, force_checkpoint,
    maybe_checkpoint, perform_checkpoint,
};
pub use database::{Database, DatabaseError, GcStats, GcTickResult, Snapshot};
pub use file::{DatabaseFile, FileError};
pub use gc::{GcConfig, spawn_gc_task};
pub use hlc::{Clock as HlcClock, ClockError as HlcClockError};
pub use indexes::primary::{PrimaryIndex, PrimaryIndexError};
pub use io::{Storage, StorageError};
pub use page::{PAGE_SIZE, Page, PageError, PageHeader, PageId, PageType};
pub use recovery::{RecoveryError, RecoveryResult, needs_recovery, recover};
pub use superblock::{Superblock, SuperblockError};
pub use time::{SystemTimeSource, TimeSource};
pub use tombstone::{Tombstone, TombstoneError, TombstoneList};
pub use transaction::{Transaction, TransactionError};
pub use wal::{LogRecord, LogRecordPayload, LogRecordType, Lsn, Wal, WalError};

use crate::types::{ChangeNotification, ConnectionId};

/// A filtered receiver for change notifications.
///
/// This wraps a broadcast receiver and automatically filters out notifications
/// that originated from the subscriber's own connection. This ensures that
/// a connection never receives notifications for its own writes.
pub struct FilteredChangeReceiver {
    receiver: tokio::sync::broadcast::Receiver<ChangeNotification>,
    /// The connection ID to filter out (this connection's own ID).
    exclude_connection_id: ConnectionId,
}

impl FilteredChangeReceiver {
    /// Create a new filtered receiver.
    #[allow(clippy::missing_const_for_fn)] // broadcast::Receiver not const-compatible
    pub(crate) fn new(
        receiver: tokio::sync::broadcast::Receiver<ChangeNotification>,
        exclude_connection_id: ConnectionId,
    ) -> Self {
        Self {
            receiver,
            exclude_connection_id,
        }
    }

    /// Try to receive the next notification without blocking.
    ///
    /// Returns notifications from other connections only.
    /// Notifications from this connection are automatically skipped.
    pub fn try_recv(
        &mut self,
    ) -> Result<ChangeNotification, tokio::sync::broadcast::error::TryRecvError> {
        loop {
            let notification = self.receiver.try_recv()?;
            // Skip notifications from our own connection
            if notification.source_connection_id != self.exclude_connection_id {
                return Ok(notification);
            }
            // Continue looping to get the next notification
        }
    }

    /// Receive the next notification, waiting if necessary.
    ///
    /// Returns notifications from other connections only.
    /// Notifications from this connection are automatically skipped.
    pub async fn recv(
        &mut self,
    ) -> Result<ChangeNotification, tokio::sync::broadcast::error::RecvError> {
        loop {
            let notification = self.receiver.recv().await?;
            // Skip notifications from our own connection
            if notification.source_connection_id != self.exclude_connection_id {
                return Ok(notification);
            }
            // Continue looping to get the next notification
        }
    }
}
