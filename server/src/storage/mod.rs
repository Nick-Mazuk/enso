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
//! use server::storage::{DatabaseFile, Transaction, TripleValue};
//! use std::path::Path;
//!
//! // Create a new database
//! let path = Path::new("/tmp/my_database");
//! let mut db = DatabaseFile::create(path).unwrap();
//!
//! // Begin a transaction
//! let mut txn = Transaction::begin(&mut db).unwrap();
//!
//! // Insert a triple
//! let entity_id = [1u8; 16];
//! let attribute_id = [2u8; 16];
//! txn.insert(entity_id, attribute_id, TripleValue::String("hello".into())).unwrap();
//!
//! // Commit with durability
//! txn.commit().unwrap();
//! ```

mod allocator;
pub mod btree;
pub mod checkpoint;
mod database;
mod file;
pub mod hlc;
pub mod indexes;
pub mod io;
pub mod overflow;
mod page;
pub mod recovery;
mod superblock;
pub mod time;
mod transaction;
mod triple;
pub mod wal;

pub use allocator::PageAllocator;
pub use checkpoint::{
    CheckpointConfig, CheckpointError, CheckpointResult, CheckpointState, force_checkpoint,
    maybe_checkpoint, perform_checkpoint,
};
pub use database::{Database, DatabaseError, GcResult, Snapshot};
pub use file::{DatabaseFile, FileError};
pub use hlc::{Clock as HlcClock, ClockError as HlcClockError};
pub use indexes::primary::{PrimaryIndex, PrimaryIndexError};
pub use io::{Storage, StorageError};
pub use page::{PAGE_SIZE, Page, PageError, PageHeader, PageId, PageType};
pub use recovery::{RecoveryError, RecoveryResult, needs_recovery, recover};
pub use superblock::{HlcTimestamp, Superblock, SuperblockError};
pub use time::{SystemTimeSource, TimeSource};
pub use transaction::{Transaction, TransactionError};
pub use triple::{AttributeId, EntityId, TripleError, TripleRecord, TripleValue, TxnId};
pub use wal::{LogRecord, LogRecordPayload, LogRecordType, Lsn, Wal, WalError};

// =============================================================================
// Change Notification Types
// =============================================================================

/// Type of change to a triple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    /// A new triple was created.
    Insert,
    /// An existing triple was modified with a newer HLC.
    Update,
    /// A triple was removed.
    Delete,
}

/// A record of a single triple change.
#[derive(Debug, Clone)]
#[allow(clippy::disallowed_methods)] // Clone needed for broadcast channel
pub struct ChangeRecord {
    /// The type of change.
    pub change_type: ChangeType,
    /// The entity ID of the affected triple.
    pub entity_id: EntityId,
    /// The attribute ID of the affected triple.
    pub attribute_id: AttributeId,
    /// The value of the triple. `None` for Delete operations.
    pub value: Option<TripleValue>,
    /// The HLC timestamp of the change.
    pub hlc: HlcTimestamp,
}

/// Notification of changes, broadcast to all subscribers.
///
/// This is sent via the broadcast channel when triples are modified.
/// Subscribers receive this and can convert to protocol-specific formats.
#[derive(Debug, Clone)]
#[allow(clippy::disallowed_methods)] // Clone needed for broadcast channel
pub struct ChangeNotification {
    /// The changes that occurred in this transaction.
    pub changes: Vec<ChangeRecord>,
}
