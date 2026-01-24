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
//! ```ignore
//! use storage::{DatabaseFile, Transaction, TripleValue};
//!
//! // Create a new database
//! let mut db = DatabaseFile::create(path)?;
//!
//! // Begin a transaction
//! let mut txn = Transaction::begin(&mut db)?;
//!
//! // Insert a triple
//! let entity_id = [1u8; 16];
//! let attribute_id = [2u8; 16];
//! txn.insert(entity_id, attribute_id, TripleValue::String("hello".into()))?;
//!
//! // Commit with durability
//! txn.commit()?;
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
