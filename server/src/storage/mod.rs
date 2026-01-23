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
mod database;
mod file;
pub mod indexes;
mod page;
mod superblock;
mod transaction;
mod triple;
pub mod wal;

pub use allocator::PageAllocator;
pub use database::{Database, DatabaseError};
pub use file::{DatabaseFile, FileError};
pub use indexes::primary::{PrimaryIndex, PrimaryIndexError};
pub use page::{PAGE_SIZE, Page, PageError, PageHeader, PageId, PageType};
pub use superblock::{HlcTimestamp, Superblock, SuperblockError};
pub use transaction::{Transaction, TransactionError};
pub use triple::{AttributeId, EntityId, TripleError, TripleRecord, TripleValue, TxnId};
pub use wal::{LogRecord, LogRecordPayload, LogRecordType, Lsn, Wal, WalError};
