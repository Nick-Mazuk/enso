//! Storage abstraction for deterministic simulation testing.
//!
//! This module provides a `Storage` trait that abstracts over page-based storage
//! operations, allowing the system to use real file I/O in production and simulated
//! in-memory storage in tests.
//!
//! # Design
//!
//! The trait is designed to be a minimal abstraction over the core storage operations:
//! - Page read/write operations
//! - File synchronization
//! - Page allocation
//! - Superblock management
//! - WAL operations
//!
//! This allows for deterministic simulation testing where we can:
//! - Inject faults at the storage level
//! - Control exactly when writes become durable
//! - Simulate partial writes and corruption

use std::sync::Arc;

use crate::storage::buffer_pool::BufferPool;
use crate::storage::page::{Page, PageId};
use crate::storage::superblock::Superblock;
use crate::storage::wal::{LogRecord, LogRecordPayload, Lsn, WalError};
use crate::types::HlcTimestamp;

/// Errors that can occur during storage operations.
#[derive(Debug)]
pub enum StorageError {
    /// I/O error.
    Io(std::io::Error),
    /// Page out of bounds.
    PageOutOfBounds { page_id: PageId, total_pages: u64 },
    /// Superblock error.
    Superblock(String),
    /// WAL not initialized.
    WalNotInitialized,
    /// WAL error.
    Wal(WalError),
    /// Injected fault for simulation.
    InjectedFault(String),
    /// Corruption detected.
    Corruption(String),
    /// Buffer pool exhausted - no buffers available.
    BufferPoolExhausted,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::PageOutOfBounds {
                page_id,
                total_pages,
            } => write!(
                f,
                "page {page_id} out of bounds (total pages: {total_pages})"
            ),
            Self::Superblock(e) => write!(f, "superblock error: {e}"),
            Self::WalNotInitialized => write!(f, "WAL not initialized"),
            Self::Wal(e) => write!(f, "WAL error: {e}"),
            Self::InjectedFault(msg) => write!(f, "injected fault: {msg}"),
            Self::Corruption(msg) => write!(f, "corruption: {msg}"),
            Self::BufferPoolExhausted => write!(f, "buffer pool exhausted"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Wal(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<WalError> for StorageError {
    fn from(e: WalError) -> Self {
        Self::Wal(e)
    }
}

/// Abstraction over page-based storage operations.
///
/// This trait allows swapping between real file storage and simulated in-memory
/// storage for deterministic testing.
///
/// # Implementation Notes
///
/// Implementations must ensure:
/// - `read_page` returns the last written content for a page
/// - `sync` makes all previous writes durable
/// - `allocate_pages` extends the storage capacity
/// - Superblock changes are persisted on `write_superblock` + `sync`
pub trait Storage {
    // ========== Buffer Pool ==========

    /// Get a reference to the buffer pool.
    ///
    /// The buffer pool is used to lease page buffers for read and write operations.
    fn buffer_pool(&self) -> &Arc<BufferPool>;

    // ========== Page Operations ==========

    /// Read a page from storage.
    ///
    /// Returns the page content at the given page ID.
    /// Returns an error if the page ID is out of bounds or buffer pool exhausted.
    fn read_page(&mut self, page_id: PageId) -> Result<Page, StorageError>;

    /// Write a page to storage.
    ///
    /// The write may be buffered until `sync` is called.
    /// Returns an error if the page ID is out of bounds.
    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<(), StorageError>;

    /// Sync all pending writes to durable storage.
    fn sync(&mut self) -> Result<(), StorageError>;

    /// Allocate new pages at the end of storage.
    ///
    /// Returns the page ID of the first allocated page.
    fn allocate_pages(&mut self, count: u64) -> Result<PageId, StorageError>;

    /// Get the total number of pages in storage.
    fn total_pages(&self) -> u64;

    // ========== Superblock Operations ==========

    /// Get a reference to the superblock.
    fn superblock(&self) -> &Superblock;

    /// Get a mutable reference to the superblock.
    fn superblock_mut(&mut self) -> &mut Superblock;

    /// Write the superblock to storage.
    ///
    /// The write may be buffered until `sync` is called.
    fn write_superblock(&mut self) -> Result<(), StorageError>;

    // ========== WAL Operations ==========

    /// Check if the WAL has been initialized.
    fn has_wal(&self) -> bool;

    /// Initialize the WAL region.
    ///
    /// Allocates pages for the WAL and updates the superblock.
    /// Should only be called once when creating a new database.
    fn init_wal(&mut self, capacity: u64) -> Result<(), StorageError>;

    /// Append a record to the WAL.
    ///
    /// Returns the LSN assigned to the record.
    fn wal_append(
        &mut self,
        txn_id: u64,
        hlc: HlcTimestamp,
        payload: LogRecordPayload,
    ) -> Result<Lsn, StorageError>;

    /// Sync the WAL to durable storage.
    fn wal_sync(&mut self) -> Result<(), StorageError>;

    /// Read all WAL records.
    fn wal_read_all(&mut self) -> Result<Vec<LogRecord>, StorageError>;

    /// Get changes since a given HLC timestamp.
    fn wal_changes_since(&mut self, since: HlcTimestamp) -> Result<Vec<LogRecord>, StorageError>;

    /// Get the next LSN that will be assigned.
    fn wal_next_lsn(&self) -> Result<Lsn, StorageError>;

    /// Get the current WAL head position (relative offset).
    fn wal_head(&self) -> u64;

    /// Get the last assigned LSN.
    fn wal_last_lsn(&self) -> Lsn;

    /// Update the checkpoint LSN in the superblock.
    fn set_checkpoint_lsn(&mut self, lsn: Lsn);

    /// Update the checkpoint HLC in the superblock.
    fn set_checkpoint_hlc(&mut self, hlc: HlcTimestamp);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_error_display() {
        let e = StorageError::PageOutOfBounds {
            page_id: 10,
            total_pages: 5,
        };
        assert!(e.to_string().contains("page 10"));
        assert!(e.to_string().contains("total pages: 5"));

        let e = StorageError::InjectedFault("test fault".to_string());
        assert!(e.to_string().contains("test fault"));
    }
}
