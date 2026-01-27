//! Database file I/O operations.
//!
//! This module handles reading and writing pages to the database file.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use crate::storage::buffer_pool::BufferPool;
use crate::storage::io::{Storage, StorageError};
use crate::storage::page::{PAGE_SIZE, PAGE_SIZE_U64, Page, PageId};
use crate::storage::superblock::{Superblock, SuperblockError};
use crate::storage::wal::{self, LogRecord, LogRecordPayload, Lsn, Wal, WalError};
use crate::types::HlcTimestamp;

/// A database file handle with low-level page I/O operations.
pub struct DatabaseFile {
    file: File,
    superblock: Superblock,
    buffer_pool: Arc<BufferPool>,
}

impl DatabaseFile {
    /// Create a new database file at the given path.
    ///
    /// Returns an error if the file already exists.
    /// The provided buffer pool is shared across all databases.
    pub fn create(path: &Path, buffer_pool: Arc<BufferPool>) -> Result<Self, FileError> {
        if path.exists() {
            return Err(FileError::AlreadyExists(path.to_path_buf()));
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(FileError::Io)?;

        // Initialize with a fresh superblock
        let superblock = Superblock::new();
        let page = superblock
            .to_page(&buffer_pool)
            .ok_or(FileError::BufferPoolExhausted)?;

        file.write_all(page.as_bytes()).map_err(FileError::Io)?;
        file.sync_all().map_err(FileError::Io)?;

        Ok(Self {
            file,
            superblock,
            buffer_pool,
        })
    }

    /// Open an existing database file.
    ///
    /// The provided buffer pool is shared across all databases.
    pub fn open(path: &Path, buffer_pool: Arc<BufferPool>) -> Result<Self, FileError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(FileError::Io)?;

        // Read and validate the superblock
        let mut buf = [0u8; PAGE_SIZE];
        file.read_exact(&mut buf).map_err(FileError::Io)?;

        // Create a page from the buffer to parse the superblock
        let page = buffer_pool
            .lease_page()
            .ok_or(FileError::BufferPoolExhausted)?;
        // Copy the read bytes into the page buffer
        let mut page = page;
        page.as_bytes_mut().copy_from_slice(&buf);

        let superblock = Superblock::from_page(&page).map_err(FileError::Superblock)?;

        Ok(Self {
            file,
            superblock,
            buffer_pool,
        })
    }

    /// Get a reference to the superblock.
    #[must_use]
    pub const fn superblock(&self) -> &Superblock {
        &self.superblock
    }

    /// Get a mutable reference to the superblock.
    pub const fn superblock_mut(&mut self) -> &mut Superblock {
        &mut self.superblock
    }

    /// Read a page from the file.
    ///
    /// Returns an error if page is out of bounds or buffer pool is exhausted.
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page, FileError> {
        if page_id >= self.superblock.total_page_count {
            return Err(FileError::PageOutOfBounds {
                page_id,
                total_pages: self.superblock.total_page_count,
            });
        }

        // Lease a page buffer from the pool
        let mut page = self
            .buffer_pool
            .lease_page()
            .ok_or(FileError::BufferPoolExhausted)?;

        let offset = page_id * PAGE_SIZE_U64;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(FileError::Io)?;

        self.file
            .read_exact(page.as_bytes_mut())
            .map_err(FileError::Io)?;

        Ok(page)
    }

    /// Read a page from the file using pread (position-independent).
    ///
    /// This method uses `pread` via `FileExt::read_exact_at()` which does not
    /// modify the file cursor position. This allows concurrent reads from
    /// multiple threads without requiring mutable access.
    ///
    /// Returns an error if page is out of bounds or buffer pool is exhausted.
    #[cfg(unix)]
    pub fn read_page_at(&self, page_id: PageId) -> Result<Page, FileError> {
        if page_id >= self.superblock.total_page_count {
            return Err(FileError::PageOutOfBounds {
                page_id,
                total_pages: self.superblock.total_page_count,
            });
        }

        // Lease a page buffer from the pool
        let mut page = self
            .buffer_pool
            .lease_page()
            .ok_or(FileError::BufferPoolExhausted)?;

        let offset = page_id * PAGE_SIZE_U64;
        self.file
            .read_exact_at(page.as_bytes_mut(), offset)
            .map_err(FileError::Io)?;

        Ok(page)
    }

    /// Write a page to the file.
    pub fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<(), FileError> {
        if page_id >= self.superblock.total_page_count {
            return Err(FileError::PageOutOfBounds {
                page_id,
                total_pages: self.superblock.total_page_count,
            });
        }

        let offset = page_id * PAGE_SIZE_U64;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(FileError::Io)?;

        self.file
            .write_all(page.as_bytes())
            .map_err(FileError::Io)?;

        Ok(())
    }

    /// Write the superblock to page 0.
    pub fn write_superblock(&mut self) -> Result<(), FileError> {
        let page = self
            .superblock
            .to_page(&self.buffer_pool)
            .ok_or(FileError::BufferPoolExhausted)?;

        self.file.seek(SeekFrom::Start(0)).map_err(FileError::Io)?;
        self.file
            .write_all(page.as_bytes())
            .map_err(FileError::Io)?;

        Ok(())
    }

    /// Get a reference to the buffer pool.
    #[must_use]
    pub const fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Allocate new pages at the end of the file.
    ///
    /// Returns the page ID of the first allocated page.
    pub fn allocate_pages(&mut self, count: u64) -> Result<PageId, FileError> {
        let first_new_page = self.superblock.total_page_count;

        // Extend the file
        let new_total = first_new_page + count;
        let new_size = new_total * PAGE_SIZE_U64;

        self.file.set_len(new_size).map_err(FileError::Io)?;

        // Update superblock
        self.superblock.total_page_count = new_total;
        self.superblock.file_size = new_size;

        Ok(first_new_page)
    }

    /// Sync all pending writes to disk.
    pub fn sync(&self) -> Result<(), FileError> {
        self.file.sync_all().map_err(FileError::Io)
    }

    /// Get the total number of pages in the file.
    #[must_use]
    pub const fn total_pages(&self) -> u64 {
        self.superblock.total_page_count
    }

    /// Initialize the WAL region in the database file.
    ///
    /// This allocates pages for the WAL and updates the superblock.
    /// Should only be called once when creating a new database.
    ///
    /// # Arguments
    /// - `capacity`: The desired WAL capacity in bytes (will be rounded up to page size)
    pub fn init_wal(&mut self, capacity: u64) -> Result<(), FileError> {
        let capacity = capacity.max(wal::MIN_WAL_CAPACITY);
        let wal_pages = wal::pages_for_capacity(capacity);
        let actual_capacity = wal_pages * PAGE_SIZE_U64;

        // Allocate pages for the WAL
        let first_wal_page = self.allocate_pages(wal_pages)?;

        // Calculate the byte offset where WAL region starts
        let wal_start_offset = first_wal_page * PAGE_SIZE_U64;

        // Update superblock with WAL information
        self.superblock.txn_log_start = wal_start_offset;
        self.superblock.txn_log_end = wal_start_offset; // head = start initially
        self.superblock.txn_log_capacity = actual_capacity;
        self.superblock.last_checkpoint_lsn = 0;

        // Write updated superblock
        Self::write_superblock(self)?;
        Self::sync(self)?;

        Ok(())
    }

    /// Check if the WAL has been initialized.
    #[must_use]
    pub const fn has_wal(&self) -> bool {
        self.superblock.txn_log_capacity > 0
    }

    /// Get the WAL capacity in bytes.
    #[must_use]
    pub const fn wal_capacity(&self) -> u64 {
        self.superblock.txn_log_capacity
    }

    /// Get a WAL handle for writing log records.
    ///
    /// Returns an error if the WAL has not been initialized.
    #[allow(clippy::missing_const_for_fn)] // Cannot be const due to early return with ?
    pub fn wal(&mut self) -> Result<Wal<'_, File>, WalError> {
        if !self.has_wal() {
            return Err(WalError::NotInitialized);
        }

        let region_start = self.superblock.txn_log_start;
        let capacity = self.superblock.txn_log_capacity;

        // head is stored as absolute file offset, convert to relative
        let head = self.superblock.txn_log_end - region_start;

        // For now, tail starts at 0 (we'll add proper tail tracking later)
        // In a full implementation, we'd track this in the superblock or scan on open
        let tail = 0;

        // Next LSN is last WAL LSN + 1 (or 1 if no writes yet)
        let next_lsn = if self.superblock.last_wal_lsn > 0 {
            self.superblock.last_wal_lsn + 1
        } else {
            1
        };

        Ok(Wal::new(
            &mut self.file,
            region_start,
            capacity,
            head,
            tail,
            next_lsn,
        ))
    }

    /// Update the WAL head position in the superblock.
    ///
    /// This should be called after appending records to persist the new head position.
    pub const fn update_wal_head(&mut self, relative_head: u64, last_lsn: Lsn) {
        let absolute_head = self.superblock.txn_log_start + relative_head;
        self.superblock.txn_log_end = absolute_head;
        self.superblock.last_wal_lsn = last_lsn;
    }

    /// Get mutable access to the underlying file handle.
    ///
    /// This is needed for WAL operations that need direct file access.
    pub const fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

/// Errors that can occur during file operations.
#[derive(Debug)]
pub enum FileError {
    /// I/O error.
    Io(std::io::Error),
    /// File already exists.
    AlreadyExists(std::path::PathBuf),
    /// Superblock error.
    Superblock(SuperblockError),
    /// Page ID out of bounds.
    PageOutOfBounds { page_id: PageId, total_pages: u64 },
    /// Buffer pool exhausted.
    BufferPoolExhausted,
}

impl std::fmt::Display for FileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::AlreadyExists(p) => write!(f, "file already exists: {}", p.display()),
            Self::Superblock(e) => write!(f, "superblock error: {e}"),
            Self::PageOutOfBounds {
                page_id,
                total_pages,
            } => {
                write!(
                    f,
                    "page {page_id} out of bounds (total pages: {total_pages})"
                )
            }
            Self::BufferPoolExhausted => write!(f, "buffer pool exhausted"),
        }
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Superblock(e) => Some(e),
            Self::AlreadyExists(_) | Self::PageOutOfBounds { .. } | Self::BufferPoolExhausted => {
                None
            }
        }
    }
}

impl From<FileError> for StorageError {
    fn from(e: FileError) -> Self {
        match e {
            FileError::Io(io_err) => Self::Io(io_err),
            FileError::PageOutOfBounds {
                page_id,
                total_pages,
            } => Self::PageOutOfBounds {
                page_id,
                total_pages,
            },
            FileError::AlreadyExists(path) => Self::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("file already exists: {}", path.display()),
            )),
            FileError::Superblock(e) => Self::Superblock(e.to_string()),
            FileError::BufferPoolExhausted => Self::BufferPoolExhausted,
        }
    }
}

impl Storage for DatabaseFile {
    fn buffer_pool(&self) -> &Arc<BufferPool> {
        Self::buffer_pool(self)
    }

    fn read_page(&mut self, page_id: PageId) -> Result<Page, StorageError> {
        Self::read_page(self, page_id).map_err(StorageError::from)
    }

    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<(), StorageError> {
        Self::write_page(self, page_id, page).map_err(StorageError::from)
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        Self::sync(self).map_err(StorageError::from)
    }

    fn allocate_pages(&mut self, count: u64) -> Result<PageId, StorageError> {
        Self::allocate_pages(self, count).map_err(StorageError::from)
    }

    fn total_pages(&self) -> u64 {
        Self::total_pages(self)
    }

    fn superblock(&self) -> &Superblock {
        Self::superblock(self)
    }

    fn superblock_mut(&mut self) -> &mut Superblock {
        Self::superblock_mut(self)
    }

    fn write_superblock(&mut self) -> Result<(), StorageError> {
        Self::write_superblock(self).map_err(StorageError::from)
    }

    fn has_wal(&self) -> bool {
        Self::has_wal(self)
    }

    fn init_wal(&mut self, capacity: u64) -> Result<(), StorageError> {
        Self::init_wal(self, capacity).map_err(StorageError::from)
    }

    fn wal_append(
        &mut self,
        txn_id: u64,
        hlc: HlcTimestamp,
        payload: LogRecordPayload,
    ) -> Result<Lsn, StorageError> {
        let (lsn, head, last_lsn) = {
            let mut wal = self.wal()?;
            let lsn = wal.append(txn_id, hlc, payload)?;
            (lsn, wal.head(), wal.last_lsn())
        };
        self.update_wal_head(head, last_lsn);
        Ok(lsn)
    }

    fn wal_sync(&mut self) -> Result<(), StorageError> {
        let mut wal = self.wal()?;
        wal.sync()?;
        Ok(())
    }

    fn wal_read_all(&mut self) -> Result<Vec<LogRecord>, StorageError> {
        let mut wal = self.wal()?;
        Ok(wal.read_all()?)
    }

    fn wal_changes_since(&mut self, since: HlcTimestamp) -> Result<Vec<LogRecord>, StorageError> {
        let mut wal = self.wal()?;
        Ok(wal.changes_since(since)?)
    }

    fn wal_next_lsn(&self) -> Result<Lsn, StorageError> {
        if !self.has_wal() {
            return Err(StorageError::WalNotInitialized);
        }
        // Next LSN is last WAL LSN + 1 (or 1 if no writes yet)
        if self.superblock.last_wal_lsn > 0 {
            Ok(self.superblock.last_wal_lsn + 1)
        } else {
            Ok(1)
        }
    }

    fn wal_head(&self) -> u64 {
        if self.superblock.txn_log_capacity == 0 {
            0
        } else {
            self.superblock.txn_log_end - self.superblock.txn_log_start
        }
    }

    fn wal_last_lsn(&self) -> Lsn {
        self.superblock.last_wal_lsn
    }

    fn set_checkpoint_lsn(&mut self, lsn: Lsn) {
        self.superblock.last_checkpoint_lsn = lsn;
    }

    fn set_checkpoint_hlc(&mut self, hlc: HlcTimestamp) {
        self.superblock.last_checkpoint_hlc = hlc;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_pool::BufferPool;
    use std::fs;
    use tempfile::tempdir;

    /// Create a test buffer pool with reasonable capacity for tests.
    fn test_pool() -> Arc<BufferPool> {
        BufferPool::new(100)
    }

    #[test]
    fn test_create_and_open() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        let pool = test_pool();

        // Create a new database
        {
            let db = DatabaseFile::create(&path, Arc::clone(&pool)).expect("create db");
            assert_eq!(db.total_pages(), 1);
            assert_eq!(db.superblock().next_txn_id, 1);
        }

        // Reopen it
        {
            let db = DatabaseFile::open(&path, Arc::clone(&pool)).expect("open db");
            assert_eq!(db.total_pages(), 1);
            assert_eq!(db.superblock().next_txn_id, 1);
        }
    }

    #[test]
    fn test_create_already_exists() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        let pool = test_pool();

        // Create the file
        fs::write(&path, b"existing").expect("write file");

        // Try to create database - should fail
        let result = DatabaseFile::create(&path, pool);
        assert!(matches!(result, Err(FileError::AlreadyExists(_))));
    }

    #[test]
    fn test_allocate_and_write_pages() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        let pool = test_pool();

        let mut db = DatabaseFile::create(&path, pool).expect("create db");

        // Allocate 5 new pages
        let first_page = db.allocate_pages(5).expect("allocate");
        assert_eq!(first_page, 1); // Page 0 is superblock
        assert_eq!(db.total_pages(), 6);

        // Write to a page
        let mut page = db.buffer_pool().lease_page_zeroed().expect("lease page");
        page.write_bytes(0, b"hello world");
        db.write_page(3, &page).expect("write page");

        // Read it back
        let read_page = db.read_page(3).expect("read page");
        assert_eq!(read_page.read_bytes(0, 11), b"hello world");
    }

    #[test]
    fn test_page_out_of_bounds() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        let pool = test_pool();

        let mut db = DatabaseFile::create(&path, pool).expect("create db");

        // Try to read page that doesn't exist
        let result = db.read_page(100);
        assert!(matches!(result, Err(FileError::PageOutOfBounds { .. })));
    }

    #[test]
    fn test_superblock_persistence() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        let pool = test_pool();

        // Create and modify superblock
        {
            let mut db = DatabaseFile::create(&path, Arc::clone(&pool)).expect("create db");
            db.superblock_mut().next_txn_id = 42;
            db.superblock_mut().primary_index_root = 5;
            db.write_superblock().expect("write superblock");
            db.sync().expect("sync");
        }

        // Reopen and verify
        {
            let db = DatabaseFile::open(&path, pool).expect("open db");
            assert_eq!(db.superblock().next_txn_id, 42);
            assert_eq!(db.superblock().primary_index_root, 5);
        }
    }

    #[test]
    fn test_page_data_persistence() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        let pool = test_pool();

        // Write data
        {
            let mut db = DatabaseFile::create(&path, Arc::clone(&pool)).expect("create db");
            db.allocate_pages(2).expect("allocate");

            let mut page = db.buffer_pool().lease_page_zeroed().expect("lease page");
            page.write_u64(100, 0xDEAD_BEEF_CAFE_BABE);
            db.write_page(1, &page).expect("write");
            db.write_superblock().expect("write superblock");
            db.sync().expect("sync");
        }

        // Read back
        {
            let mut db = DatabaseFile::open(&path, pool).expect("open db");
            let page = db.read_page(1).expect("read");
            assert_eq!(page.read_u64(100), 0xDEAD_BEEF_CAFE_BABE);
        }
    }
}
