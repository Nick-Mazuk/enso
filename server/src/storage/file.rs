//! Database file I/O operations.
//!
//! This module handles reading and writing pages to the database file.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::storage::page::{PAGE_SIZE, PAGE_SIZE_U64, Page, PageId};
use crate::storage::superblock::{Superblock, SuperblockError};

/// A database file handle with low-level page I/O operations.
pub struct DatabaseFile {
    file: File,
    superblock: Superblock,
}

impl DatabaseFile {
    /// Create a new database file at the given path.
    ///
    /// Returns an error if the file already exists.
    pub fn create(path: &Path) -> Result<Self, FileError> {
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
        let page = superblock.to_page();

        file.write_all(page.as_bytes()).map_err(FileError::Io)?;
        file.sync_all().map_err(FileError::Io)?;

        Ok(Self { file, superblock })
    }

    /// Open an existing database file.
    pub fn open(path: &Path) -> Result<Self, FileError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(FileError::Io)?;

        // Read and validate the superblock
        let mut buf = [0u8; PAGE_SIZE];
        file.read_exact(&mut buf).map_err(FileError::Io)?;

        let page = Page::from_bytes(buf);
        let superblock = Superblock::from_page(&page).map_err(FileError::Superblock)?;

        Ok(Self { file, superblock })
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
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page, FileError> {
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

        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf).map_err(FileError::Io)?;

        Ok(Page::from_bytes(buf))
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
        let page = self.superblock.to_page();

        self.file.seek(SeekFrom::Start(0)).map_err(FileError::Io)?;
        self.file
            .write_all(page.as_bytes())
            .map_err(FileError::Io)?;

        Ok(())
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
        }
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Superblock(e) => Some(e),
            Self::AlreadyExists(_) | Self::PageOutOfBounds { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_create_and_open() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        // Create a new database
        {
            let db = DatabaseFile::create(&path).expect("create db");
            assert_eq!(db.total_pages(), 1);
            assert_eq!(db.superblock().next_txn_id, 1);
        }

        // Reopen it
        {
            let db = DatabaseFile::open(&path).expect("open db");
            assert_eq!(db.total_pages(), 1);
            assert_eq!(db.superblock().next_txn_id, 1);
        }
    }

    #[test]
    fn test_create_already_exists() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        // Create the file
        fs::write(&path, b"existing").expect("write file");

        // Try to create database - should fail
        let result = DatabaseFile::create(&path);
        assert!(matches!(result, Err(FileError::AlreadyExists(_))));
    }

    #[test]
    fn test_allocate_and_write_pages() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        let mut db = DatabaseFile::create(&path).expect("create db");

        // Allocate 5 new pages
        let first_page = db.allocate_pages(5).expect("allocate");
        assert_eq!(first_page, 1); // Page 0 is superblock
        assert_eq!(db.total_pages(), 6);

        // Write to a page
        let mut page = Page::new();
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

        let mut db = DatabaseFile::create(&path).expect("create db");

        // Try to read page that doesn't exist
        let result = db.read_page(100);
        assert!(matches!(result, Err(FileError::PageOutOfBounds { .. })));
    }

    #[test]
    fn test_superblock_persistence() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        // Create and modify superblock
        {
            let mut db = DatabaseFile::create(&path).expect("create db");
            db.superblock_mut().next_txn_id = 42;
            db.superblock_mut().primary_index_root = 5;
            db.write_superblock().expect("write superblock");
            db.sync().expect("sync");
        }

        // Reopen and verify
        {
            let db = DatabaseFile::open(&path).expect("open db");
            assert_eq!(db.superblock().next_txn_id, 42);
            assert_eq!(db.superblock().primary_index_root, 5);
        }
    }

    #[test]
    fn test_page_data_persistence() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        // Write data
        {
            let mut db = DatabaseFile::create(&path).expect("create db");
            db.allocate_pages(2).expect("allocate");

            let mut page = Page::new();
            page.write_u64(100, 0xDEAD_BEEF_CAFE_BABE);
            db.write_page(1, &page).expect("write");
            db.write_superblock().expect("write superblock");
            db.sync().expect("sync");
        }

        // Read back
        {
            let mut db = DatabaseFile::open(&path).expect("open db");
            let page = db.read_page(1).expect("read");
            assert_eq!(page.read_u64(100), 0xDEAD_BEEF_CAFE_BABE);
        }
    }
}
