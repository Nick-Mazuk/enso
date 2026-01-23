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
//! use storage::{DatabaseFile, PageAllocator};
//!
//! // Create a new database
//! let mut db = DatabaseFile::create(path)?;
//!
//! // Allocate pages for data
//! let page_id = db.allocate_pages(1)?;
//!
//! // Write data to a page
//! let mut page = Page::new();
//! page.write_bytes(0, b"hello");
//! db.write_page(page_id, &page)?;
//!
//! // Sync to disk
//! db.sync()?;
//! ```

mod allocator;
mod file;
mod page;
mod superblock;

pub use allocator::PageAllocator;
pub use file::{DatabaseFile, FileError};
pub use page::{Page, PageError, PageHeader, PageId, PageType, PAGE_SIZE};
pub use superblock::{HlcTimestamp, Superblock, SuperblockError};
