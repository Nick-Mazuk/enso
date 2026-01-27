//! Overflow page management for large values.
//!
//! Values larger than `MAX_INLINE_VALUE_SIZE` (1024 bytes) are stored in
//! overflow pages. Each overflow page can hold up to ~8KB of data, and
//! pages are chained together for values larger than a single page.
//!
//! # Overflow Page Format
//!
//! ```text
//! +----------------+----------------+----------------+------------------+
//! | Page Header    | Next Page ID   | Data Length    | Data...          |
//! | (8 bytes)      | (8 bytes)      | (4 bytes)      | (variable)       |
//! +----------------+----------------+----------------+------------------+
//! ```
//!
//! # Overflow Reference Format
//!
//! When a value is stored in overflow pages, the B-tree leaf stores an
//! "overflow reference" instead of the actual value:
//!
//! ```text
//! +----------------+----------------+----------------+
//! | Marker (0xFF)  | First Page ID  | Total Length   |
//! | (1 byte)       | (8 bytes)      | (4 bytes)      |
//! +----------------+----------------+----------------+
//! ```

use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::page::{PAGE_SIZE, PageHeader, PageId, PageType};

/// Size of overflow page header (after page header).
/// - Next page ID: 8 bytes
/// - Data length in this page: 4 bytes
const OVERFLOW_HEADER_SIZE: usize = 12;

/// Offset where overflow data starts.
const OVERFLOW_DATA_OFFSET: usize = PageHeader::SIZE + OVERFLOW_HEADER_SIZE;

/// Maximum data per overflow page.
pub const OVERFLOW_DATA_PER_PAGE: usize = PAGE_SIZE - OVERFLOW_DATA_OFFSET;

/// Marker byte indicating an overflow reference.
pub const OVERFLOW_MARKER: u8 = 0xFF;

/// Size of an overflow reference stored in leaf nodes.
/// - Marker: 1 byte
/// - Page ID: 8 bytes
/// - Total length: 4 bytes
pub const OVERFLOW_REF_SIZE: usize = 13;

/// An overflow reference stored in a B-tree leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverflowRef {
    /// First page of the overflow chain.
    pub first_page: PageId,
    /// Total length of the value.
    pub total_length: u32,
}

impl OverflowRef {
    /// Create a new overflow reference.
    #[must_use]
    pub const fn new(first_page: PageId, total_length: u32) -> Self {
        Self {
            first_page,
            total_length,
        }
    }

    /// Serialize the overflow reference to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; OVERFLOW_REF_SIZE] {
        let mut buf = [0u8; OVERFLOW_REF_SIZE];
        buf[0] = OVERFLOW_MARKER;
        buf[1..9].copy_from_slice(&self.first_page.to_le_bytes());
        buf[9..13].copy_from_slice(&self.total_length.to_le_bytes());
        buf
    }

    /// Deserialize an overflow reference from bytes.
    ///
    /// Returns `None` if the bytes don't start with the overflow marker.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < OVERFLOW_REF_SIZE || bytes[0] != OVERFLOW_MARKER {
            return None;
        }

        let first_page = u64::from_le_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let total_length = u32::from_le_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]);

        Some(Self {
            first_page,
            total_length,
        })
    }

    /// Check if a value starts with the overflow marker.
    #[must_use]
    pub fn is_overflow_ref(bytes: &[u8]) -> bool {
        bytes.first() == Some(&OVERFLOW_MARKER)
    }
}

/// Write a large value to overflow pages.
///
/// Allocates one or more overflow pages and writes the value.
/// Returns an overflow reference that can be stored in the B-tree leaf.
pub fn write_overflow(file: &mut DatabaseFile, value: &[u8]) -> Result<OverflowRef, OverflowError> {
    if value.is_empty() {
        return Err(OverflowError::EmptyValue);
    }

    let total_length = value.len();
    let mut remaining = value;
    let mut first_page = 0;
    let mut prev_page_id = 0;

    while !remaining.is_empty() {
        // Allocate a new overflow page
        let page_id = file.allocate_pages(1)?;

        // Determine how much data goes in this page
        let chunk_size = remaining.len().min(OVERFLOW_DATA_PER_PAGE);
        let (chunk, rest) = remaining.split_at(chunk_size);
        remaining = rest;

        // Create the overflow page
        let mut page = file
            .buffer_pool()
            .lease_page_zeroed()
            .ok_or(OverflowError::File(FileError::BufferPoolExhausted))?;

        // Write page header
        let header = PageHeader {
            page_type: PageType::Overflow,
            flags: 0,
            checksum: 0, // Will be computed later if needed
        };
        page.write_bytes(0, &header.to_bytes());

        // Write overflow header
        // Next page: 0 for now, will update if there's a next page
        page.write_u64(PageHeader::SIZE, 0);
        // Data length
        #[allow(clippy::cast_possible_truncation)]
        page.write_u32(PageHeader::SIZE + 8, chunk_size as u32);

        // Write data
        page.write_bytes(OVERFLOW_DATA_OFFSET, chunk);

        // Write the page
        file.write_page(page_id, &page)?;

        // Track first page
        if first_page == 0 {
            first_page = page_id;
        }

        // Link previous page to this one
        if prev_page_id != 0 {
            let mut prev_page = file.read_page(prev_page_id)?;
            prev_page.write_u64(PageHeader::SIZE, page_id);
            file.write_page(prev_page_id, &prev_page)?;
        }

        prev_page_id = page_id;
    }

    #[allow(clippy::cast_possible_truncation)]
    Ok(OverflowRef::new(first_page, total_length as u32))
}

/// Read a large value from overflow pages.
///
/// Follows the overflow page chain and reconstructs the full value.
pub fn read_overflow(
    file: &mut DatabaseFile,
    overflow_ref: &OverflowRef,
) -> Result<Vec<u8>, OverflowError> {
    let mut result = Vec::with_capacity(overflow_ref.total_length as usize);
    let mut current_page_id = overflow_ref.first_page;

    while current_page_id != 0 {
        let page = file.read_page(current_page_id)?;

        // Verify page type
        let page_type = page.read_u8(0);
        if page_type != PageType::Overflow as u8 {
            return Err(OverflowError::InvalidPageType(page_type));
        }

        // Read overflow header
        let next_page = page.read_u64(PageHeader::SIZE);
        let data_length = page.read_u32(PageHeader::SIZE + 8) as usize;

        // Read data
        let data = page.read_bytes(OVERFLOW_DATA_OFFSET, data_length);
        result.extend_from_slice(data);

        current_page_id = next_page;
    }

    if result.len() != overflow_ref.total_length as usize {
        return Err(OverflowError::LengthMismatch {
            expected: overflow_ref.total_length as usize,
            actual: result.len(),
        });
    }

    Ok(result)
}

/// Free overflow pages.
///
/// Follows the overflow page chain and marks pages as free.
/// Note: Currently this just counts the pages without actually freeing them.
/// The pages become orphaned and can be reclaimed by a future compaction process.
pub fn free_overflow(
    file: &mut DatabaseFile,
    overflow_ref: &OverflowRef,
) -> Result<u64, OverflowError> {
    let mut current_page_id = overflow_ref.first_page;
    let mut pages_counted = 0u64;

    while current_page_id != 0 {
        let page = file.read_page(current_page_id)?;

        // Read next page
        let next_page = page.read_u64(PageHeader::SIZE);

        // Mark the page as free by writing a Free page type
        // This makes the pages identifiable for future reclamation
        let mut free_page = file
            .buffer_pool()
            .lease_page_zeroed()
            .ok_or(OverflowError::File(FileError::BufferPoolExhausted))?;
        let header = PageHeader {
            page_type: PageType::Free,
            flags: 0,
            checksum: 0,
        };
        free_page.write_bytes(0, &header.to_bytes());
        // Preserve the chain pointer so reclamation can follow the chain
        free_page.write_u64(PageHeader::SIZE, next_page);
        file.write_page(current_page_id, &free_page)?;

        pages_counted += 1;
        current_page_id = next_page;
    }

    Ok(pages_counted)
}

/// Errors that can occur during overflow operations.
#[derive(Debug)]
pub enum OverflowError {
    /// File operation failed.
    File(FileError),
    /// Empty value (overflow not needed).
    EmptyValue,
    /// Invalid page type encountered.
    InvalidPageType(u8),
    /// Length mismatch when reading.
    LengthMismatch { expected: usize, actual: usize },
}

impl std::fmt::Display for OverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(e) => write!(f, "file error: {e}"),
            Self::EmptyValue => write!(f, "empty value cannot use overflow"),
            Self::InvalidPageType(t) => write!(f, "invalid page type in overflow chain: 0x{t:02x}"),
            Self::LengthMismatch { expected, actual } => {
                write!(
                    f,
                    "overflow length mismatch: expected {expected}, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for OverflowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(e) => Some(e),
            _ => None,
        }
    }
}

impl From<FileError> for OverflowError {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_pool::BufferPool;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn test_pool() -> Arc<BufferPool> {
        BufferPool::new(100)
    }

    fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_overflow_ref_roundtrip() {
        let overflow_ref = OverflowRef::new(12345, 67890);
        let bytes = overflow_ref.to_bytes();

        assert_eq!(bytes[0], OVERFLOW_MARKER);

        let restored = OverflowRef::from_bytes(&bytes).expect("should parse");
        assert_eq!(restored.first_page, 12345);
        assert_eq!(restored.total_length, 67890);
    }

    #[test]
    fn test_overflow_ref_detection() {
        let overflow_ref = OverflowRef::new(1, 100);
        let bytes = overflow_ref.to_bytes();

        assert!(OverflowRef::is_overflow_ref(&bytes));
        assert!(!OverflowRef::is_overflow_ref(&[0x00, 0x01, 0x02]));
        assert!(!OverflowRef::is_overflow_ref(&[]));
    }

    #[test]
    fn test_overflow_single_page() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut file = DatabaseFile::create(&path, pool).expect("create db");

        // Value that fits in one overflow page
        let value = vec![0xABu8; 2048];
        let overflow_ref = write_overflow(&mut file, &value).expect("write overflow");

        assert_eq!(overflow_ref.total_length, 2048);

        let restored = read_overflow(&mut file, &overflow_ref).expect("read overflow");
        assert_eq!(restored, value);
    }

    #[test]
    fn test_overflow_multiple_pages() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut file = DatabaseFile::create(&path, pool).expect("create db");

        // Value that spans multiple overflow pages
        let value = vec![0xCDu8; 20000]; // ~3 pages needed
        let overflow_ref = write_overflow(&mut file, &value).expect("write overflow");

        assert_eq!(overflow_ref.total_length, 20000);

        let restored = read_overflow(&mut file, &overflow_ref).expect("read overflow");
        assert_eq!(restored, value);
    }

    #[test]
    fn test_overflow_free() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut file = DatabaseFile::create(&path, pool).expect("create db");

        // Write a value that spans multiple pages
        let value = vec![0xEFu8; 20000];
        let overflow_ref = write_overflow(&mut file, &value).expect("write overflow");

        // Free the overflow pages
        let pages_freed = free_overflow(&mut file, &overflow_ref).expect("free overflow");
        assert!(pages_freed >= 3); // At least 3 pages for 20KB
    }

    #[test]
    fn test_overflow_exact_page_boundary() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut file = DatabaseFile::create(&path, pool).expect("create db");

        // Value that exactly fills one page
        let value = vec![0x11u8; OVERFLOW_DATA_PER_PAGE];
        let overflow_ref = write_overflow(&mut file, &value).expect("write overflow");

        let restored = read_overflow(&mut file, &overflow_ref).expect("read overflow");
        assert_eq!(restored, value);
    }

    #[test]
    fn test_overflow_variable_data() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut file = DatabaseFile::create(&path, pool).expect("create db");

        // Value with varying byte patterns
        let mut value = Vec::with_capacity(15000);
        for i in 0..15000u32 {
            #[allow(clippy::cast_possible_truncation)]
            value.push((i % 256) as u8);
        }

        let overflow_ref = write_overflow(&mut file, &value).expect("write overflow");
        let restored = read_overflow(&mut file, &overflow_ref).expect("read overflow");
        assert_eq!(restored, value);
    }
}
