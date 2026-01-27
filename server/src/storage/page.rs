//! Page types and constants for the storage engine.
//!
//! The storage engine uses 8KB pages as the fundamental unit of I/O.
//!
//! Pages are allocated from a buffer pool to reduce heap allocation overhead.
//! When a page is dropped, its buffer is automatically returned to the pool.

use std::sync::Arc;

use crate::storage::buffer_pool::BufferPool;

/// Page size in bytes (8KB).
pub const PAGE_SIZE: usize = 8192;

/// Page size as u64 for offset calculations.
pub const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;

/// A page identifier (0-indexed page number).
pub type PageId = u64;

/// Page type identifiers stored in page headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Superblock (page 0 only)
    Superblock = 0x01,
    /// Page allocation bitmap
    AllocationBitmap = 0x02,
    /// B-tree internal node
    BTreeInternal = 0x03,
    /// B-tree leaf node
    BTreeLeaf = 0x04,
    /// Overflow page for large values
    Overflow = 0x05,
    /// Free page (on free list)
    Free = 0x06,
    /// Transaction log page
    TransactionLog = 0x07,
}

impl TryFrom<u8> for PageType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Superblock),
            0x02 => Ok(Self::AllocationBitmap),
            0x03 => Ok(Self::BTreeInternal),
            0x04 => Ok(Self::BTreeLeaf),
            0x05 => Ok(Self::Overflow),
            0x06 => Ok(Self::Free),
            0x07 => Ok(Self::TransactionLog),
            _ => Err(value),
        }
    }
}

/// Common page header present at the start of every page (except superblock).
///
/// Layout:
/// - `page_type`: 1 byte
/// - `flags`: 1 byte
/// - `checksum`: 4 bytes (CRC32)
/// - `reserved`: 2 bytes
///
/// Total: 8 bytes
#[derive(Debug, Copy, Clone)]
pub struct PageHeader {
    pub page_type: PageType,
    pub flags: u8,
    pub checksum: u32,
}

impl PageHeader {
    /// Size of the page header in bytes.
    pub const SIZE: usize = 8;

    /// Usable space in a page after the header.
    pub const USABLE_SPACE: usize = PAGE_SIZE - Self::SIZE;

    /// Serialize the header to bytes.
    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0] = self.page_type as u8;
        buf[1] = self.flags;
        buf[2..6].copy_from_slice(&self.checksum.to_le_bytes());
        // bytes 6-7 reserved
        buf
    }

    /// Deserialize a header from bytes.
    pub fn from_bytes(bytes: [u8; Self::SIZE]) -> Result<Self, PageError> {
        let page_type = PageType::try_from(bytes[0]).map_err(PageError::InvalidPageType)?;
        let flags = bytes[1];
        let checksum = u32::from_le_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);

        Ok(Self {
            page_type,
            flags,
            checksum,
        })
    }
}

/// A raw page buffer backed by a buffer pool.
///
/// Pages are leased from a `BufferPool` and automatically returned when dropped.
/// This eliminates per-read heap allocations and reduces memory fragmentation.
///
/// # Invariants
/// - Buffer is always Some until dropped
/// - Buffer size is always `PAGE_SIZE` bytes
pub struct Page {
    /// The underlying buffer. Option to allow `take()` in Drop.
    buffer: Option<Box<[u8; PAGE_SIZE]>>,
    /// Reference back to the pool for return on drop.
    pool: Arc<BufferPool>,
}

impl Page {
    /// Create a new page from a leased buffer.
    ///
    /// This is an internal constructor. Use `BufferPool::lease()` or
    /// `BufferPool::lease_zeroed()` to create pages.
    ///
    /// # Pre-conditions
    /// - `buffer` must be a valid `PAGE_SIZE` buffer from `pool`
    ///
    /// # Post-conditions
    /// - Page owns the buffer until dropped
    pub(crate) const fn from_pool(buffer: Box<[u8; PAGE_SIZE]>, pool: Arc<BufferPool>) -> Self {
        Self {
            buffer: Some(buffer),
            pool,
        }
    }

    /// Get the raw page data.
    ///
    /// # Panics
    /// Panics if called after the buffer has been taken (should never happen in normal use).
    #[must_use]
    #[allow(clippy::expect_used)] // Buffer being None indicates a bug
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        self.buffer.as_ref().expect("buffer taken before drop")
    }

    /// Get mutable access to the raw page data.
    ///
    /// # Panics
    /// Panics if called after the buffer has been taken (should never happen in normal use).
    #[allow(clippy::expect_used)] // Buffer being None indicates a bug
    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.buffer.as_mut().expect("buffer taken before drop")
    }

    /// Read bytes at a specific offset.
    #[must_use]
    pub fn read_bytes(&self, offset: usize, len: usize) -> &[u8] {
        &self.as_bytes()[offset..offset + len]
    }

    /// Write bytes at a specific offset.
    pub fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self.as_bytes_mut()[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    /// Read a u8 at the given offset.
    #[must_use]
    pub fn read_u8(&self, offset: usize) -> u8 {
        self.as_bytes()[offset]
    }

    /// Write a u8 at the given offset.
    pub fn write_u8(&mut self, offset: usize, value: u8) {
        self.as_bytes_mut()[offset] = value;
    }

    /// Read a u32 (little-endian) at the given offset.
    #[must_use]
    pub fn read_u32(&self, offset: usize) -> u32 {
        let data = self.as_bytes();
        u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ])
    }

    /// Write a u32 (little-endian) at the given offset.
    pub fn write_u32(&mut self, offset: usize, value: u32) {
        self.as_bytes_mut()[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    /// Read a u64 (little-endian) at the given offset.
    #[must_use]
    pub fn read_u64(&self, offset: usize) -> u64 {
        let data = self.as_bytes();
        u64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ])
    }

    /// Write a u64 (little-endian) at the given offset.
    pub fn write_u64(&mut self, offset: usize, value: u64) {
        self.as_bytes_mut()[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// Compute CRC32 checksum of the page data (excluding the checksum field itself).
    /// Assumes checksum is stored at bytes 2-5 of the header.
    #[must_use]
    pub fn compute_checksum(&self) -> u32 {
        let data = self.as_bytes();
        // For checksum calculation, we hash everything except the checksum field (bytes 2-5)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data[0..2]); // page_type + flags
        hasher.update(&[0u8; 4]); // zero out checksum field
        hasher.update(&data[6..]); // rest of page
        hasher.finalize()
    }

    /// Get a reference to the pool this page belongs to.
    #[must_use]
    pub const fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_buffer(buffer);
        }
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.buffer {
            Some(data) => f
                .debug_struct("Page")
                .field("first_16_bytes", &&data[..16])
                .finish_non_exhaustive(),
            None => f.debug_struct("Page").field("buffer", &"taken").finish(),
        }
    }
}

impl Clone for Page {
    /// Clone the page by leasing a new buffer from the same pool.
    ///
    /// # Panics
    /// Panics if the pool is exhausted. This is intentional as Clone
    /// cannot return an error, and pool exhaustion in clone typically
    /// indicates a configuration issue (pool too small).
    #[allow(clippy::disallowed_methods)] // Clone needed for simulation testing
    #[allow(clippy::expect_used)] // Pool exhaustion during clone indicates config issue
    fn clone(&self) -> Self {
        let new_buffer = self
            .pool
            .lease()
            .expect("buffer pool exhausted during Page::clone()");
        let mut new_page = Self {
            buffer: Some(new_buffer),
            pool: Arc::clone(&self.pool),
        };
        // Copy data from self to new page
        new_page.as_bytes_mut().copy_from_slice(self.as_bytes());
        new_page
    }
}

/// Errors related to page operations.
#[derive(Debug)]
pub enum PageError {
    /// Invalid page type byte.
    InvalidPageType(u8),
    /// Checksum mismatch.
    ChecksumMismatch { expected: u32, actual: u32 },
}

impl std::fmt::Display for PageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPageType(v) => write!(f, "invalid page type: 0x{v:02x}"),
            Self::ChecksumMismatch { expected, actual } => {
                write!(f, "checksum mismatch: expected {expected}, got {actual}")
            }
        }
    }
}

impl std::error::Error for PageError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> Arc<BufferPool> {
        BufferPool::new(10)
    }

    #[test]
    fn test_page_header_roundtrip() {
        let header = PageHeader {
            page_type: PageType::BTreeLeaf,
            flags: 0x42,
            checksum: 0xDEAD_BEEF,
        };

        let bytes = header.to_bytes();
        let restored = PageHeader::from_bytes(bytes).expect("should parse");

        assert_eq!(restored.page_type, PageType::BTreeLeaf);
        assert_eq!(restored.flags, 0x42);
        assert_eq!(restored.checksum, 0xDEAD_BEEF);
    }

    #[test]
    fn test_page_read_write() {
        let pool = test_pool();
        let mut page = pool
            .lease_zeroed()
            .map(|buf| Page::from_pool(buf, Arc::clone(&pool)))
            .expect("should lease");

        page.write_u8(0, 0xFF);
        assert_eq!(page.read_u8(0), 0xFF);

        page.write_u32(100, 0x1234_5678);
        assert_eq!(page.read_u32(100), 0x1234_5678);

        page.write_u64(200, 0x0102_0304_0506_0708);
        assert_eq!(page.read_u64(200), 0x0102_0304_0506_0708);

        page.write_bytes(500, b"hello");
        assert_eq!(page.read_bytes(500, 5), b"hello");
    }

    #[test]
    fn test_page_type_conversion() {
        assert_eq!(PageType::try_from(0x01), Ok(PageType::Superblock));
        assert_eq!(PageType::try_from(0x04), Ok(PageType::BTreeLeaf));
        assert!(PageType::try_from(0xFF).is_err());
    }

    #[test]
    fn test_page_returns_to_pool_on_drop() {
        let pool = BufferPool::new(2);
        assert_eq!(pool.available(), 2);

        {
            let _page1 = pool
                .lease_zeroed()
                .map(|buf| Page::from_pool(buf, Arc::clone(&pool)))
                .expect("should lease");
            assert_eq!(pool.available(), 1);

            let _page2 = pool
                .lease_zeroed()
                .map(|buf| Page::from_pool(buf, Arc::clone(&pool)))
                .expect("should lease");
            assert_eq!(pool.available(), 0);
        }

        // Both pages dropped, buffers returned
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_page_clone() {
        let pool = test_pool();
        let mut page = pool
            .lease_zeroed()
            .map(|buf| Page::from_pool(buf, Arc::clone(&pool)))
            .expect("should lease");

        page.write_u64(0, 0xDEAD_BEEF);

        let cloned = page.clone();
        assert_eq!(cloned.read_u64(0), 0xDEAD_BEEF);

        // Original and clone are independent
        assert_eq!(pool.available(), 8); // 10 - 2 = 8
    }
}
