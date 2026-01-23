//! Page types and constants for the storage engine.
//!
//! The storage engine uses 8KB pages as the fundamental unit of I/O.

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

/// A raw page buffer.
pub struct Page {
    data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    /// Create a new zeroed page.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: Box::new([0u8; PAGE_SIZE]),
        }
    }

    /// Create a page from raw bytes.
    #[must_use]
    #[allow(clippy::large_types_passed_by_value)]
    pub fn from_bytes(bytes: [u8; PAGE_SIZE]) -> Self {
        Self {
            data: Box::new(bytes),
        }
    }

    /// Get the raw page data.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// Get mutable access to the raw page data.
    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }

    /// Read bytes at a specific offset.
    #[must_use]
    pub fn read_bytes(&self, offset: usize, len: usize) -> &[u8] {
        &self.data[offset..offset + len]
    }

    /// Write bytes at a specific offset.
    pub fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self.data[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    /// Read a u8 at the given offset.
    #[must_use]
    pub fn read_u8(&self, offset: usize) -> u8 {
        self.data[offset]
    }

    /// Write a u8 at the given offset.
    pub fn write_u8(&mut self, offset: usize, value: u8) {
        self.data[offset] = value;
    }

    /// Read a u32 (little-endian) at the given offset.
    #[must_use]
    pub fn read_u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes([
            self.data[offset],
            self.data[offset + 1],
            self.data[offset + 2],
            self.data[offset + 3],
        ])
    }

    /// Write a u32 (little-endian) at the given offset.
    pub fn write_u32(&mut self, offset: usize, value: u32) {
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    /// Read a u64 (little-endian) at the given offset.
    #[must_use]
    pub fn read_u64(&self, offset: usize) -> u64 {
        u64::from_le_bytes([
            self.data[offset],
            self.data[offset + 1],
            self.data[offset + 2],
            self.data[offset + 3],
            self.data[offset + 4],
            self.data[offset + 5],
            self.data[offset + 6],
            self.data[offset + 7],
        ])
    }

    /// Write a u64 (little-endian) at the given offset.
    pub fn write_u64(&mut self, offset: usize, value: u64) {
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// Compute CRC32 checksum of the page data (excluding the checksum field itself).
    /// Assumes checksum is stored at bytes 2-5 of the header.
    #[must_use]
    pub fn compute_checksum(&self) -> u32 {
        // For checksum calculation, we hash everything except the checksum field (bytes 2-5)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.data[0..2]); // page_type + flags
        hasher.update(&[0u8; 4]); // zero out checksum field
        hasher.update(&self.data[6..]); // rest of page
        hasher.finalize()
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("first_16_bytes", &&self.data[..16])
            .finish_non_exhaustive()
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
        let mut page = Page::new();

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
}
