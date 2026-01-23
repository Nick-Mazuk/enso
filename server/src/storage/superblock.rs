//! Superblock structure and serialization.
//!
//! The superblock occupies page 0 and contains metadata about the database file.

// PAGE_SIZE is a compile-time constant that fits in u32.
#![allow(clippy::cast_possible_truncation)]

use crate::storage::page::{Page, PageId, PAGE_SIZE};

/// Magic number identifying an Enso database file: "ENSOTRPL"
pub const MAGIC: [u8; 8] = *b"ENSOTRPL";

/// Current format version.
pub const FORMAT_VERSION: u32 = 1;

/// Page size as u32 for storage in superblock.
const PAGE_SIZE_U32: u32 = PAGE_SIZE as u32;

/// Superblock field offsets.
mod offsets {
    pub const MAGIC: usize = 0;
    pub const FORMAT_VERSION: usize = 8;
    pub const PAGE_SIZE: usize = 12;
    pub const FILE_SIZE: usize = 16;
    pub const TOTAL_PAGE_COUNT: usize = 24;
    pub const PRIMARY_INDEX_ROOT: usize = 32;
    pub const ATTRIBUTE_INDEX_ROOT: usize = 40;
    pub const FREE_LIST_HEAD: usize = 48;
    pub const LAST_CHECKPOINT_LSN: usize = 56;
    pub const LAST_CHECKPOINT_HLC: usize = 64;
    pub const TXN_LOG_START: usize = 80;
    pub const TXN_LOG_END: usize = 88;
    pub const TXN_LOG_CAPACITY: usize = 96;
    pub const ACTIVE_TXN_COUNT: usize = 104;
    pub const NEXT_TXN_ID: usize = 112;
    pub const SCHEMA_VERSION: usize = 120;
    // 128-1023: reserved
    // 1024-8191: checkpoint metadata
}

/// Hybrid Logical Clock timestamp (16 bytes).
///
/// Layout:
/// - `physical_time`: 8 bytes (nanoseconds since Unix epoch)
/// - `logical_counter`: 4 bytes
/// - `node_id`: 4 bytes
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct HlcTimestamp {
    /// Physical time in nanoseconds since Unix epoch.
    pub physical_time: u64,
    /// Logical counter for ordering events at the same physical time.
    pub logical_counter: u32,
    /// Node identifier for distributed uniqueness.
    pub node_id: u32,
}

impl HlcTimestamp {
    /// Size in bytes.
    pub const SIZE: usize = 16;

    /// Serialize to bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; Self::SIZE] {
        let phys = self.physical_time.to_le_bytes();
        let logic = self.logical_counter.to_le_bytes();
        let node = self.node_id.to_le_bytes();
        [
            phys[0], phys[1], phys[2], phys[3], phys[4], phys[5], phys[6], phys[7], logic[0],
            logic[1], logic[2], logic[3], node[0], node[1], node[2], node[3],
        ]
    }

    /// Deserialize from bytes.
    #[must_use]
    pub const fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        Self {
            physical_time: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            logical_counter: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            node_id: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        }
    }
}

/// The superblock contains all metadata about the database file.
#[derive(Debug, Copy, Clone)]
pub struct Superblock {
    /// Format version number.
    pub format_version: u32,
    /// Page size in bytes (should always be `PAGE_SIZE`).
    pub page_size: u32,
    /// Total file size in bytes.
    pub file_size: u64,
    /// Total number of pages in the file.
    pub total_page_count: u64,
    /// Root page of the primary (`entity_id`, `attribute_id`) index.
    pub primary_index_root: PageId,
    /// Root page of the attribute index.
    pub attribute_index_root: PageId,
    /// Head of the free page list.
    pub free_list_head: PageId,
    /// Log sequence number of the last checkpoint.
    pub last_checkpoint_lsn: u64,
    /// HLC timestamp of the last checkpoint.
    pub last_checkpoint_hlc: HlcTimestamp,
    /// Transaction log region start offset in file.
    pub txn_log_start: u64,
    /// Transaction log region end offset (write position).
    pub txn_log_end: u64,
    /// Transaction log capacity in bytes.
    pub txn_log_capacity: u64,
    /// Number of active transactions.
    pub active_txn_count: u64,
    /// Next transaction ID to assign.
    pub next_txn_id: u64,
    /// Schema version for migrations.
    pub schema_version: u64,
}

impl Superblock {
    /// Create a new superblock with default values for a fresh database.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            page_size: PAGE_SIZE_U32,
            file_size: PAGE_SIZE as u64, // Initially just the superblock
            total_page_count: 1,
            primary_index_root: 0,
            attribute_index_root: 0,
            free_list_head: 0,
            last_checkpoint_lsn: 0,
            last_checkpoint_hlc: HlcTimestamp {
                physical_time: 0,
                logical_counter: 0,
                node_id: 0,
            },
            txn_log_start: 0,
            txn_log_end: 0,
            txn_log_capacity: 0,
            active_txn_count: 0,
            next_txn_id: 1,
            schema_version: 1,
        }
    }

    /// Serialize the superblock to a page.
    #[must_use]
    pub fn to_page(&self) -> Page {
        let mut page = Page::new();

        page.write_bytes(offsets::MAGIC, &MAGIC);
        page.write_u32(offsets::FORMAT_VERSION, self.format_version);
        page.write_u32(offsets::PAGE_SIZE, self.page_size);
        page.write_u64(offsets::FILE_SIZE, self.file_size);
        page.write_u64(offsets::TOTAL_PAGE_COUNT, self.total_page_count);
        page.write_u64(offsets::PRIMARY_INDEX_ROOT, self.primary_index_root);
        page.write_u64(offsets::ATTRIBUTE_INDEX_ROOT, self.attribute_index_root);
        page.write_u64(offsets::FREE_LIST_HEAD, self.free_list_head);
        page.write_u64(offsets::LAST_CHECKPOINT_LSN, self.last_checkpoint_lsn);
        page.write_bytes(offsets::LAST_CHECKPOINT_HLC, &self.last_checkpoint_hlc.to_bytes());
        page.write_u64(offsets::TXN_LOG_START, self.txn_log_start);
        page.write_u64(offsets::TXN_LOG_END, self.txn_log_end);
        page.write_u64(offsets::TXN_LOG_CAPACITY, self.txn_log_capacity);
        page.write_u64(offsets::ACTIVE_TXN_COUNT, self.active_txn_count);
        page.write_u64(offsets::NEXT_TXN_ID, self.next_txn_id);
        page.write_u64(offsets::SCHEMA_VERSION, self.schema_version);

        page
    }

    /// Deserialize a superblock from a page.
    pub fn from_page(page: &Page) -> Result<Self, SuperblockError> {
        // Validate magic number
        let magic_slice = page.read_bytes(offsets::MAGIC, 8);
        let mut magic = [0u8; 8];
        magic.copy_from_slice(magic_slice);
        if magic != MAGIC {
            return Err(SuperblockError::InvalidMagic(magic));
        }

        let format_version = page.read_u32(offsets::FORMAT_VERSION);
        if format_version != FORMAT_VERSION {
            return Err(SuperblockError::UnsupportedVersion(format_version));
        }

        let page_size = page.read_u32(offsets::PAGE_SIZE);
        if page_size != PAGE_SIZE_U32 {
            return Err(SuperblockError::InvalidPageSize(page_size));
        }

        let hlc_slice = page.read_bytes(offsets::LAST_CHECKPOINT_HLC, 16);
        let mut hlc_bytes = [0u8; 16];
        hlc_bytes.copy_from_slice(hlc_slice);

        Ok(Self {
            format_version,
            page_size,
            file_size: page.read_u64(offsets::FILE_SIZE),
            total_page_count: page.read_u64(offsets::TOTAL_PAGE_COUNT),
            primary_index_root: page.read_u64(offsets::PRIMARY_INDEX_ROOT),
            attribute_index_root: page.read_u64(offsets::ATTRIBUTE_INDEX_ROOT),
            free_list_head: page.read_u64(offsets::FREE_LIST_HEAD),
            last_checkpoint_lsn: page.read_u64(offsets::LAST_CHECKPOINT_LSN),
            last_checkpoint_hlc: HlcTimestamp::from_bytes(&hlc_bytes),
            txn_log_start: page.read_u64(offsets::TXN_LOG_START),
            txn_log_end: page.read_u64(offsets::TXN_LOG_END),
            txn_log_capacity: page.read_u64(offsets::TXN_LOG_CAPACITY),
            active_txn_count: page.read_u64(offsets::ACTIVE_TXN_COUNT),
            next_txn_id: page.read_u64(offsets::NEXT_TXN_ID),
            schema_version: page.read_u64(offsets::SCHEMA_VERSION),
        })
    }
}

impl Default for Superblock {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur when reading a superblock.
#[derive(Debug)]
pub enum SuperblockError {
    /// Invalid magic number.
    InvalidMagic([u8; 8]),
    /// Unsupported format version.
    UnsupportedVersion(u32),
    /// Invalid page size.
    InvalidPageSize(u32),
}

impl std::fmt::Display for SuperblockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMagic(magic) => {
                write!(f, "invalid magic number: {:?}", String::from_utf8_lossy(magic))
            }
            Self::UnsupportedVersion(v) => write!(f, "unsupported format version: {v}"),
            Self::InvalidPageSize(s) => write!(f, "invalid page size: {s}"),
        }
    }
}

impl std::error::Error for SuperblockError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_superblock_roundtrip() {
        let mut sb = Superblock::new();
        sb.file_size = 1024 * 1024;
        sb.total_page_count = 128;
        sb.primary_index_root = 5;
        sb.attribute_index_root = 10;
        sb.free_list_head = 15;
        sb.next_txn_id = 42;
        sb.last_checkpoint_hlc = HlcTimestamp {
            physical_time: 1_234_567_890,
            logical_counter: 100,
            node_id: 1,
        };

        let page = sb.to_page();
        let restored = Superblock::from_page(&page).expect("should parse");

        assert_eq!(restored.format_version, FORMAT_VERSION);
        assert_eq!(restored.page_size, PAGE_SIZE_U32);
        assert_eq!(restored.file_size, 1024 * 1024);
        assert_eq!(restored.total_page_count, 128);
        assert_eq!(restored.primary_index_root, 5);
        assert_eq!(restored.attribute_index_root, 10);
        assert_eq!(restored.free_list_head, 15);
        assert_eq!(restored.next_txn_id, 42);
        assert_eq!(restored.last_checkpoint_hlc.physical_time, 1_234_567_890);
        assert_eq!(restored.last_checkpoint_hlc.logical_counter, 100);
        assert_eq!(restored.last_checkpoint_hlc.node_id, 1);
    }

    #[test]
    fn test_superblock_invalid_magic() {
        let mut page = Page::new();
        page.write_bytes(0, b"BADMAGIC");

        let result = Superblock::from_page(&page);
        assert!(matches!(result, Err(SuperblockError::InvalidMagic(_))));
    }

    #[test]
    fn test_hlc_roundtrip() {
        let hlc = HlcTimestamp {
            physical_time: 0x0102_0304_0506_0708,
            logical_counter: 0x090A_0B0C,
            node_id: 0x0D0E_0F10,
        };

        let bytes = hlc.to_bytes();
        let restored = HlcTimestamp::from_bytes(&bytes);

        assert_eq!(restored, hlc);
    }
}
