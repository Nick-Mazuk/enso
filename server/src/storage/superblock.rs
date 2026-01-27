//! Superblock structure and serialization.
//!
//! The superblock occupies page 0 and contains metadata about the database file.

// PAGE_SIZE is a compile-time constant that fits in u32.
#![allow(clippy::cast_possible_truncation)]

use std::sync::Arc;

use crate::storage::buffer_pool::BufferPool;
use crate::storage::page::{PAGE_SIZE, Page, PageId};
use crate::types::HlcTimestamp;

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
    pub const ENTITY_ATTRIBUTE_INDEX_ROOT: usize = 48;
    pub const FREE_LIST_HEAD: usize = 56;
    pub const LAST_CHECKPOINT_LSN: usize = 64;
    pub const LAST_CHECKPOINT_HLC: usize = 72;
    pub const LAST_WAL_LSN: usize = 88;
    pub const TXN_LOG_START: usize = 96;
    pub const TXN_LOG_END: usize = 104;
    pub const TXN_LOG_CAPACITY: usize = 112;
    pub const ACTIVE_TXN_COUNT: usize = 120;
    pub const NEXT_TXN_ID: usize = 128;
    pub const SCHEMA_VERSION: usize = 136;
    // Tombstone list metadata (for incremental GC)
    pub const TOMBSTONE_HEAD_PAGE: usize = 144;
    pub const TOMBSTONE_TAIL_PAGE: usize = 152;
    pub const TOMBSTONE_TAIL_SLOT: usize = 160;
    pub const TOMBSTONE_COUNT: usize = 168;
    // 176-1023: reserved
    // 1024-8191: checkpoint metadata
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
    /// Root page of the entity-attribute index.
    pub entity_attribute_index_root: PageId,
    /// Head of the free page list.
    pub free_list_head: PageId,
    /// Log sequence number of the last checkpoint.
    pub last_checkpoint_lsn: u64,
    /// HLC timestamp of the last checkpoint.
    pub last_checkpoint_hlc: HlcTimestamp,
    /// Highest LSN written to the WAL (for continuing writes after restart).
    pub last_wal_lsn: u64,
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
    /// Head page of the tombstone list (oldest tombstones, for GC).
    pub tombstone_head_page: PageId,
    /// Tail page of the tombstone list (newest tombstones, for appends).
    pub tombstone_tail_page: PageId,
    /// Next write slot in the tombstone tail page.
    pub tombstone_tail_slot: u64,
    /// Total count of pending tombstones.
    pub tombstone_count: u64,
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
            entity_attribute_index_root: 0,
            free_list_head: 0,
            last_checkpoint_lsn: 0,
            last_checkpoint_hlc: HlcTimestamp {
                physical_time: 0,
                logical_counter: 0,
                node_id: 0,
            },
            last_wal_lsn: 0,
            txn_log_start: 0,
            txn_log_end: 0,
            txn_log_capacity: 0,
            active_txn_count: 0,
            next_txn_id: 1,
            schema_version: 1,
            tombstone_head_page: 0,
            tombstone_tail_page: 0,
            tombstone_tail_slot: 0,
            tombstone_count: 0,
        }
    }

    /// Serialize the superblock to a page.
    ///
    /// Returns `None` if the buffer pool is exhausted.
    pub fn to_page(&self, pool: &Arc<BufferPool>) -> Option<Page> {
        let mut page = pool.lease_page_zeroed()?;

        page.write_bytes(offsets::MAGIC, &MAGIC);
        page.write_u32(offsets::FORMAT_VERSION, self.format_version);
        page.write_u32(offsets::PAGE_SIZE, self.page_size);
        page.write_u64(offsets::FILE_SIZE, self.file_size);
        page.write_u64(offsets::TOTAL_PAGE_COUNT, self.total_page_count);
        page.write_u64(offsets::PRIMARY_INDEX_ROOT, self.primary_index_root);
        page.write_u64(offsets::ATTRIBUTE_INDEX_ROOT, self.attribute_index_root);
        page.write_u64(
            offsets::ENTITY_ATTRIBUTE_INDEX_ROOT,
            self.entity_attribute_index_root,
        );
        page.write_u64(offsets::FREE_LIST_HEAD, self.free_list_head);
        page.write_u64(offsets::LAST_CHECKPOINT_LSN, self.last_checkpoint_lsn);
        page.write_bytes(
            offsets::LAST_CHECKPOINT_HLC,
            &self.last_checkpoint_hlc.to_bytes(),
        );
        page.write_u64(offsets::LAST_WAL_LSN, self.last_wal_lsn);
        page.write_u64(offsets::TXN_LOG_START, self.txn_log_start);
        page.write_u64(offsets::TXN_LOG_END, self.txn_log_end);
        page.write_u64(offsets::TXN_LOG_CAPACITY, self.txn_log_capacity);
        page.write_u64(offsets::ACTIVE_TXN_COUNT, self.active_txn_count);
        page.write_u64(offsets::NEXT_TXN_ID, self.next_txn_id);
        page.write_u64(offsets::SCHEMA_VERSION, self.schema_version);
        page.write_u64(offsets::TOMBSTONE_HEAD_PAGE, self.tombstone_head_page);
        page.write_u64(offsets::TOMBSTONE_TAIL_PAGE, self.tombstone_tail_page);
        page.write_u64(offsets::TOMBSTONE_TAIL_SLOT, self.tombstone_tail_slot);
        page.write_u64(offsets::TOMBSTONE_COUNT, self.tombstone_count);

        Some(page)
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
            entity_attribute_index_root: page.read_u64(offsets::ENTITY_ATTRIBUTE_INDEX_ROOT),
            free_list_head: page.read_u64(offsets::FREE_LIST_HEAD),
            last_checkpoint_lsn: page.read_u64(offsets::LAST_CHECKPOINT_LSN),
            last_checkpoint_hlc: HlcTimestamp::from_bytes(&hlc_bytes),
            last_wal_lsn: page.read_u64(offsets::LAST_WAL_LSN),
            txn_log_start: page.read_u64(offsets::TXN_LOG_START),
            txn_log_end: page.read_u64(offsets::TXN_LOG_END),
            txn_log_capacity: page.read_u64(offsets::TXN_LOG_CAPACITY),
            active_txn_count: page.read_u64(offsets::ACTIVE_TXN_COUNT),
            next_txn_id: page.read_u64(offsets::NEXT_TXN_ID),
            schema_version: page.read_u64(offsets::SCHEMA_VERSION),
            tombstone_head_page: page.read_u64(offsets::TOMBSTONE_HEAD_PAGE),
            tombstone_tail_page: page.read_u64(offsets::TOMBSTONE_TAIL_PAGE),
            tombstone_tail_slot: page.read_u64(offsets::TOMBSTONE_TAIL_SLOT),
            tombstone_count: page.read_u64(offsets::TOMBSTONE_COUNT),
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
                write!(
                    f,
                    "invalid magic number: {:?}",
                    String::from_utf8_lossy(magic)
                )
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

    fn test_pool() -> Arc<BufferPool> {
        BufferPool::new(10)
    }

    #[test]
    fn test_superblock_roundtrip() {
        let pool = test_pool();

        let mut sb = Superblock::new();
        sb.file_size = 1024 * 1024;
        sb.total_page_count = 128;
        sb.primary_index_root = 5;
        sb.attribute_index_root = 10;
        sb.entity_attribute_index_root = 12;
        sb.free_list_head = 15;
        sb.next_txn_id = 42;
        sb.last_checkpoint_hlc = HlcTimestamp {
            physical_time: 1_234_567_890,
            logical_counter: 100,
            node_id: 1,
        };

        let page = sb.to_page(&pool).expect("should serialize");
        let restored = Superblock::from_page(&page).expect("should parse");

        assert_eq!(restored.format_version, FORMAT_VERSION);
        assert_eq!(restored.page_size, PAGE_SIZE_U32);
        assert_eq!(restored.file_size, 1024 * 1024);
        assert_eq!(restored.total_page_count, 128);
        assert_eq!(restored.primary_index_root, 5);
        assert_eq!(restored.attribute_index_root, 10);
        assert_eq!(restored.entity_attribute_index_root, 12);
        assert_eq!(restored.free_list_head, 15);
        assert_eq!(restored.next_txn_id, 42);
        assert_eq!(restored.last_checkpoint_hlc.physical_time, 1_234_567_890);
        assert_eq!(restored.last_checkpoint_hlc.logical_counter, 100);
        assert_eq!(restored.last_checkpoint_hlc.node_id, 1);
    }

    #[test]
    fn test_superblock_invalid_magic() {
        let pool = test_pool();
        let mut page = pool.lease_page_zeroed().expect("should lease");
        page.write_bytes(0, b"BADMAGIC");

        let result = Superblock::from_page(&page);
        assert!(matches!(result, Err(SuperblockError::InvalidMagic(_))));
    }
}
