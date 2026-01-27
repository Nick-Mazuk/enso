// Page counts and slot indices fit in usize on all platforms.
#![allow(clippy::cast_possible_truncation)]

//! Tombstone tracking for incremental garbage collection.
//!
//! This module provides a disk-based linked list of tombstones (deleted records)
//! that enables O(1) memory usage regardless of how many records are pending GC.
//!
//! # Design
//!
//! Instead of loading all tombstones into memory (which would use 40MB per million
//! deleted records), we maintain a linked list of pages on disk:
//!
//! - **Head page**: Where GC reads from (oldest tombstones)
//! - **Tail page**: Where commits append to (newest tombstones)
//! - **Write buffer**: Small in-memory buffer for batching appends
//!
//! # Page Format
//!
//! ```text
//! Offset   Size   Field
//! 0-7      8      Page type marker (PageType::TombstoneList)
//! 8-15     8      Entry count in this page (0-204)
//! 16-23    8      Next page ID (0 = end of list)
//! 24-31    8      Head slot (only used in head page, for partial consumption)
//! 32+      var    Tombstone entries (40 bytes each)
//! ```

use crate::storage::file::DatabaseFile;
use crate::storage::page::{PAGE_SIZE, PageHeader, PageId, PageType};
use crate::types::{AttributeId, EntityId, TxnId};

/// Size of a serialized tombstone in bytes.
pub const TOMBSTONE_SIZE: usize = 40;

/// Header size in a tombstone page.
const TOMBSTONE_PAGE_HEADER_SIZE: usize = 32;

/// Maximum number of tombstones per page.
pub const TOMBSTONES_PER_PAGE: usize = (PAGE_SIZE - TOMBSTONE_PAGE_HEADER_SIZE) / TOMBSTONE_SIZE;

/// Offset of the entry count field in a tombstone page.
const OFFSET_ENTRY_COUNT: usize = 8;

/// Offset of the next page ID field in a tombstone page.
const OFFSET_NEXT_PAGE: usize = 16;

/// Offset of the head slot field in a tombstone page.
const OFFSET_HEAD_SLOT: usize = 24;

/// Offset where tombstone entries begin.
const OFFSET_ENTRIES: usize = TOMBSTONE_PAGE_HEADER_SIZE;

/// A tombstone entry tracking a deleted record awaiting GC.
///
/// # Invariants
///
/// - `deleted_txn > 0` (0 means not deleted)
/// - Tombstones are added in transaction order (`deleted_txn` is monotonically increasing)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tombstone {
    /// Entity ID of the deleted record.
    pub entity_id: EntityId,
    /// Attribute ID of the deleted record.
    pub attribute_id: AttributeId,
    /// Transaction ID that performed the deletion.
    pub deleted_txn: TxnId,
}

impl Tombstone {
    /// Create a new tombstone.
    ///
    /// # Pre-conditions
    /// - `deleted_txn > 0`
    #[must_use]
    pub const fn new(entity_id: EntityId, attribute_id: AttributeId, deleted_txn: TxnId) -> Self {
        debug_assert!(deleted_txn > 0, "deleted_txn must be positive");
        Self {
            entity_id,
            attribute_id,
            deleted_txn,
        }
    }

    /// Check if this tombstone is eligible for garbage collection.
    ///
    /// A tombstone can be collected if:
    /// - No active snapshots exist (`min_active_txn` is `None`), OR
    /// - The deletion is invisible to all snapshots (`deleted_txn < min_active_txn`)
    #[must_use]
    pub const fn is_gc_eligible(&self, min_active_txn: Option<TxnId>) -> bool {
        match min_active_txn {
            Some(min_txn) => self.deleted_txn < min_txn,
            None => true,
        }
    }

    /// Serialize the tombstone to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; TOMBSTONE_SIZE] {
        let mut bytes = [0u8; TOMBSTONE_SIZE];
        bytes[0..16].copy_from_slice(&self.entity_id.0);
        bytes[16..32].copy_from_slice(&self.attribute_id.0);
        bytes[32..40].copy_from_slice(&self.deleted_txn.to_le_bytes());
        bytes
    }

    /// Deserialize a tombstone from bytes.
    ///
    /// # Pre-conditions
    /// - `bytes.len() >= TOMBSTONE_SIZE`
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() >= TOMBSTONE_SIZE);

        let mut entity = [0u8; 16];
        let mut attribute = [0u8; 16];
        entity.copy_from_slice(&bytes[0..16]);
        attribute.copy_from_slice(&bytes[16..32]);
        let deleted_txn = u64::from_le_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
        ]);

        Self {
            entity_id: EntityId(entity),
            attribute_id: AttributeId(attribute),
            deleted_txn,
        }
    }
}

/// A disk-based linked list of tombstones with O(1) memory usage.
///
/// # Invariants
///
/// - `head_page_id == 0` implies empty list (`tail_page_id` also 0)
/// - `head_page_id != 0` implies `tail_page_id != 0`
/// - `head_slot` is only meaningful when `head_page_id` != 0
/// - `tail_slot` is the next available slot in the tail page (0 to `TOMBSTONES_PER_PAGE`)
/// - Write buffer entries are ordered by `deleted_txn` (ascending)
///
/// # Memory Usage
///
/// O(1) per database regardless of tombstone count:
/// - 2 page IDs (16 bytes)
/// - 2 slot indices (16 bytes)
/// - Write buffer (~100 entries max = 4KB)
pub struct TombstoneList {
    /// Page ID of the first (oldest) tombstone page. GC reads from here.
    /// 0 means the list is empty.
    head_page_id: PageId,
    /// Index of the first unconsumed tombstone in the head page.
    head_slot: usize,
    /// Page ID of the last (newest) tombstone page. Commits append here.
    /// 0 means the list is empty.
    tail_page_id: PageId,
    /// Index of the next available slot in the tail page.
    tail_slot: usize,
    /// Write buffer for current batch of tombstones.
    /// Flushed to disk when full or when explicitly requested.
    write_buffer: Vec<Tombstone>,
    /// Total count of tombstones (for stats).
    count: u64,
}

impl TombstoneList {
    /// Maximum size of the write buffer before auto-flush.
    const WRITE_BUFFER_CAPACITY: usize = 100;

    /// Create a new empty tombstone list.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            head_page_id: 0,
            head_slot: 0,
            tail_page_id: 0,
            tail_slot: 0,
            write_buffer: Vec::new(),
            count: 0,
        }
    }

    /// Create a tombstone list from persisted state.
    ///
    /// This is called during database open to restore the list state from
    /// the superblock metadata.
    #[must_use]
    pub const fn from_persisted(
        head_page_id: PageId,
        head_slot: usize,
        tail_page_id: PageId,
        tail_slot: usize,
        count: u64,
    ) -> Self {
        Self {
            head_page_id,
            head_slot,
            tail_page_id,
            tail_slot,
            write_buffer: Vec::new(),
            count,
        }
    }

    /// Check if the list is empty.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Vec::is_empty is not const
    pub fn is_empty(&self) -> bool {
        self.head_page_id == 0 && self.write_buffer.is_empty()
    }

    /// Get the total count of pending tombstones.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// Get the head page ID.
    #[must_use]
    pub const fn head_page_id(&self) -> PageId {
        self.head_page_id
    }

    /// Get the head slot index.
    #[must_use]
    pub const fn head_slot(&self) -> usize {
        self.head_slot
    }

    /// Get the tail page ID.
    #[must_use]
    pub const fn tail_page_id(&self) -> PageId {
        self.tail_page_id
    }

    /// Get the tail slot index.
    #[must_use]
    pub const fn tail_slot(&self) -> usize {
        self.tail_slot
    }

    /// Get the number of buffered tombstones pending flush.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Vec::len is not const
    pub fn buffered_count(&self) -> usize {
        self.write_buffer.len()
    }

    /// Append a tombstone to the list.
    ///
    /// The tombstone is added to the write buffer. When the buffer fills up,
    /// it must be flushed to disk via `flush()`.
    ///
    /// # Pre-conditions
    /// - `tombstone.deleted_txn >= last appended tombstone's deleted_txn`
    pub fn append(&mut self, tombstone: Tombstone) {
        debug_assert!(
            self.write_buffer
                .last()
                .is_none_or(|t| t.deleted_txn <= tombstone.deleted_txn),
            "Tombstones must be appended in transaction order"
        );

        self.write_buffer.push(tombstone);
        self.count += 1;
    }

    /// Check if the write buffer needs to be flushed.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Vec::len is not const
    pub fn needs_flush(&self) -> bool {
        self.write_buffer.len() >= Self::WRITE_BUFFER_CAPACITY
    }

    /// Flush the write buffer to disk.
    ///
    /// Writes buffered tombstones to the tail page, allocating new pages as needed.
    ///
    /// # Post-conditions
    /// - Write buffer is empty
    /// - Tail page/slot updated to reflect new position
    pub fn flush(&mut self, file: &mut DatabaseFile) -> Result<(), TombstoneError> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        // Process buffered tombstones
        for tombstone in std::mem::take(&mut self.write_buffer) {
            self.write_tombstone_to_disk(file, &tombstone)?;
        }

        Ok(())
    }

    /// Write a single tombstone to the tail page.
    fn write_tombstone_to_disk(
        &mut self,
        file: &mut DatabaseFile,
        tombstone: &Tombstone,
    ) -> Result<(), TombstoneError> {
        // If list is empty, allocate the first page
        if self.tail_page_id == 0 {
            let page_id = file.allocate_pages(1)?;
            self.head_page_id = page_id;
            self.tail_page_id = page_id;
            self.head_slot = 0;
            self.tail_slot = 0;

            // Initialize the new page
            self.init_tombstone_page(file, page_id)?;
        }

        // If tail page is full, allocate a new page
        if self.tail_slot >= TOMBSTONES_PER_PAGE {
            let new_page_id = file.allocate_pages(1)?;

            // Link old tail to new page
            let mut old_tail = file.read_page(self.tail_page_id)?;
            old_tail.write_u64(OFFSET_NEXT_PAGE, new_page_id);
            file.write_page(self.tail_page_id, &old_tail)?;

            // Initialize new page
            self.init_tombstone_page(file, new_page_id)?;

            self.tail_page_id = new_page_id;
            self.tail_slot = 0;
        }

        // Write tombstone to tail page
        let mut page = file.read_page(self.tail_page_id)?;
        let offset = OFFSET_ENTRIES + self.tail_slot * TOMBSTONE_SIZE;
        page.write_bytes(offset, &tombstone.to_bytes());

        // Update entry count
        let new_count = self.tail_slot + 1;
        page.write_u64(OFFSET_ENTRY_COUNT, new_count as u64);

        file.write_page(self.tail_page_id, &page)?;

        self.tail_slot += 1;

        Ok(())
    }

    /// Initialize a new tombstone page.
    #[allow(clippy::unused_self)] // Keeping &self for consistency with other methods
    fn init_tombstone_page(
        &self,
        file: &mut DatabaseFile,
        page_id: PageId,
    ) -> Result<(), TombstoneError> {
        let pool = file.buffer_pool();
        let mut page = pool
            .lease_page_zeroed()
            .ok_or(TombstoneError::BufferPoolExhausted)?;

        // Write page header
        let header = PageHeader {
            page_type: PageType::TombstoneList,
            flags: 0,
            checksum: 0,
        };
        page.write_bytes(0, &header.to_bytes());

        // Initialize fields
        page.write_u64(OFFSET_ENTRY_COUNT, 0);
        page.write_u64(OFFSET_NEXT_PAGE, 0);
        page.write_u64(OFFSET_HEAD_SLOT, 0);

        file.write_page(page_id, &page)?;

        Ok(())
    }

    /// Pop a batch of eligible tombstones from the head of the list.
    ///
    /// Reads up to `max_count` tombstones that are eligible for GC
    /// (i.e., `deleted_txn < min_active_txn`).
    ///
    /// # Arguments
    /// - `file`: Database file for page I/O
    /// - `min_active_txn`: Minimum active snapshot transaction ID
    /// - `max_count`: Maximum number of tombstones to return
    ///
    /// # Returns
    /// Vector of eligible tombstones (may be fewer than `max_count`)
    pub fn pop_batch(
        &mut self,
        file: &mut DatabaseFile,
        min_active_txn: Option<TxnId>,
        max_count: usize,
    ) -> Result<Vec<Tombstone>, TombstoneError> {
        // First, flush any buffered tombstones to ensure consistency
        self.flush(file)?;

        if self.head_page_id == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(max_count);
        let mut pages_to_free = Vec::new();

        while result.len() < max_count && self.head_page_id != 0 {
            let page = file.read_page(self.head_page_id)?;
            let entry_count = page.read_u64(OFFSET_ENTRY_COUNT) as usize;
            let next_page = page.read_u64(OFFSET_NEXT_PAGE);

            // Read entries from head_slot to entry_count
            while self.head_slot < entry_count && result.len() < max_count {
                let offset = OFFSET_ENTRIES + self.head_slot * TOMBSTONE_SIZE;
                let bytes = page.read_bytes(offset, TOMBSTONE_SIZE);
                let tombstone = Tombstone::from_bytes(bytes);

                // Check eligibility
                if !tombstone.is_gc_eligible(min_active_txn) {
                    // Not eligible - stop processing (tombstones are ordered by deleted_txn)
                    return Ok(result);
                }

                result.push(tombstone);
                self.head_slot += 1;
                self.count = self.count.saturating_sub(1);
            }

            // If we've consumed all entries in this page, move to next
            if self.head_slot >= entry_count {
                pages_to_free.push(self.head_page_id);

                if next_page == 0 {
                    // List is now empty
                    self.head_page_id = 0;
                    self.head_slot = 0;
                    self.tail_page_id = 0;
                    self.tail_slot = 0;
                } else {
                    self.head_page_id = next_page;
                    self.head_slot = 0;
                }
            }
        }

        // Note: Pages are not freed in this implementation.
        // The storage engine currently only allocates pages by extending the file.
        // Future optimization: implement a free list to reclaim tombstone pages.
        // For now, consumed tombstone pages remain allocated but unused.
        let _ = pages_to_free; // Suppress unused variable warning

        Ok(result)
    }

    /// Persist the head slot to the head page.
    ///
    /// This is needed to track partial consumption of the head page.
    pub fn persist_head_slot(&self, file: &mut DatabaseFile) -> Result<(), TombstoneError> {
        if self.head_page_id == 0 {
            return Ok(());
        }

        let mut page = file.read_page(self.head_page_id)?;
        page.write_u64(OFFSET_HEAD_SLOT, self.head_slot as u64);
        file.write_page(self.head_page_id, &page)?;

        Ok(())
    }

    /// Load the head slot from the head page.
    ///
    /// Called during recovery to restore partial consumption state.
    pub fn load_head_slot(&mut self, file: &mut DatabaseFile) -> Result<(), TombstoneError> {
        if self.head_page_id == 0 {
            return Ok(());
        }

        let page = file.read_page(self.head_page_id)?;
        self.head_slot = page.read_u64(OFFSET_HEAD_SLOT) as usize;

        Ok(())
    }
}

impl Default for TombstoneList {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during tombstone operations.
#[derive(Debug)]
pub enum TombstoneError {
    /// File I/O error.
    File(crate::storage::file::FileError),
    /// Buffer pool exhausted.
    BufferPoolExhausted,
}

impl std::fmt::Display for TombstoneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(e) => write!(f, "file error: {e}"),
            Self::BufferPoolExhausted => write!(f, "buffer pool exhausted"),
        }
    }
}

impl std::error::Error for TombstoneError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(e) => Some(e),
            Self::BufferPoolExhausted => None,
        }
    }
}

impl From<crate::storage::file::FileError> for TombstoneError {
    fn from(e: crate::storage::file::FileError) -> Self {
        Self::File(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tombstone_serialization() {
        let tombstone = Tombstone::new(EntityId([1u8; 16]), AttributeId([2u8; 16]), 42);

        let bytes = tombstone.to_bytes();
        assert_eq!(bytes.len(), TOMBSTONE_SIZE);

        let restored = Tombstone::from_bytes(&bytes);
        assert_eq!(restored.entity_id, tombstone.entity_id);
        assert_eq!(restored.attribute_id, tombstone.attribute_id);
        assert_eq!(restored.deleted_txn, tombstone.deleted_txn);
    }

    #[test]
    fn test_tombstone_gc_eligibility() {
        let tombstone = Tombstone::new(EntityId([1u8; 16]), AttributeId([2u8; 16]), 10);

        // No active snapshots - always eligible
        assert!(tombstone.is_gc_eligible(None));

        // Snapshot after deletion - eligible
        assert!(tombstone.is_gc_eligible(Some(11)));
        assert!(tombstone.is_gc_eligible(Some(100)));

        // Snapshot at or before deletion - not eligible
        assert!(!tombstone.is_gc_eligible(Some(10)));
        assert!(!tombstone.is_gc_eligible(Some(5)));
    }

    #[test]
    fn test_tombstone_list_empty() {
        let list = TombstoneList::new();
        assert!(list.is_empty());
        assert_eq!(list.count(), 0);
        assert_eq!(list.head_page_id(), 0);
        assert_eq!(list.tail_page_id(), 0);
    }

    #[test]
    fn test_tombstone_list_append() {
        let mut list = TombstoneList::new();

        let t1 = Tombstone::new(EntityId([1u8; 16]), AttributeId([1u8; 16]), 1);
        let t2 = Tombstone::new(EntityId([2u8; 16]), AttributeId([2u8; 16]), 2);

        list.append(t1);
        assert_eq!(list.count(), 1);
        assert_eq!(list.buffered_count(), 1);

        list.append(t2);
        assert_eq!(list.count(), 2);
        assert_eq!(list.buffered_count(), 2);

        // Not yet flushed, so still "empty" on disk
        assert_eq!(list.head_page_id(), 0);
    }

    #[test]
    fn test_tombstone_list_needs_flush() {
        let mut list = TombstoneList::new();

        for i in 0..TombstoneList::WRITE_BUFFER_CAPACITY - 1 {
            list.append(Tombstone::new(
                EntityId([i as u8; 16]),
                AttributeId([0u8; 16]),
                i as u64 + 1,
            ));
            assert!(!list.needs_flush());
        }

        // One more pushes it to capacity
        list.append(Tombstone::new(
            EntityId([99u8; 16]),
            AttributeId([0u8; 16]),
            100,
        ));
        assert!(list.needs_flush());
    }

    #[test]
    fn test_tombstone_list_from_persisted() {
        let list = TombstoneList::from_persisted(10, 5, 20, 100, 1000);

        assert!(!list.is_empty());
        assert_eq!(list.head_page_id(), 10);
        assert_eq!(list.head_slot(), 5);
        assert_eq!(list.tail_page_id(), 20);
        assert_eq!(list.tail_slot(), 100);
        assert_eq!(list.count(), 1000);
    }

    #[test]
    fn test_tombstones_per_page() {
        // Verify the calculation
        assert_eq!(TOMBSTONES_PER_PAGE, (PAGE_SIZE - 32) / 40);
        assert_eq!(TOMBSTONES_PER_PAGE, 204);
    }
}
