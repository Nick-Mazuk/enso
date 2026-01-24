//! Attribute index implementation.
//!
//! The attribute index maps `attribute_id` -> list of `entity_id`s.
//! This enables efficient queries like "find all entities with attribute X".
//!
//! # Key Format
//!
//! Keys are 32 bytes: `(attribute_id: [u8; 16], entity_id: [u8; 16])`
//!
//! This format allows:
//! - Point lookup: check if (attribute, entity) pair exists
//! - Attribute scan: iterate all entities with a given attribute
//!
//! # Value Format
//!
//! Values are minimal (just a marker byte) since the key contains all information.
//! The value stores the `created_txn` for MVCC visibility.

use crate::storage::btree::{BTree, BTreeError, KEY_SIZE, Key};
use crate::storage::file::DatabaseFile;
use crate::storage::page::PageId;
use crate::storage::triple::{AttributeId, EntityId, TxnId};

/// Marker value size: just the `created_txn` (8 bytes) and `deleted_txn` (8 bytes).
const ENTRY_VALUE_SIZE: usize = 16;

/// Attribute index for efficient attribute-based queries.
///
/// Maps `(attribute_id, entity_id)` -> MVCC metadata.
pub struct AttributeIndex<'a> {
    tree: BTree<'a>,
}

impl<'a> AttributeIndex<'a> {
    /// Create or open an attribute index.
    ///
    /// If `root_page` is 0, creates a new empty index.
    pub fn new(file: &'a mut DatabaseFile, root_page: PageId) -> Result<Self, AttributeIndexError> {
        let tree = BTree::new(file, root_page)?;
        Ok(Self { tree })
    }

    /// Get the root page ID.
    #[must_use]
    pub const fn root_page(&self) -> PageId {
        self.tree.root_page()
    }

    /// Get mutable access to the underlying database file.
    pub fn file_mut(&mut self) -> &mut DatabaseFile {
        self.tree.file_mut()
    }

    /// Check if an (attribute, entity) pair exists in the index.
    pub fn contains(
        &mut self,
        attribute_id: &AttributeId,
        entity_id: &EntityId,
    ) -> Result<bool, AttributeIndexError> {
        let key = make_attribute_key(attribute_id, entity_id);
        Ok(self.tree.get(&key)?.is_some())
    }

    /// Get the MVCC metadata for an (attribute, entity) pair.
    ///
    /// Returns `(created_txn, deleted_txn)` if the entry exists.
    pub fn get(
        &mut self,
        attribute_id: &AttributeId,
        entity_id: &EntityId,
    ) -> Result<Option<(TxnId, TxnId)>, AttributeIndexError> {
        let key = make_attribute_key(attribute_id, entity_id);
        match self.tree.get(&key)? {
            Some(value) if value.len() >= ENTRY_VALUE_SIZE => {
                let created_txn = u64::from_le_bytes([
                    value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                ]);
                let deleted_txn = u64::from_le_bytes([
                    value[8], value[9], value[10], value[11], value[12], value[13], value[14],
                    value[15],
                ]);
                Ok(Some((created_txn, deleted_txn)))
            }
            _ => Ok(None),
        }
    }

    /// Check if an entry is visible to a given transaction.
    pub fn is_visible(
        &mut self,
        attribute_id: &AttributeId,
        entity_id: &EntityId,
        snapshot_txn: TxnId,
    ) -> Result<bool, AttributeIndexError> {
        match self.get(attribute_id, entity_id)? {
            Some((created_txn, deleted_txn)) => {
                let visible =
                    created_txn <= snapshot_txn && (deleted_txn == 0 || deleted_txn > snapshot_txn);
                Ok(visible)
            }
            None => Ok(false),
        }
    }

    /// Insert an (attribute, entity) pair into the index.
    pub fn insert(
        &mut self,
        attribute_id: &AttributeId,
        entity_id: &EntityId,
        created_txn: TxnId,
    ) -> Result<(), AttributeIndexError> {
        let key = make_attribute_key(attribute_id, entity_id);
        let value = make_entry_value(created_txn, 0);
        self.tree.insert(key, value)?;
        Ok(())
    }

    /// Mark an (attribute, entity) pair as deleted.
    pub fn mark_deleted(
        &mut self,
        attribute_id: &AttributeId,
        entity_id: &EntityId,
        deleted_txn: TxnId,
    ) -> Result<bool, AttributeIndexError> {
        let key = make_attribute_key(attribute_id, entity_id);

        // Get existing entry to preserve created_txn
        let Some(existing) = self.tree.get(&key)? else {
            return Ok(false);
        };

        if existing.len() < ENTRY_VALUE_SIZE {
            return Ok(false);
        }

        let created_txn = u64::from_le_bytes([
            existing[0],
            existing[1],
            existing[2],
            existing[3],
            existing[4],
            existing[5],
            existing[6],
            existing[7],
        ]);

        let value = make_entry_value(created_txn, deleted_txn);
        self.tree.insert(key, value)?;
        Ok(true)
    }

    /// Remove an entry completely (for garbage collection).
    pub fn remove(
        &mut self,
        attribute_id: &AttributeId,
        entity_id: &EntityId,
    ) -> Result<bool, AttributeIndexError> {
        let key = make_attribute_key(attribute_id, entity_id);
        Ok(self.tree.remove(&key)?.is_some())
    }

    /// Scan all entities with a given attribute.
    ///
    /// Returns an iterator over entity IDs that have the specified attribute.
    pub fn scan_attribute(
        &mut self,
        attribute_id: &AttributeId,
    ) -> Result<AttributeScanIterator<'_>, AttributeIndexError> {
        // Start key: (attribute_id, 0x00...)
        let start_key = make_attribute_key(attribute_id, &[0u8; 16]);
        let cursor = self.tree.iter_from(&start_key)?;

        Ok(AttributeScanIterator {
            cursor,
            attribute_id: *attribute_id,
            snapshot_txn: None,
            done: false,
        })
    }

    /// Scan all visible entities with a given attribute at a snapshot.
    pub fn scan_attribute_visible(
        &mut self,
        attribute_id: &AttributeId,
        snapshot_txn: TxnId,
    ) -> Result<AttributeScanIterator<'_>, AttributeIndexError> {
        let start_key = make_attribute_key(attribute_id, &[0u8; 16]);
        let cursor = self.tree.iter_from(&start_key)?;

        Ok(AttributeScanIterator {
            cursor,
            attribute_id: *attribute_id,
            snapshot_txn: Some(snapshot_txn),
            done: false,
        })
    }

    /// Count all entries in the index.
    pub fn count(&mut self) -> Result<usize, AttributeIndexError> {
        Ok(self.tree.count()?)
    }
}

/// Iterator over entities with a specific attribute.
pub struct AttributeScanIterator<'a> {
    cursor: crate::storage::btree::BTreeIterator<'a>,
    attribute_id: AttributeId,
    snapshot_txn: Option<TxnId>,
    done: bool,
}

impl AttributeScanIterator<'_> {
    /// Get the next entity ID with this attribute.
    pub fn next_entity(&mut self) -> Result<Option<EntityId>, AttributeIndexError> {
        if self.done {
            return Ok(None);
        }

        loop {
            let Some((key, value)) = self.cursor.next_entry()? else {
                self.done = true;
                return Ok(None);
            };

            let (attr_id, entity_id) = split_attribute_key(&key);

            // Check if we're still on the same attribute
            if attr_id != self.attribute_id {
                self.done = true;
                return Ok(None);
            }

            // Apply visibility filter if set
            if let Some(snapshot_txn) = self.snapshot_txn {
                if value.len() >= ENTRY_VALUE_SIZE {
                    let created_txn = u64::from_le_bytes([
                        value[0], value[1], value[2], value[3], value[4], value[5], value[6],
                        value[7],
                    ]);
                    let deleted_txn = u64::from_le_bytes([
                        value[8], value[9], value[10], value[11], value[12], value[13], value[14],
                        value[15],
                    ]);

                    let visible = created_txn <= snapshot_txn
                        && (deleted_txn == 0 || deleted_txn > snapshot_txn);

                    if !visible {
                        continue; // Skip non-visible entries
                    }
                }
            }

            return Ok(Some(entity_id));
        }
    }
}

/// Create a key for the attribute index.
fn make_attribute_key(attribute_id: &AttributeId, entity_id: &EntityId) -> Key {
    let mut key = [0u8; KEY_SIZE];
    key[..16].copy_from_slice(attribute_id);
    key[16..].copy_from_slice(entity_id);
    key
}

/// Split an attribute index key into its components.
fn split_attribute_key(key: &Key) -> (AttributeId, EntityId) {
    let mut attribute_id = [0u8; 16];
    let mut entity_id = [0u8; 16];
    attribute_id.copy_from_slice(&key[..16]);
    entity_id.copy_from_slice(&key[16..]);
    (attribute_id, entity_id)
}

/// Create the value for an attribute index entry.
fn make_entry_value(created_txn: TxnId, deleted_txn: TxnId) -> Vec<u8> {
    let mut value = Vec::with_capacity(ENTRY_VALUE_SIZE);
    value.extend_from_slice(&created_txn.to_le_bytes());
    value.extend_from_slice(&deleted_txn.to_le_bytes());
    value
}

/// Errors that can occur during attribute index operations.
#[derive(Debug)]
pub enum AttributeIndexError {
    /// B-tree operation failed.
    BTree(BTreeError),
}

impl std::fmt::Display for AttributeIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BTree(e) => write!(f, "B-tree error: {e}"),
        }
    }
}

impl std::error::Error for AttributeIndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BTree(e) => Some(e),
        }
    }
}

impl From<BTreeError> for AttributeIndexError {
    fn from(e: BTreeError) -> Self {
        Self::BTree(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::file::DatabaseFile;
    use tempfile::tempdir;

    fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_attribute_index_basic() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = AttributeIndex::new(&mut file, 0).expect("create index");

        let attr1 = [1u8; 16];
        let entity1 = [10u8; 16];
        let entity2 = [20u8; 16];

        // Insert
        index.insert(&attr1, &entity1, 1).expect("insert");
        index.insert(&attr1, &entity2, 1).expect("insert");

        // Check contains
        assert!(index.contains(&attr1, &entity1).expect("contains"));
        assert!(index.contains(&attr1, &entity2).expect("contains"));
        assert!(!index.contains(&attr1, &[30u8; 16]).expect("contains"));

        // Get
        let (created, deleted) = index.get(&attr1, &entity1).expect("get").expect("exists");
        assert_eq!(created, 1);
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_attribute_index_scan() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = AttributeIndex::new(&mut file, 0).expect("create index");

        let attr1 = [1u8; 16];
        let attr2 = [2u8; 16];

        // Insert entities for attr1
        for i in 0..5u8 {
            let mut entity = [0u8; 16];
            entity[0] = i;
            index.insert(&attr1, &entity, 1).expect("insert");
        }

        // Insert entities for attr2
        for i in 0..3u8 {
            let mut entity = [0u8; 16];
            entity[0] = i + 100;
            index.insert(&attr2, &entity, 1).expect("insert");
        }

        // Scan attr1
        let mut scan = index.scan_attribute(&attr1).expect("scan");
        let mut count = 0;
        while scan.next_entity().expect("next").is_some() {
            count += 1;
        }
        assert_eq!(count, 5);

        // Scan attr2
        let mut scan = index.scan_attribute(&attr2).expect("scan");
        let mut count = 0;
        while scan.next_entity().expect("next").is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_attribute_index_visibility() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = AttributeIndex::new(&mut file, 0).expect("create index");

        let attr = [1u8; 16];
        let entity = [10u8; 16];

        // Insert at txn 10
        index.insert(&attr, &entity, 10).expect("insert");

        // Visible to txn >= 10
        assert!(!index.is_visible(&attr, &entity, 9).expect("vis"));
        assert!(index.is_visible(&attr, &entity, 10).expect("vis"));
        assert!(index.is_visible(&attr, &entity, 100).expect("vis"));

        // Mark deleted at txn 50
        index.mark_deleted(&attr, &entity, 50).expect("delete");

        // Now visible only in [10, 50)
        assert!(!index.is_visible(&attr, &entity, 9).expect("vis"));
        assert!(index.is_visible(&attr, &entity, 10).expect("vis"));
        assert!(index.is_visible(&attr, &entity, 49).expect("vis"));
        assert!(!index.is_visible(&attr, &entity, 50).expect("vis"));
    }

    #[test]
    fn test_attribute_index_remove() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = AttributeIndex::new(&mut file, 0).expect("create index");

        let attr = [1u8; 16];
        let entity = [10u8; 16];

        index.insert(&attr, &entity, 1).expect("insert");
        assert!(index.contains(&attr, &entity).expect("contains"));

        index.remove(&attr, &entity).expect("remove");
        assert!(!index.contains(&attr, &entity).expect("contains"));
    }

    #[test]
    fn test_attribute_scan_with_visibility() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = AttributeIndex::new(&mut file, 0).expect("create index");

        let attr = [1u8; 16];

        // Insert entities at different txns
        let entity1 = [1u8; 16]; // created at 10
        let entity2 = [2u8; 16]; // created at 20
        let entity3 = [3u8; 16]; // created at 30

        index.insert(&attr, &entity1, 10).expect("insert");
        index.insert(&attr, &entity2, 20).expect("insert");
        index.insert(&attr, &entity3, 30).expect("insert");

        // Scan at snapshot 15 - should only see entity1
        let mut scan = index.scan_attribute_visible(&attr, 15).expect("scan");
        let mut entities = Vec::new();
        while let Some(e) = scan.next_entity().expect("next") {
            entities.push(e);
        }
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0], entity1);

        // Scan at snapshot 25 - should see entity1 and entity2
        let mut scan = index.scan_attribute_visible(&attr, 25).expect("scan");
        let mut entities = Vec::new();
        while let Some(e) = scan.next_entity().expect("next") {
            entities.push(e);
        }
        assert_eq!(entities.len(), 2);
    }
}
