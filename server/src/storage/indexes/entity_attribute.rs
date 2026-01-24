//! Entity-attribute index implementation.
//!
//! The entity-attribute index maps `entity_id` -> list of `attribute_id`s.
//! This enables efficient queries like "find all attributes for entity X".
//!
//! # Key Format
//!
//! Keys are 32 bytes: `(entity_id: [u8; 16], attribute_id: [u8; 16])`
//!
//! This format allows:
//! - Point lookup: check if (entity, attribute) pair exists
//! - Entity scan: iterate all attributes for a given entity
//!
//! # Value Format
//!
//! Values store MVCC metadata: `created_txn` (8 bytes) and `deleted_txn` (8 bytes).

use crate::storage::btree::{BTree, BTreeError, KEY_SIZE, Key};
use crate::storage::file::DatabaseFile;
use crate::storage::page::PageId;
use crate::storage::triple::{AttributeId, EntityId, TxnId};

/// MVCC value size: `created_txn` (8 bytes) and `deleted_txn` (8 bytes).
const ENTRY_VALUE_SIZE: usize = 16;

/// Entity-attribute index for efficient entity-based queries.
///
/// Maps `(entity_id, attribute_id)` -> MVCC metadata.
pub struct EntityAttributeIndex<'a> {
    tree: BTree<'a>,
}

impl<'a> EntityAttributeIndex<'a> {
    /// Create or open an entity-attribute index.
    ///
    /// If `root_page` is 0, creates a new empty index.
    pub fn new(
        file: &'a mut DatabaseFile,
        root_page: PageId,
    ) -> Result<Self, EntityAttributeIndexError> {
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

    /// Check if an (entity, attribute) pair exists in the index.
    pub fn contains(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<bool, EntityAttributeIndexError> {
        let key = make_entity_attribute_key(entity_id, attribute_id);
        Ok(self.tree.get(&key)?.is_some())
    }

    /// Get the MVCC metadata for an (entity, attribute) pair.
    ///
    /// Returns `(created_txn, deleted_txn)` if the entry exists.
    pub fn get(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<Option<(TxnId, TxnId)>, EntityAttributeIndexError> {
        let key = make_entity_attribute_key(entity_id, attribute_id);
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
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        snapshot_txn: TxnId,
    ) -> Result<bool, EntityAttributeIndexError> {
        match self.get(entity_id, attribute_id)? {
            Some((created_txn, deleted_txn)) => {
                let visible =
                    created_txn <= snapshot_txn && (deleted_txn == 0 || deleted_txn > snapshot_txn);
                Ok(visible)
            }
            None => Ok(false),
        }
    }

    /// Insert an (entity, attribute) pair into the index.
    pub fn insert(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        created_txn: TxnId,
    ) -> Result<(), EntityAttributeIndexError> {
        let key = make_entity_attribute_key(entity_id, attribute_id);
        let value = make_entry_value(created_txn, 0);
        self.tree.insert(key, value)?;
        Ok(())
    }

    /// Mark an (entity, attribute) pair as deleted.
    pub fn mark_deleted(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        deleted_txn: TxnId,
    ) -> Result<bool, EntityAttributeIndexError> {
        let key = make_entity_attribute_key(entity_id, attribute_id);

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
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<bool, EntityAttributeIndexError> {
        let key = make_entity_attribute_key(entity_id, attribute_id);
        Ok(self.tree.remove(&key)?.is_some())
    }

    /// Scan all attributes for a given entity.
    ///
    /// Returns an iterator over attribute IDs that the entity has.
    pub fn scan_entity(
        &mut self,
        entity_id: &EntityId,
    ) -> Result<EntityScanIterator<'_>, EntityAttributeIndexError> {
        // Start key: (entity_id, 0x00...)
        let start_key = make_entity_attribute_key(entity_id, &[0u8; 16]);
        let cursor = self.tree.iter_from(&start_key)?;

        Ok(EntityScanIterator {
            cursor,
            entity_id: *entity_id,
            snapshot_txn: None,
            done: false,
        })
    }

    /// Scan all visible attributes for a given entity at a snapshot.
    pub fn scan_entity_visible(
        &mut self,
        entity_id: &EntityId,
        snapshot_txn: TxnId,
    ) -> Result<EntityScanIterator<'_>, EntityAttributeIndexError> {
        let start_key = make_entity_attribute_key(entity_id, &[0u8; 16]);
        let cursor = self.tree.iter_from(&start_key)?;

        Ok(EntityScanIterator {
            cursor,
            entity_id: *entity_id,
            snapshot_txn: Some(snapshot_txn),
            done: false,
        })
    }

    /// Count all entries in the index.
    pub fn count(&mut self) -> Result<usize, EntityAttributeIndexError> {
        Ok(self.tree.count()?)
    }
}

/// Iterator over attributes for a specific entity.
pub struct EntityScanIterator<'a> {
    cursor: crate::storage::btree::BTreeIterator<'a>,
    entity_id: EntityId,
    snapshot_txn: Option<TxnId>,
    done: bool,
}

impl EntityScanIterator<'_> {
    /// Get the next attribute ID for this entity.
    pub fn next_attribute(&mut self) -> Result<Option<AttributeId>, EntityAttributeIndexError> {
        if self.done {
            return Ok(None);
        }

        loop {
            let Some((key, value)) = self.cursor.next_entry()? else {
                self.done = true;
                return Ok(None);
            };

            let (ent_id, attribute_id) = split_entity_attribute_key(&key);

            // Check if we're still on the same entity
            if ent_id != self.entity_id {
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

            return Ok(Some(attribute_id));
        }
    }
}

/// Create a key for the entity-attribute index.
fn make_entity_attribute_key(entity_id: &EntityId, attribute_id: &AttributeId) -> Key {
    let mut key = [0u8; KEY_SIZE];
    key[..16].copy_from_slice(entity_id);
    key[16..].copy_from_slice(attribute_id);
    key
}

/// Split an entity-attribute index key into its components.
fn split_entity_attribute_key(key: &Key) -> (EntityId, AttributeId) {
    let mut entity_id = [0u8; 16];
    let mut attribute_id = [0u8; 16];
    entity_id.copy_from_slice(&key[..16]);
    attribute_id.copy_from_slice(&key[16..]);
    (entity_id, attribute_id)
}

/// Create the value for an entity-attribute index entry.
fn make_entry_value(created_txn: TxnId, deleted_txn: TxnId) -> Vec<u8> {
    let mut value = Vec::with_capacity(ENTRY_VALUE_SIZE);
    value.extend_from_slice(&created_txn.to_le_bytes());
    value.extend_from_slice(&deleted_txn.to_le_bytes());
    value
}

/// Errors that can occur during entity-attribute index operations.
#[derive(Debug)]
pub enum EntityAttributeIndexError {
    /// B-tree operation failed.
    BTree(BTreeError),
}

impl std::fmt::Display for EntityAttributeIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BTree(e) => write!(f, "B-tree error: {e}"),
        }
    }
}

impl std::error::Error for EntityAttributeIndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BTree(e) => Some(e),
        }
    }
}

impl From<BTreeError> for EntityAttributeIndexError {
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
    fn test_entity_attribute_index_basic() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = EntityAttributeIndex::new(&mut file, 0).expect("create index");

        let entity1 = [1u8; 16];
        let attr1 = [10u8; 16];
        let attr2 = [20u8; 16];

        // Insert
        index.insert(&entity1, &attr1, 1).expect("insert");
        index.insert(&entity1, &attr2, 1).expect("insert");

        // Check contains
        assert!(index.contains(&entity1, &attr1).expect("contains"));
        assert!(index.contains(&entity1, &attr2).expect("contains"));
        assert!(!index.contains(&entity1, &[30u8; 16]).expect("contains"));

        // Get
        let (created, deleted) = index.get(&entity1, &attr1).expect("get").expect("exists");
        assert_eq!(created, 1);
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_entity_attribute_index_scan() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = EntityAttributeIndex::new(&mut file, 0).expect("create index");

        let entity1 = [1u8; 16];
        let entity2 = [2u8; 16];

        // Insert attributes for entity1
        for i in 0..5u8 {
            let mut attr = [0u8; 16];
            attr[0] = i;
            index.insert(&entity1, &attr, 1).expect("insert");
        }

        // Insert attributes for entity2
        for i in 0..3u8 {
            let mut attr = [0u8; 16];
            attr[0] = i + 100;
            index.insert(&entity2, &attr, 1).expect("insert");
        }

        // Scan entity1
        let mut scan = index.scan_entity(&entity1).expect("scan");
        let mut count = 0;
        while scan.next_attribute().expect("next").is_some() {
            count += 1;
        }
        assert_eq!(count, 5);

        // Scan entity2
        let mut scan = index.scan_entity(&entity2).expect("scan");
        let mut count = 0;
        while scan.next_attribute().expect("next").is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_entity_attribute_index_visibility() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = EntityAttributeIndex::new(&mut file, 0).expect("create index");

        let entity = [1u8; 16];
        let attr = [10u8; 16];

        // Insert at txn 10
        index.insert(&entity, &attr, 10).expect("insert");

        // Visible to txn >= 10
        assert!(!index.is_visible(&entity, &attr, 9).expect("vis"));
        assert!(index.is_visible(&entity, &attr, 10).expect("vis"));
        assert!(index.is_visible(&entity, &attr, 100).expect("vis"));

        // Mark deleted at txn 50
        index.mark_deleted(&entity, &attr, 50).expect("delete");

        // Now visible only in [10, 50)
        assert!(!index.is_visible(&entity, &attr, 9).expect("vis"));
        assert!(index.is_visible(&entity, &attr, 10).expect("vis"));
        assert!(index.is_visible(&entity, &attr, 49).expect("vis"));
        assert!(!index.is_visible(&entity, &attr, 50).expect("vis"));
    }

    #[test]
    fn test_entity_attribute_index_remove() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = EntityAttributeIndex::new(&mut file, 0).expect("create index");

        let entity = [1u8; 16];
        let attr = [10u8; 16];

        index.insert(&entity, &attr, 1).expect("insert");
        assert!(index.contains(&entity, &attr).expect("contains"));

        index.remove(&entity, &attr).expect("remove");
        assert!(!index.contains(&entity, &attr).expect("contains"));
    }

    #[test]
    fn test_entity_scan_with_visibility() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = EntityAttributeIndex::new(&mut file, 0).expect("create index");

        let entity = [1u8; 16];

        // Insert attributes at different txns
        let attr1 = [1u8; 16]; // created at 10
        let attr2 = [2u8; 16]; // created at 20
        let attr3 = [3u8; 16]; // created at 30

        index.insert(&entity, &attr1, 10).expect("insert");
        index.insert(&entity, &attr2, 20).expect("insert");
        index.insert(&entity, &attr3, 30).expect("insert");

        // Scan at snapshot 15 - should only see attr1
        let mut scan = index.scan_entity_visible(&entity, 15).expect("scan");
        let mut attrs = Vec::new();
        while let Some(a) = scan.next_attribute().expect("next") {
            attrs.push(a);
        }
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0], attr1);

        // Scan at snapshot 25 - should see attr1 and attr2
        let mut scan = index.scan_entity_visible(&entity, 25).expect("scan");
        let mut attrs = Vec::new();
        while let Some(a) = scan.next_attribute().expect("next") {
            attrs.push(a);
        }
        assert_eq!(attrs.len(), 2);
    }
}
