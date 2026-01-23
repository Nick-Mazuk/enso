//! Primary index implementation.
//!
//! The primary index maps (`entity_id`, `attribute_id`) to the full triple record.
//! It is backed by a B-tree and provides efficient point lookups and entity scans.

use crate::storage::btree::{BTree, BTreeError, make_key, split_key};
use crate::storage::file::DatabaseFile;
use crate::storage::page::PageId;
use crate::storage::triple::{AttributeId, EntityId, TripleError, TripleRecord, TxnId};

/// Primary index for triple storage.
///
/// Maps (`entity_id`, `attribute_id`) -> `TripleRecord`.
pub struct PrimaryIndex<'a> {
    tree: BTree<'a>,
}

impl<'a> PrimaryIndex<'a> {
    /// Create or open a primary index.
    ///
    /// If `root_page` is 0, creates a new empty index.
    pub fn new(file: &'a mut DatabaseFile, root_page: PageId) -> Result<Self, PrimaryIndexError> {
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

    /// Look up a single triple by entity and attribute ID.
    pub fn get(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<Option<TripleRecord>, PrimaryIndexError> {
        let key = make_key(entity_id, attribute_id);
        let value = self.tree.get(&key)?;

        match value {
            Some(bytes) => {
                let record = TripleRecord::from_bytes(&bytes)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Look up a single triple, checking visibility against a snapshot.
    pub fn get_visible(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        snapshot_txn: TxnId,
    ) -> Result<Option<TripleRecord>, PrimaryIndexError> {
        let record = self.get(entity_id, attribute_id)?;

        match record {
            Some(r) if r.is_visible_to(snapshot_txn) => Ok(Some(r)),
            _ => Ok(None),
        }
    }

    /// Insert a new triple record.
    ///
    /// Returns the old record if updating an existing triple.
    pub fn insert(
        &mut self,
        record: &TripleRecord,
    ) -> Result<Option<TripleRecord>, PrimaryIndexError> {
        let key = make_key(&record.entity_id, &record.attribute_id);
        let value = record.to_bytes();

        let old_value = self.tree.insert(key, value)?;

        match old_value {
            Some(bytes) => {
                let old_record = TripleRecord::from_bytes(&bytes)?;
                Ok(Some(old_record))
            }
            None => Ok(None),
        }
    }

    /// Mark a triple as deleted by setting its `deleted_txn`.
    ///
    /// Returns the updated record, or None if not found.
    pub fn mark_deleted(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        deleted_txn: TxnId,
    ) -> Result<Option<TripleRecord>, PrimaryIndexError> {
        let Some(mut record) = self.get(entity_id, attribute_id)? else {
            return Ok(None);
        };

        record.deleted_txn = deleted_txn;
        self.insert(&record)
    }

    /// Remove a triple completely from the index.
    ///
    /// This is for garbage collection, not for regular deletion (use `mark_deleted`).
    pub fn remove(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<Option<TripleRecord>, PrimaryIndexError> {
        let key = make_key(entity_id, attribute_id);
        let old_value = self.tree.remove(&key)?;

        match old_value {
            Some(bytes) => {
                let record = TripleRecord::from_bytes(&bytes)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Scan all triples for an entity.
    ///
    /// Returns an iterator over all triples where `entity_id` matches.
    pub fn scan_entity(
        &mut self,
        entity_id: &EntityId,
    ) -> Result<EntityScanIterator<'_>, PrimaryIndexError> {
        // Create key with entity_id and zeroed attribute_id (start of range)
        let start_key = make_key(entity_id, &[0u8; 16]);
        let cursor = self.tree.iter_from(&start_key)?;

        Ok(EntityScanIterator {
            cursor,
            entity_id: *entity_id,
            done: false,
        })
    }

    /// Count the total number of triples in the index.
    pub fn count(&mut self) -> Result<usize, PrimaryIndexError> {
        Ok(self.tree.count()?)
    }

    /// Create a cursor over all triples in key order.
    pub fn cursor(&mut self) -> Result<PrimaryIndexCursor<'_>, PrimaryIndexError> {
        let cursor = self.tree.cursor()?;
        Ok(PrimaryIndexCursor { cursor })
    }
}

/// Cursor over all triples in the primary index.
pub struct PrimaryIndexCursor<'a> {
    cursor: crate::storage::btree::BTreeIterator<'a>,
}

impl PrimaryIndexCursor<'_> {
    /// Get the next triple record.
    pub fn next_record(&mut self) -> Result<Option<TripleRecord>, PrimaryIndexError> {
        match self.cursor.next_entry()? {
            Some((_, value)) => {
                let record = TripleRecord::from_bytes(&value)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }
}

/// Iterator over triples for a specific entity.
pub struct EntityScanIterator<'a> {
    cursor: crate::storage::btree::BTreeIterator<'a>,
    entity_id: EntityId,
    done: bool,
}

impl EntityScanIterator<'_> {
    /// Get the next triple for this entity.
    pub fn next_record(&mut self) -> Result<Option<TripleRecord>, PrimaryIndexError> {
        if self.done {
            return Ok(None);
        }

        if let Some((key, value)) = self.cursor.next_entry()? {
            let (entity_id, _) = split_key(&key);

            // Check if we're still on the same entity
            if entity_id != self.entity_id {
                self.done = true;
                return Ok(None);
            }

            let record = TripleRecord::from_bytes(&value)?;
            Ok(Some(record))
        } else {
            self.done = true;
            Ok(None)
        }
    }
}

/// Errors that can occur during primary index operations.
#[derive(Debug)]
pub enum PrimaryIndexError {
    /// B-tree operation failed.
    BTree(BTreeError),
    /// Triple serialization/deserialization failed.
    Triple(TripleError),
}

impl std::fmt::Display for PrimaryIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BTree(e) => write!(f, "B-tree error: {e}"),
            Self::Triple(e) => write!(f, "triple error: {e}"),
        }
    }
}

impl std::error::Error for PrimaryIndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BTree(e) => Some(e),
            Self::Triple(e) => Some(e),
        }
    }
}

impl From<BTreeError> for PrimaryIndexError {
    fn from(e: BTreeError) -> Self {
        Self::BTree(e)
    }
}

impl From<TripleError> for PrimaryIndexError {
    fn from(e: TripleError) -> Self {
        Self::Triple(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::file::DatabaseFile;
    use crate::storage::superblock::HlcTimestamp;
    use crate::storage::triple::TripleValue;
    use tempfile::tempdir;

    fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_primary_index_basic_operations() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = PrimaryIndex::new(&mut file, 0).expect("create index");

        let entity_id = [1u8; 16];
        let attribute_id = [2u8; 16];

        // Insert
        let record = TripleRecord::new(
            entity_id,
            attribute_id,
            1,
            HlcTimestamp::new(1000, 0),
            TripleValue::String("hello".to_string()),
        );

        let old = index.insert(&record).expect("insert");
        assert!(old.is_none());

        // Get
        let fetched = index.get(&entity_id, &attribute_id).expect("get");
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.entity_id, entity_id);
        assert_eq!(fetched.attribute_id, attribute_id);
        assert_eq!(fetched.value, TripleValue::String("hello".to_string()));

        // Update
        let new_record = TripleRecord::new(
            entity_id,
            attribute_id,
            2,
            HlcTimestamp::new(2000, 0),
            TripleValue::String("world".to_string()),
        );

        let old = index.insert(&new_record).expect("update");
        assert!(old.is_some());
        assert_eq!(old.unwrap().value, TripleValue::String("hello".to_string()));

        // Verify update
        let fetched = index
            .get(&entity_id, &attribute_id)
            .expect("get after update");
        assert_eq!(
            fetched.unwrap().value,
            TripleValue::String("world".to_string())
        );
    }

    #[test]
    fn test_primary_index_visibility() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = PrimaryIndex::new(&mut file, 0).expect("create index");

        let entity_id = [1u8; 16];
        let attribute_id = [2u8; 16];

        let record = TripleRecord::new(
            entity_id,
            attribute_id,
            10,
            HlcTimestamp::new(1000, 0),
            TripleValue::Number(42.0),
        );

        index.insert(&record).expect("insert");

        // Visible to txn 10 and later
        assert!(
            index
                .get_visible(&entity_id, &attribute_id, 9)
                .expect("get")
                .is_none()
        );
        assert!(
            index
                .get_visible(&entity_id, &attribute_id, 10)
                .expect("get")
                .is_some()
        );
        assert!(
            index
                .get_visible(&entity_id, &attribute_id, 100)
                .expect("get")
                .is_some()
        );

        // Mark deleted at txn 50
        index
            .mark_deleted(&entity_id, &attribute_id, 50)
            .expect("mark deleted");

        // Now visible only in range [10, 50)
        assert!(
            index
                .get_visible(&entity_id, &attribute_id, 9)
                .expect("get")
                .is_none()
        );
        assert!(
            index
                .get_visible(&entity_id, &attribute_id, 10)
                .expect("get")
                .is_some()
        );
        assert!(
            index
                .get_visible(&entity_id, &attribute_id, 49)
                .expect("get")
                .is_some()
        );
        assert!(
            index
                .get_visible(&entity_id, &attribute_id, 50)
                .expect("get")
                .is_none()
        );
    }

    #[test]
    fn test_primary_index_entity_scan() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = PrimaryIndex::new(&mut file, 0).expect("create index");

        let entity1 = [1u8; 16];
        let entity2 = [2u8; 16];

        // Insert attributes for entity1
        for i in 0..5u8 {
            let mut attr = [0u8; 16];
            attr[0] = i;
            let record = TripleRecord::new(
                entity1,
                attr,
                1,
                HlcTimestamp::new(1000, 0),
                TripleValue::Number(f64::from(i)),
            );
            index.insert(&record).expect("insert");
        }

        // Insert attributes for entity2
        for i in 0..3u8 {
            let mut attr = [0u8; 16];
            attr[0] = i;
            let record = TripleRecord::new(
                entity2,
                attr,
                1,
                HlcTimestamp::new(1000, 0),
                TripleValue::Number(f64::from(i + 100)),
            );
            index.insert(&record).expect("insert");
        }

        // Scan entity1
        let mut scan = index.scan_entity(&entity1).expect("scan");
        let mut count = 0;
        while let Some(record) = scan.next_record().expect("next") {
            assert_eq!(record.entity_id, entity1);
            count += 1;
        }
        assert_eq!(count, 5);

        // Scan entity2
        let mut scan = index.scan_entity(&entity2).expect("scan");
        let mut count = 0;
        while let Some(record) = scan.next_record().expect("next") {
            assert_eq!(record.entity_id, entity2);
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_primary_index_remove() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut index = PrimaryIndex::new(&mut file, 0).expect("create index");

        let entity_id = [1u8; 16];
        let attribute_id = [2u8; 16];

        let record = TripleRecord::new(
            entity_id,
            attribute_id,
            1,
            HlcTimestamp::new(1000, 0),
            TripleValue::Boolean(true),
        );

        index.insert(&record).expect("insert");
        assert!(index.get(&entity_id, &attribute_id).expect("get").is_some());

        let removed = index.remove(&entity_id, &attribute_id).expect("remove");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().value, TripleValue::Boolean(true));

        assert!(
            index
                .get(&entity_id, &attribute_id)
                .expect("get after remove")
                .is_none()
        );
    }

    #[test]
    fn test_primary_index_persistence() {
        let (_dir, path) = create_test_db();

        let root_page;

        // Create and populate index
        {
            let mut file = DatabaseFile::create(&path).expect("create db");
            let mut index = PrimaryIndex::new(&mut file, 0).expect("create index");

            for i in 0..50u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                let record = TripleRecord::new(
                    entity,
                    [0u8; 16],
                    1,
                    HlcTimestamp::new(1000, 0),
                    TripleValue::Number(f64::from(i)),
                );
                index.insert(&record).expect("insert");
            }

            root_page = index.root_page();

            file.superblock_mut().primary_index_root = root_page;
            file.write_superblock().expect("write superblock");
            file.sync().expect("sync");
        }

        // Reopen and verify
        {
            let mut file = DatabaseFile::open(&path).expect("open db");
            let stored_root = file.superblock().primary_index_root;
            assert_eq!(stored_root, root_page);

            let mut index = PrimaryIndex::new(&mut file, stored_root).expect("open index");

            for i in 0..50u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                let record = index.get(&entity, &[0u8; 16]).expect("get");
                assert!(record.is_some());
                assert_eq!(record.unwrap().value, TripleValue::Number(f64::from(i)));
            }
        }
    }
}
