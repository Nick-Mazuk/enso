//! Simple transaction implementation for Phase 1.
//!
//! This is a single-threaded transaction that provides:
//! - Basic durability via fsync on commit
//! - Simple read/write operations
//!
//! Phase 3 will add MVCC and multi-reader support.

use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::indexes::primary::{
    EntityScanIterator, PrimaryIndex, PrimaryIndexCursor, PrimaryIndexError,
};
use crate::storage::superblock::HlcTimestamp;
use crate::storage::triple::{
    AttributeId, EntityId, TripleError, TripleRecord, TripleValue, TxnId,
};

/// A simple transaction for Phase 1.
///
/// Provides basic read/write operations with durability via fsync on commit.
/// Single-threaded only - MVCC will be added in Phase 3.
pub struct Transaction<'a> {
    index: PrimaryIndex<'a>,
    txn_id: TxnId,
    hlc: HlcTimestamp,
    committed: bool,
}

impl<'a> Transaction<'a> {
    /// Begin a new transaction.
    ///
    /// The caller must ensure only one transaction is active at a time (Phase 1 limitation).
    pub fn begin(file: &'a mut DatabaseFile) -> Result<Self, TransactionError> {
        // Get next transaction ID
        let txn_id = file.superblock().next_txn_id;

        // Get current HLC (in a real implementation, this would come from a clock)
        let hlc = file.superblock().last_checkpoint_hlc;

        // Get or create primary index
        let root_page = file.superblock().primary_index_root;

        // Create primary index (which owns the file reference)
        let index = PrimaryIndex::new(file, root_page)?;

        Ok(Self {
            index,
            txn_id,
            hlc,
            committed: false,
        })
    }

    /// Get the transaction ID.
    #[must_use]
    pub const fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Get the transaction's HLC timestamp.
    #[must_use]
    pub const fn hlc(&self) -> HlcTimestamp {
        self.hlc
    }

    /// Look up a single triple by entity and attribute ID.
    pub fn get(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<Option<TripleRecord>, TransactionError> {
        // Phase 1: simple get without MVCC visibility checks
        // We filter out deleted records (deleted_txn != 0) since we still track deletion
        match self.index.get(entity_id, attribute_id)? {
            Some(record) if !record.is_deleted() => Ok(Some(record)),
            _ => Ok(None),
        }
    }

    /// Scan all triples for an entity.
    pub fn scan_entity(
        &mut self,
        entity_id: &EntityId,
    ) -> Result<EntityScanIterator<'_>, TransactionError> {
        Ok(self.index.scan_entity(entity_id)?)
    }

    /// Create a cursor over all triples.
    pub fn cursor(&mut self) -> Result<PrimaryIndexCursor<'_>, TransactionError> {
        Ok(self.index.cursor()?)
    }

    /// Insert or update a triple.
    ///
    /// The triple will be created with the transaction's ID and HLC timestamp.
    /// If a triple already exists for this (entity, attribute), it will be overwritten.
    pub fn insert(
        &mut self,
        entity_id: EntityId,
        attribute_id: AttributeId,
        value: TripleValue,
    ) -> Result<(), TransactionError> {
        let record = TripleRecord::new(entity_id, attribute_id, self.txn_id, self.hlc, value);
        // Phase 1: simple insert/overwrite without MVCC versioning
        self.index.insert(&record)?;
        Ok(())
    }

    /// Update an existing triple.
    ///
    /// Returns an error if the triple does not exist.
    pub fn update(
        &mut self,
        entity_id: EntityId,
        attribute_id: AttributeId,
        value: TripleValue,
    ) -> Result<(), TransactionError> {
        // Check that the triple exists and is not deleted
        if self.get(&entity_id, &attribute_id)?.is_none() {
            return Err(TransactionError::NotFound);
        }

        self.insert(entity_id, attribute_id, value)
    }

    /// Delete a triple.
    ///
    /// Returns an error if the triple does not exist.
    /// The triple is marked as deleted rather than removed, preparing for MVCC in Phase 3.
    pub fn delete(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<(), TransactionError> {
        // Check that the triple exists and is not already deleted
        if self.get(entity_id, attribute_id)?.is_none() {
            return Err(TransactionError::NotFound);
        }

        self.index
            .mark_deleted(entity_id, attribute_id, self.txn_id)?;
        Ok(())
    }

    /// Commit the transaction.
    ///
    /// This persists all changes to disk with fsync.
    pub fn commit(mut self) -> Result<(), TransactionError> {
        // Update superblock with new root page and transaction ID
        let root_page = self.index.root_page();
        let file = self.index.file_mut();
        file.superblock_mut().primary_index_root = root_page;
        file.superblock_mut().next_txn_id = self.txn_id + 1;

        // Write superblock
        file.write_superblock()?;

        // Sync to disk
        file.sync()?;

        self.committed = true;
        Ok(())
    }

    /// Abort the transaction.
    ///
    /// In Phase 1, this simply discards the transaction without cleanup.
    /// A real implementation would need to roll back in-memory changes.
    pub fn abort(mut self) {
        // In Phase 1, we don't have proper rollback
        // The changes are lost when the Transaction is dropped
        self.committed = true; // Prevent drop warning
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // Transaction was dropped without commit or abort
            // In a real implementation, we'd log a warning
        }
    }
}

/// Errors that can occur during transaction operations.
#[derive(Debug)]
pub enum TransactionError {
    /// File I/O error.
    File(FileError),
    /// Primary index error.
    Index(PrimaryIndexError),
    /// Triple error.
    Triple(TripleError),
    /// Triple not found for update/delete.
    NotFound,
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(e) => write!(f, "file error: {e}"),
            Self::Index(e) => write!(f, "index error: {e}"),
            Self::Triple(e) => write!(f, "triple error: {e}"),
            Self::NotFound => write!(f, "triple not found"),
        }
    }
}

impl std::error::Error for TransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(e) => Some(e),
            Self::Index(e) => Some(e),
            Self::Triple(e) => Some(e),
            Self::NotFound => None,
        }
    }
}

impl From<FileError> for TransactionError {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

impl From<PrimaryIndexError> for TransactionError {
    fn from(e: PrimaryIndexError) -> Self {
        Self::Index(e)
    }
}

impl From<TripleError> for TransactionError {
    fn from(e: TripleError) -> Self {
        Self::Triple(e)
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
    fn test_transaction_insert_and_get() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        {
            let mut txn = Transaction::begin(&mut file).expect("begin txn");

            let entity_id = [1u8; 16];
            let attribute_id = [2u8; 16];

            txn.insert(
                entity_id,
                attribute_id,
                TripleValue::String("hello".to_string()),
            )
            .expect("insert");

            let record = txn.get(&entity_id, &attribute_id).expect("get");
            assert!(record.is_some());
            assert_eq!(
                record.unwrap().value,
                TripleValue::String("hello".to_string())
            );

            txn.commit().expect("commit");
        }

        // Verify persistence
        {
            let mut file = DatabaseFile::open(&path).expect("open db");
            let mut txn = Transaction::begin(&mut file).expect("begin txn");

            let entity_id = [1u8; 16];
            let attribute_id = [2u8; 16];

            let record = txn
                .get(&entity_id, &attribute_id)
                .expect("get after reopen");
            assert!(record.is_some());
            assert_eq!(
                record.unwrap().value,
                TripleValue::String("hello".to_string())
            );

            txn.abort();
        }
    }

    #[test]
    fn test_transaction_update() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut txn = Transaction::begin(&mut file).expect("begin txn");

        let entity_id = [1u8; 16];
        let attribute_id = [2u8; 16];

        // Insert
        txn.insert(entity_id, attribute_id, TripleValue::Number(1.0))
            .expect("insert");

        // Update
        txn.update(entity_id, attribute_id, TripleValue::Number(2.0))
            .expect("update");

        // Verify
        let record = txn.get(&entity_id, &attribute_id).expect("get");
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, TripleValue::Number(2.0));

        txn.commit().expect("commit");
    }

    #[test]
    fn test_transaction_delete() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut txn = Transaction::begin(&mut file).expect("begin txn");

        let entity_id = [1u8; 16];
        let attribute_id = [2u8; 16];

        // Insert
        txn.insert(entity_id, attribute_id, TripleValue::Boolean(true))
            .expect("insert");

        // Delete
        txn.delete(&entity_id, &attribute_id).expect("delete");

        // Verify deleted (not visible)
        let record = txn
            .get(&entity_id, &attribute_id)
            .expect("get after delete");
        assert!(record.is_none());

        txn.commit().expect("commit");
    }

    #[test]
    fn test_transaction_update_not_found() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut txn = Transaction::begin(&mut file).expect("begin txn");

        let entity_id = [1u8; 16];
        let attribute_id = [2u8; 16];

        // Try to update non-existent triple
        let result = txn.update(entity_id, attribute_id, TripleValue::Null);
        assert!(matches!(result, Err(TransactionError::NotFound)));

        txn.abort();
    }

    #[test]
    fn test_transaction_delete_not_found() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut txn = Transaction::begin(&mut file).expect("begin txn");

        let entity_id = [1u8; 16];
        let attribute_id = [2u8; 16];

        // Try to delete non-existent triple
        let result = txn.delete(&entity_id, &attribute_id);
        assert!(matches!(result, Err(TransactionError::NotFound)));

        txn.abort();
    }

    #[test]
    fn test_transaction_multiple_entities() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        {
            let mut txn = Transaction::begin(&mut file).expect("begin txn");

            // Insert triples for multiple entities
            for i in 0..10u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;

                for j in 0..5u8 {
                    let mut attr = [0u8; 16];
                    attr[0] = j;
                    txn.insert(entity, attr, TripleValue::Number(f64::from(i * 10 + j)))
                        .expect("insert");
                }
            }

            txn.commit().expect("commit");
        }

        // Verify by scanning entity 5
        {
            let mut file = DatabaseFile::open(&path).expect("open db");
            let mut txn = Transaction::begin(&mut file).expect("begin txn");

            let mut entity = [0u8; 16];
            entity[0] = 5;

            let mut scan = txn.scan_entity(&entity).expect("scan");
            let mut count = 0;
            while let Some(record) = scan.next_record().expect("next") {
                assert_eq!(record.entity_id[0], 5);
                count += 1;
            }
            assert_eq!(count, 5);

            txn.abort();
        }
    }
}
