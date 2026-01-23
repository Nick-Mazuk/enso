//! High-level database interface with WAL and crash recovery.
//!
//! This module provides a durable database with:
//! - Write-ahead logging for crash recovery
//! - Automatic checkpointing
//! - Transaction support with proper durability guarantees
//!
//! # Usage
//!
//! ```ignore
//! use storage::Database;
//!
//! // Create a new database (initializes WAL)
//! let mut db = Database::create(path)?;
//!
//! // Or open existing (runs recovery if needed)
//! let mut db = Database::open(path)?;
//!
//! // Begin a transaction
//! let mut txn = db.begin()?;
//! txn.insert(entity_id, attr_id, value)?;
//! txn.commit()?;  // Writes to WAL, then applies to index
//! ```

use std::path::Path;

use crate::storage::checkpoint::{
    CheckpointConfig, CheckpointError, CheckpointResult, CheckpointState, force_checkpoint,
    maybe_checkpoint,
};
use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::indexes::primary::{PrimaryIndex, PrimaryIndexError};
use crate::storage::recovery::{self, RecoveryError, RecoveryResult};
use crate::storage::superblock::HlcTimestamp;
use crate::storage::triple::{
    AttributeId, EntityId, TripleError, TripleRecord, TripleValue, TxnId,
};
use crate::storage::wal::{LogRecordPayload, Lsn, WalError, DEFAULT_WAL_CAPACITY};

/// A database instance with WAL and crash recovery.
///
/// This is the main entry point for working with the storage engine.
/// It manages the underlying database file, WAL, and checkpointing.
pub struct Database {
    file: DatabaseFile,
    checkpoint_state: CheckpointState,
    /// Current HLC timestamp (simplified - in production would be a proper clock)
    current_hlc: HlcTimestamp,
}

impl Database {
    /// Create a new database at the given path.
    ///
    /// The path must not already exist. Initializes WAL with default capacity.
    pub fn create(path: &Path) -> Result<Self, DatabaseError> {
        Self::create_with_options(path, DEFAULT_WAL_CAPACITY, CheckpointConfig::default())
    }

    /// Create a new database with custom options.
    pub fn create_with_options(
        path: &Path,
        wal_capacity: u64,
        checkpoint_config: CheckpointConfig,
    ) -> Result<Self, DatabaseError> {
        let mut file = DatabaseFile::create(path)?;

        // Initialize WAL
        file.init_wal(wal_capacity)?;

        let checkpoint_state = CheckpointState::from_database(&file, checkpoint_config);
        let current_hlc = HlcTimestamp::new(1, 0);

        Ok(Self {
            file,
            checkpoint_state,
            current_hlc,
        })
    }

    /// Open an existing database at the given path.
    ///
    /// Runs crash recovery if needed to restore consistent state.
    pub fn open(path: &Path) -> Result<(Self, Option<RecoveryResult>), DatabaseError> {
        Self::open_with_options(path, CheckpointConfig::default())
    }

    /// Open an existing database with custom options.
    pub fn open_with_options(
        path: &Path,
        checkpoint_config: CheckpointConfig,
    ) -> Result<(Self, Option<RecoveryResult>), DatabaseError> {
        let mut file = DatabaseFile::open(path)?;

        // Run recovery if needed
        let recovery_result = if file.has_wal() && recovery::needs_recovery(&mut file)? {
            Some(recovery::recover(&mut file)?)
        } else {
            None
        };

        let checkpoint_state = CheckpointState::from_database(&file, checkpoint_config);

        // Initialize HLC from last checkpoint or default
        let current_hlc = if file.superblock().last_checkpoint_hlc.physical_time > 0 {
            HlcTimestamp::new(
                file.superblock().last_checkpoint_hlc.physical_time + 1,
                0,
            )
        } else {
            HlcTimestamp::new(1, 0)
        };

        Ok((
            Self {
                file,
                checkpoint_state,
                current_hlc,
            },
            recovery_result,
        ))
    }

    /// Open an existing database or create a new one if it doesn't exist.
    pub fn open_or_create(path: &Path) -> Result<(Self, Option<RecoveryResult>), DatabaseError> {
        if path.exists() {
            Self::open(path)
        } else {
            let db = Self::create(path)?;
            Ok((db, None))
        }
    }

    /// Begin a new transaction.
    ///
    /// The transaction buffers all operations and writes to WAL on commit.
    #[allow(clippy::missing_const_for_fn)] // Cannot be const due to &mut self
    pub fn begin(&mut self) -> Result<WalTransaction<'_>, DatabaseError> {
        // Get next transaction ID
        let txn_id = self.file.superblock().next_txn_id;

        // Advance HLC
        self.current_hlc = HlcTimestamp::new(
            self.current_hlc.physical_time + 1,
            self.current_hlc.logical_counter,
        );

        Ok(WalTransaction::new(
            &mut self.file,
            &mut self.checkpoint_state,
            &mut self.current_hlc,
            txn_id,
        ))
    }

    /// Get the current checkpoint state.
    #[must_use]
    pub const fn checkpoint_state(&self) -> &CheckpointState {
        &self.checkpoint_state
    }

    /// Force a checkpoint.
    pub fn checkpoint(&mut self) -> Result<CheckpointResult, DatabaseError> {
        Ok(force_checkpoint(
            &mut self.file,
            &mut self.checkpoint_state,
            self.current_hlc,
        )?)
    }

    /// Close the database cleanly.
    ///
    /// Performs a final checkpoint to minimize recovery time on next open.
    pub fn close(mut self) -> Result<(), DatabaseError> {
        if self.file.has_wal() {
            force_checkpoint(&mut self.file, &mut self.checkpoint_state, self.current_hlc)?;
        }
        self.file.sync()?;
        Ok(())
    }

    /// Get the next LSN that will be assigned.
    pub fn next_lsn(&mut self) -> Result<Lsn, DatabaseError> {
        if !self.file.has_wal() {
            return Ok(0);
        }
        let wal = self.file.wal()?;
        Ok(wal.next_lsn())
    }
}

/// A buffered operation for WAL transaction.
#[derive(Debug)]
enum BufferedOp {
    Insert {
        entity_id: EntityId,
        attribute_id: AttributeId,
        value: TripleValue,
    },
    Update {
        entity_id: EntityId,
        attribute_id: AttributeId,
        value: TripleValue,
    },
    Delete {
        entity_id: EntityId,
        attribute_id: AttributeId,
    },
}

/// A WAL-backed transaction.
///
/// Operations are buffered and written to WAL on commit, then applied to the index.
/// This ensures crash recovery can replay committed transactions.
pub struct WalTransaction<'a> {
    file: &'a mut DatabaseFile,
    checkpoint_state: &'a mut CheckpointState,
    current_hlc: &'a mut HlcTimestamp,
    txn_id: TxnId,
    hlc: HlcTimestamp,
    /// Buffered operations to be written on commit
    operations: Vec<BufferedOp>,
    /// Whether this transaction has been finalized
    finalized: bool,
}

impl<'a> WalTransaction<'a> {
    const fn new(
        file: &'a mut DatabaseFile,
        checkpoint_state: &'a mut CheckpointState,
        current_hlc: &'a mut HlcTimestamp,
        txn_id: TxnId,
    ) -> Self {
        let hlc = *current_hlc;
        Self {
            file,
            checkpoint_state,
            current_hlc,
            txn_id,
            hlc,
            operations: Vec::new(),
            finalized: false,
        }
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
    ///
    /// Note: This reads from the committed state, not buffered operations.
    pub fn get(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<Option<TripleRecord>, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(self.file, root_page)?;

        match index.get(entity_id, attribute_id)? {
            Some(record) if !record.is_deleted() => Ok(Some(record)),
            _ => Ok(None),
        }
    }

    /// Scan all triples for an entity.
    ///
    /// Returns all triples for the given entity as a vector.
    pub fn scan_entity(&mut self, entity_id: &EntityId) -> Result<Vec<TripleRecord>, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(self.file, root_page)?;
        let mut scan = index.scan_entity(entity_id)?;

        let mut results = Vec::new();
        while let Some(record) = scan.next_record()? {
            if !record.is_deleted() {
                results.push(record);
            }
        }

        Ok(results)
    }

    /// Count all triples in the index.
    pub fn count(&mut self) -> Result<usize, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(self.file, root_page)?;
        Ok(index.count()?)
    }

    /// Insert a triple.
    ///
    /// The operation is buffered until commit.
    pub fn insert(&mut self, entity_id: EntityId, attribute_id: AttributeId, value: TripleValue) {
        self.operations.push(BufferedOp::Insert {
            entity_id,
            attribute_id,
            value,
        });
    }

    /// Update a triple.
    ///
    /// The operation is buffered until commit.
    pub fn update(
        &mut self,
        entity_id: EntityId,
        attribute_id: AttributeId,
        value: TripleValue,
    ) -> Result<(), DatabaseError> {
        // Check that the triple exists
        if self.get(&entity_id, &attribute_id)?.is_none() {
            return Err(DatabaseError::NotFound);
        }

        self.operations.push(BufferedOp::Update {
            entity_id,
            attribute_id,
            value,
        });
        Ok(())
    }

    /// Delete a triple.
    ///
    /// The operation is buffered until commit.
    pub fn delete(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<(), DatabaseError> {
        // Check that the triple exists
        if self.get(entity_id, attribute_id)?.is_none() {
            return Err(DatabaseError::NotFound);
        }

        self.operations.push(BufferedOp::Delete {
            entity_id: *entity_id,
            attribute_id: *attribute_id,
        });
        Ok(())
    }

    /// Commit the transaction.
    ///
    /// This:
    /// 1. Writes BEGIN record to WAL
    /// 2. Writes all buffered operations to WAL
    /// 3. Writes COMMIT record to WAL
    /// 4. Syncs WAL
    /// 5. Applies operations to the index
    /// 6. Updates superblock
    /// 7. Optionally triggers checkpoint
    pub fn commit(mut self) -> Result<(), DatabaseError> {
        self.finalized = true;

        if self.operations.is_empty() {
            // Nothing to commit
            return Ok(());
        }

        let txn_id = self.txn_id;
        let hlc = self.hlc;

        // Step 1-4: Write to WAL
        let wal_bytes_written = if self.file.has_wal() {
            self.write_to_wal(txn_id, hlc)?
        } else {
            0
        };

        // Step 5: Apply operations to index
        self.apply_to_index(txn_id, hlc)?;

        // Step 6: Update superblock
        self.file.superblock_mut().next_txn_id = txn_id + 1;
        self.file.write_superblock()?;
        self.file.sync()?;

        // Step 7: Update checkpoint state and maybe checkpoint
        self.checkpoint_state.record_commit();
        self.checkpoint_state.record_wal_write(wal_bytes_written);

        // Advance HLC
        *self.current_hlc = HlcTimestamp::new(hlc.physical_time + 1, 0);

        // Check if we should checkpoint
        if self.file.has_wal() {
            maybe_checkpoint(self.file, self.checkpoint_state, *self.current_hlc)?;
        }

        Ok(())
    }

    /// Write all operations to WAL.
    fn write_to_wal(&mut self, txn_id: TxnId, hlc: HlcTimestamp) -> Result<u64, DatabaseError> {
        let mut total_bytes = 0u64;

        let mut wal = self.file.wal()?;

        // BEGIN
        wal.append(txn_id, hlc, LogRecordPayload::Begin)?;

        // Write each operation
        for op in &self.operations {
            match op {
                BufferedOp::Insert {
                    entity_id,
                    attribute_id,
                    value,
                } => {
                    let record =
                        TripleRecord::new(*entity_id, *attribute_id, txn_id, hlc, value.clone_value());
                    let payload = LogRecordPayload::insert(&record);
                    total_bytes += payload.serialized_size() as u64;
                    wal.append(txn_id, hlc, payload)?;
                }
                BufferedOp::Update {
                    entity_id,
                    attribute_id,
                    value,
                } => {
                    let record =
                        TripleRecord::new(*entity_id, *attribute_id, txn_id, hlc, value.clone_value());
                    let payload = LogRecordPayload::update(&record);
                    total_bytes += payload.serialized_size() as u64;
                    wal.append(txn_id, hlc, payload)?;
                }
                BufferedOp::Delete {
                    entity_id,
                    attribute_id,
                } => {
                    let payload = LogRecordPayload::delete(*entity_id, *attribute_id);
                    total_bytes += payload.serialized_size() as u64;
                    wal.append(txn_id, hlc, payload)?;
                }
            }
        }

        // COMMIT
        wal.append(txn_id, hlc, LogRecordPayload::Commit)?;

        // Sync WAL
        wal.sync()?;

        // Extract values before dropping wal (which borrows self.file)
        let head = wal.head();
        let last_lsn = wal.last_lsn();
        #[allow(clippy::drop_non_drop)] // Needed to release the mutable borrow
        drop(wal);

        // Update WAL head in file
        self.file.update_wal_head(head, last_lsn);

        Ok(total_bytes)
    }

    /// Apply buffered operations to the index.
    fn apply_to_index(&mut self, txn_id: TxnId, hlc: HlcTimestamp) -> Result<(), DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(self.file, root_page)?;

        for op in &self.operations {
            match op {
                BufferedOp::Insert {
                    entity_id,
                    attribute_id,
                    value,
                }
                | BufferedOp::Update {
                    entity_id,
                    attribute_id,
                    value,
                } => {
                    let record =
                        TripleRecord::new(*entity_id, *attribute_id, txn_id, hlc, value.clone_value());
                    index.insert(&record)?;
                }
                BufferedOp::Delete {
                    entity_id,
                    attribute_id,
                } => {
                    index.mark_deleted(entity_id, attribute_id, txn_id)?;
                }
            }
        }

        // Update root page in superblock
        let new_root = index.root_page();
        let file = index.file_mut();
        file.superblock_mut().primary_index_root = new_root;

        Ok(())
    }

    /// Abort the transaction.
    ///
    /// Discards all buffered operations without writing to WAL.
    pub fn abort(mut self) {
        self.finalized = true;
        self.operations.clear();
    }
}

impl Drop for WalTransaction<'_> {
    fn drop(&mut self) {
        if !self.finalized {
            // Transaction was dropped without commit or abort
            // In debug builds, this could panic or log a warning
        }
    }
}

/// Errors that can occur during database operations.
#[derive(Debug)]
pub enum DatabaseError {
    /// File I/O error.
    File(FileError),
    /// WAL error.
    Wal(WalError),
    /// Index error.
    Index(PrimaryIndexError),
    /// Triple error.
    Triple(TripleError),
    /// Recovery error.
    Recovery(RecoveryError),
    /// Checkpoint error.
    Checkpoint(CheckpointError),
    /// Triple not found for update/delete.
    NotFound,
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(e) => write!(f, "file error: {e}"),
            Self::Wal(e) => write!(f, "WAL error: {e}"),
            Self::Index(e) => write!(f, "index error: {e}"),
            Self::Triple(e) => write!(f, "triple error: {e}"),
            Self::Recovery(e) => write!(f, "recovery error: {e}"),
            Self::Checkpoint(e) => write!(f, "checkpoint error: {e}"),
            Self::NotFound => write!(f, "triple not found"),
        }
    }
}

impl std::error::Error for DatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(e) => Some(e),
            Self::Wal(e) => Some(e),
            Self::Index(e) => Some(e),
            Self::Triple(e) => Some(e),
            Self::Recovery(e) => Some(e),
            Self::Checkpoint(e) => Some(e),
            Self::NotFound => None,
        }
    }
}

impl From<FileError> for DatabaseError {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

impl From<WalError> for DatabaseError {
    fn from(e: WalError) -> Self {
        Self::Wal(e)
    }
}

impl From<PrimaryIndexError> for DatabaseError {
    fn from(e: PrimaryIndexError) -> Self {
        Self::Index(e)
    }
}

impl From<TripleError> for DatabaseError {
    fn from(e: TripleError) -> Self {
        Self::Triple(e)
    }
}

impl From<RecoveryError> for DatabaseError {
    fn from(e: RecoveryError) -> Self {
        Self::Recovery(e)
    }
}

impl From<CheckpointError> for DatabaseError {
    fn from(e: CheckpointError) -> Self {
        Self::Checkpoint(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_database_create_and_commit() {
        let (_dir, path) = create_test_db();

        // Create database
        {
            let mut db = Database::create(&path).expect("create db");
            let mut txn = db.begin().expect("begin txn");

            let entity_id = [1u8; 16];
            let attribute_id = [2u8; 16];
            txn.insert(entity_id, attribute_id, TripleValue::String("hello".to_string()));
            txn.commit().expect("commit");
        }

        // Reopen and verify (with recovery)
        {
            let (mut db, recovery) = Database::open(&path).expect("open db");

            // Recovery might have run
            if let Some(result) = recovery {
                assert!(result.records_scanned >= 0);
            }

            let mut txn = db.begin().expect("begin txn");
            let record = txn.get(&[1u8; 16], &[2u8; 16]).expect("get");
            assert!(record.is_some());
            assert_eq!(
                record.unwrap().value,
                TripleValue::String("hello".to_string())
            );
            txn.abort();
        }
    }

    #[test]
    fn test_database_open_or_create() {
        let (_dir, path) = create_test_db();

        // First call creates
        {
            let (db, recovery) = Database::open_or_create(&path).expect("open_or_create");
            assert!(recovery.is_none()); // New database, no recovery
            drop(db);
        }

        // Second call opens
        {
            let (mut db, _) = Database::open_or_create(&path).expect("open_or_create again");
            let txn = db.begin().expect("begin");
            txn.abort();
        }
    }

    #[test]
    fn test_database_abort_no_persist() {
        let (_dir, path) = create_test_db();

        // Create and abort transaction
        {
            let mut db = Database::create(&path).expect("create db");
            let mut txn = db.begin().expect("begin txn");

            txn.insert([1u8; 16], [2u8; 16], TripleValue::String("aborted".to_string()));
            txn.abort(); // Don't commit
        }

        // Reopen and verify data is NOT there
        {
            let (mut db, _) = Database::open(&path).expect("open db");
            let mut txn = db.begin().expect("begin txn");

            let record = txn.get(&[1u8; 16], &[2u8; 16]).expect("get");
            assert!(record.is_none());
            txn.abort();
        }
    }

    #[test]
    fn test_database_update_and_delete() {
        let (_dir, path) = create_test_db();

        let mut db = Database::create(&path).expect("create db");

        // Insert
        {
            let mut txn = db.begin().expect("begin");
            txn.insert([1u8; 16], [1u8; 16], TripleValue::Number(1.0));
            txn.commit().expect("commit");
        }

        // Update
        {
            let mut txn = db.begin().expect("begin");
            txn.update([1u8; 16], [1u8; 16], TripleValue::Number(2.0))
                .expect("update");
            txn.commit().expect("commit");
        }

        // Verify update
        {
            let mut txn = db.begin().expect("begin");
            let record = txn.get(&[1u8; 16], &[1u8; 16]).expect("get");
            assert_eq!(record.unwrap().value, TripleValue::Number(2.0));
            txn.abort();
        }

        // Delete
        {
            let mut txn = db.begin().expect("begin");
            txn.delete(&[1u8; 16], &[1u8; 16]).expect("delete");
            txn.commit().expect("commit");
        }

        // Verify delete
        {
            let mut txn = db.begin().expect("begin");
            let record = txn.get(&[1u8; 16], &[1u8; 16]).expect("get");
            assert!(record.is_none());
            txn.abort();
        }
    }

    #[test]
    fn test_database_not_found_errors() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        let mut txn = db.begin().expect("begin");

        // Update non-existent
        let result = txn.update([1u8; 16], [1u8; 16], TripleValue::Null);
        assert!(matches!(result, Err(DatabaseError::NotFound)));

        // Delete non-existent
        let result = txn.delete(&[1u8; 16], &[1u8; 16]);
        assert!(matches!(result, Err(DatabaseError::NotFound)));

        txn.abort();
    }

    #[test]
    fn test_database_multiple_transactions() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Multiple sequential transactions
        for i in 0..10u8 {
            let mut txn = db.begin().expect("begin");
            let mut entity = [0u8; 16];
            entity[0] = i;
            txn.insert(entity, [1u8; 16], TripleValue::Number(f64::from(i)));
            txn.commit().expect("commit");
        }

        // Verify all data
        let mut txn = db.begin().expect("begin");
        for i in 0..10u8 {
            let mut entity = [0u8; 16];
            entity[0] = i;
            let record = txn.get(&entity, &[1u8; 16]).expect("get");
            assert!(record.is_some());
            assert_eq!(record.unwrap().value, TripleValue::Number(f64::from(i)));
        }
        txn.abort();
    }

    #[test]
    fn test_database_checkpoint() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert some data
        {
            let mut txn = db.begin().expect("begin");
            txn.insert([1u8; 16], [1u8; 16], TripleValue::Boolean(true));
            txn.commit().expect("commit");
        }

        // Force checkpoint
        let result = db.checkpoint().expect("checkpoint");
        assert!(result.checkpoint_lsn > 0);

        // Close cleanly
        db.close().expect("close");
    }

    #[test]
    fn test_database_recovery_committed() {
        let (_dir, path) = create_test_db();

        // Create database and insert without clean close
        {
            let mut db = Database::create(&path).expect("create db");
            let mut txn = db.begin().expect("begin");
            txn.insert([1u8; 16], [1u8; 16], TripleValue::String("recovered".to_string()));
            txn.commit().expect("commit");
            // Don't call close() - simulates crash after commit
        }

        // Reopen - recovery should find the committed data
        {
            let (mut db, recovery) = Database::open(&path).expect("open db");

            // Might have run recovery
            if let Some(result) = recovery {
                // The transaction was committed, so it should be replayed if needed
                assert!(result.records_scanned >= 0);
            }

            // Verify data is there
            let mut txn = db.begin().expect("begin");
            let record = txn.get(&[1u8; 16], &[1u8; 16]).expect("get");
            assert!(record.is_some());
            assert_eq!(
                record.unwrap().value,
                TripleValue::String("recovered".to_string())
            );
            txn.abort();

            db.close().expect("close");
        }
    }

    #[test]
    fn test_database_empty_commit() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Empty transaction should commit successfully
        let txn = db.begin().expect("begin");
        txn.commit().expect("commit empty");

        db.close().expect("close");
    }
}
