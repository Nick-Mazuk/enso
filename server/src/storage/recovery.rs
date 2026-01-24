//! Crash recovery for the storage engine.
//!
//! Recovery replays WAL records after the last checkpoint to restore the
//! database to a consistent state after a crash.
//!
//! # Recovery Process
//!
//! 1. Read superblock to get last checkpoint LSN
//! 2. Scan WAL from checkpoint LSN to head
//! 3. For each committed transaction:
//!    - Replay INSERT, UPDATE, DELETE operations
//!    - Skip uncommitted transactions (no COMMIT record)
//! 4. Update superblock with recovered state
//!
//! # Typical Recovery Time
//!
//! With aggressive checkpointing, recovery typically replays <1000 records,
//! completing in <10ms.

use std::collections::{HashMap, HashSet};

use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::indexes::primary::{PrimaryIndex, PrimaryIndexError};
use crate::storage::superblock::HlcTimestamp;
use crate::storage::triple::{AttributeId, EntityId, TripleError, TripleRecord, TxnId};
use crate::storage::wal::{LogRecordPayload, Lsn, WalError};

/// Result of a recovery operation.
#[derive(Debug)]
pub struct RecoveryResult {
    /// Number of WAL records scanned.
    pub records_scanned: usize,

    /// Number of committed transactions replayed.
    pub transactions_replayed: usize,

    /// Number of uncommitted transactions discarded.
    pub transactions_discarded: usize,

    /// Number of operations applied (inserts, updates, deletes).
    pub operations_applied: usize,

    /// LSN of the last checkpoint before recovery.
    pub checkpoint_lsn: Lsn,

    /// Highest LSN seen during recovery.
    pub recovered_lsn: Lsn,
}

/// Pending operations for a transaction being replayed.
#[derive(Debug, Default)]
struct PendingTransaction {
    /// Insert/Update operations: (`entity_id`, `attribute_id`) -> serialized record bytes
    inserts: HashMap<(EntityId, AttributeId), Vec<u8>>,
    /// Delete operations: set of (`entity_id`, `attribute_id`)
    deletes: HashSet<(EntityId, AttributeId)>,
    /// The commit HLC timestamp (set when COMMIT record is seen)
    commit_hlc: Option<HlcTimestamp>,
}

impl PendingTransaction {
    fn new() -> Self {
        Self::default()
    }

    const fn is_committed(&self) -> bool {
        self.commit_hlc.is_some()
    }
}

/// Perform crash recovery on the database.
///
/// This function:
/// 1. Reads the WAL from the last checkpoint
/// 2. Groups operations by transaction
/// 3. Replays only committed transactions
/// 4. Updates the database state
///
/// # Arguments
/// * `file` - The database file to recover
///
/// # Returns
/// A `RecoveryResult` with statistics about the recovery.
#[allow(clippy::too_many_lines)]
pub fn recover(file: &mut DatabaseFile) -> Result<RecoveryResult, RecoveryError> {
    // Check if WAL is initialized
    if !file.has_wal() {
        // No WAL, nothing to recover
        return Ok(RecoveryResult {
            records_scanned: 0,
            transactions_replayed: 0,
            transactions_discarded: 0,
            operations_applied: 0,
            checkpoint_lsn: 0,
            recovered_lsn: 0,
        });
    }

    let checkpoint_lsn = file.superblock().last_checkpoint_lsn;

    // Read all WAL records
    let records = {
        let mut wal = file.wal()?;
        if checkpoint_lsn > 0 {
            wal.read_from_lsn(checkpoint_lsn)?
        } else {
            wal.read_all()?
        }
    };

    if records.is_empty() {
        return Ok(RecoveryResult {
            records_scanned: 0,
            transactions_replayed: 0,
            transactions_discarded: 0,
            operations_applied: 0,
            checkpoint_lsn,
            recovered_lsn: checkpoint_lsn,
        });
    }

    // Group records by transaction
    let mut pending_txns: HashMap<TxnId, PendingTransaction> = HashMap::new();
    let mut highest_lsn: Lsn = checkpoint_lsn;
    let records_scanned = records.len();

    for record in records {
        highest_lsn = highest_lsn.max(record.lsn);

        match record.payload {
            LogRecordPayload::Begin => {
                // Start tracking a new transaction
                pending_txns.insert(record.txn_id, PendingTransaction::new());
            }
            LogRecordPayload::Insert(bytes) => {
                // Store the insert for later replay
                if let Some(txn) = pending_txns.get_mut(&record.txn_id) {
                    // Extract entity_id and attribute_id from serialized record
                    if bytes.len() >= 32 {
                        let mut entity_id = [0u8; 16];
                        let mut attribute_id = [0u8; 16];
                        entity_id.copy_from_slice(&bytes[0..16]);
                        attribute_id.copy_from_slice(&bytes[16..32]);
                        txn.inserts.insert((entity_id, attribute_id), bytes);
                        // Remove from deletes if present (insert after delete)
                        txn.deletes.remove(&(entity_id, attribute_id));
                    }
                }
            }
            LogRecordPayload::Update(bytes) => {
                // Updates are treated the same as inserts for replay
                if let Some(txn) = pending_txns.get_mut(&record.txn_id) {
                    if bytes.len() >= 32 {
                        let mut entity_id = [0u8; 16];
                        let mut attribute_id = [0u8; 16];
                        entity_id.copy_from_slice(&bytes[0..16]);
                        attribute_id.copy_from_slice(&bytes[16..32]);
                        txn.inserts.insert((entity_id, attribute_id), bytes);
                        txn.deletes.remove(&(entity_id, attribute_id));
                    }
                }
            }
            LogRecordPayload::Delete {
                entity_id,
                attribute_id,
            } => {
                if let Some(txn) = pending_txns.get_mut(&record.txn_id) {
                    // Remove any pending insert for this key
                    txn.inserts.remove(&(entity_id, attribute_id));
                    txn.deletes.insert((entity_id, attribute_id));
                }
            }
            LogRecordPayload::Commit => {
                if let Some(txn) = pending_txns.get_mut(&record.txn_id) {
                    txn.commit_hlc = Some(record.hlc);
                }
            }
            LogRecordPayload::Checkpoint { .. } => {
                // Checkpoint records don't affect recovery replay
            }
        }
    }

    // Count committed and uncommitted transactions
    let transactions_replayed = pending_txns.values().filter(|t| t.is_committed()).count();
    let transactions_discarded = pending_txns.len() - transactions_replayed;

    // Replay committed transactions
    let mut operations_applied = 0;

    // Get primary index root
    let root_page = file.superblock().primary_index_root;

    // Create primary index for applying changes
    {
        let mut index = PrimaryIndex::new(file, root_page)?;

        // Apply operations from committed transactions
        for (txn_id, txn) in &pending_txns {
            if !txn.is_committed() {
                continue;
            }

            // Apply inserts/updates
            for ((_entity_id, _attribute_id), bytes) in &txn.inserts {
                let record = TripleRecord::from_bytes(bytes)?;
                index.insert(&record)?;
                operations_applied += 1;
            }

            // Apply deletes
            for (entity_id, attribute_id) in &txn.deletes {
                // Mark as deleted with this transaction ID
                if index.mark_deleted(entity_id, attribute_id, *txn_id).is_ok() {
                    operations_applied += 1;
                }
            }
        }

        // Update superblock with new root page
        let new_root = index.root_page();
        let file = index.file_mut();
        file.superblock_mut().primary_index_root = new_root;
    }

    // Update next_txn_id to be higher than any recovered transaction
    let max_txn_id = pending_txns.keys().copied().max().unwrap_or(0);
    if max_txn_id >= file.superblock().next_txn_id {
        file.superblock_mut().next_txn_id = max_txn_id + 1;
    }

    // Persist superblock
    file.write_superblock()?;
    file.sync()?;

    Ok(RecoveryResult {
        records_scanned,
        transactions_replayed,
        transactions_discarded,
        operations_applied,
        checkpoint_lsn,
        recovered_lsn: highest_lsn,
    })
}

/// Check if recovery is needed.
///
/// Recovery is needed if there are WAL records after the last checkpoint
/// that haven't been applied to the database.
pub fn needs_recovery(file: &mut DatabaseFile) -> Result<bool, RecoveryError> {
    if !file.has_wal() {
        return Ok(false);
    }

    let checkpoint_lsn = file.superblock().last_checkpoint_lsn;

    let mut wal = file.wal()?;

    // Check if there are any records after the checkpoint
    if checkpoint_lsn > 0 {
        let records = wal.read_from_lsn(checkpoint_lsn)?;
        // If there's more than just the checkpoint record itself, we need recovery
        Ok(records.len() > 1)
    } else {
        // No checkpoint, check if WAL is non-empty
        Ok(!wal.is_empty())
    }
}

/// Errors that can occur during recovery.
#[derive(Debug)]
pub enum RecoveryError {
    /// File I/O error.
    File(FileError),
    /// WAL error.
    Wal(WalError),
    /// Index error.
    Index(PrimaryIndexError),
    /// Triple deserialization error.
    Triple(TripleError),
    /// Corrupt WAL - found commit without begin.
    OrphanCommit(TxnId),
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(e) => write!(f, "recovery file error: {e}"),
            Self::Wal(e) => write!(f, "recovery WAL error: {e}"),
            Self::Index(e) => write!(f, "recovery index error: {e}"),
            Self::Triple(e) => write!(f, "recovery triple error: {e}"),
            Self::OrphanCommit(txn_id) => {
                write!(f, "recovery found commit without begin for txn {txn_id}")
            }
        }
    }
}

impl std::error::Error for RecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(e) => Some(e),
            Self::Wal(e) => Some(e),
            Self::Index(e) => Some(e),
            Self::Triple(e) => Some(e),
            Self::OrphanCommit(_) => None,
        }
    }
}

impl From<FileError> for RecoveryError {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

impl From<WalError> for RecoveryError {
    fn from(e: WalError) -> Self {
        Self::Wal(e)
    }
}

impl From<PrimaryIndexError> for RecoveryError {
    fn from(e: PrimaryIndexError) -> Self {
        Self::Index(e)
    }
}

impl From<TripleError> for RecoveryError {
    fn from(e: TripleError) -> Self {
        Self::Triple(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::triple::TripleValue;
    use crate::storage::wal::{DEFAULT_WAL_CAPACITY, LogRecordPayload};
    use tempfile::tempdir;

    fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_recover_empty_wal() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let result = recover(&mut file).expect("recover");

        assert_eq!(result.records_scanned, 0);
        assert_eq!(result.transactions_replayed, 0);
        assert_eq!(result.operations_applied, 0);
    }

    #[test]
    fn test_recover_no_wal() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        // Don't init WAL

        let result = recover(&mut file).expect("recover");

        assert_eq!(result.records_scanned, 0);
        assert_eq!(result.transactions_replayed, 0);
    }

    #[test]
    fn test_recover_committed_transaction() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);
        let txn_id = 1;

        // Write a complete transaction to WAL
        {
            let mut wal = file.wal().expect("get wal");

            // BEGIN
            wal.append(txn_id, hlc, LogRecordPayload::Begin)
                .expect("append begin");

            // INSERT
            let triple = TripleRecord::new(
                [1u8; 16],
                [2u8; 16],
                txn_id,
                hlc,
                TripleValue::String("test".to_string()),
            );
            wal.append(txn_id, hlc, LogRecordPayload::insert(&triple))
                .expect("append insert");

            // COMMIT
            wal.append(txn_id, hlc, LogRecordPayload::Commit)
                .expect("append commit");

            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Run recovery
        let result = recover(&mut file).expect("recover");

        assert_eq!(result.records_scanned, 3);
        assert_eq!(result.transactions_replayed, 1);
        assert_eq!(result.transactions_discarded, 0);
        assert_eq!(result.operations_applied, 1);

        // Verify the data was applied
        let root_page = file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(&mut file, root_page).expect("open index");
        let record = index.get(&[1u8; 16], &[2u8; 16]).expect("get");
        assert!(record.is_some());
        assert_eq!(
            record.unwrap().value,
            TripleValue::String("test".to_string())
        );
    }

    #[test]
    fn test_recover_uncommitted_transaction() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);
        let txn_id = 1;

        // Write an incomplete transaction (no COMMIT)
        {
            let mut wal = file.wal().expect("get wal");

            wal.append(txn_id, hlc, LogRecordPayload::Begin)
                .expect("append begin");

            let triple = TripleRecord::new(
                [1u8; 16],
                [2u8; 16],
                txn_id,
                hlc,
                TripleValue::String("uncommitted".to_string()),
            );
            wal.append(txn_id, hlc, LogRecordPayload::insert(&triple))
                .expect("append insert");

            // NO COMMIT!
            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Run recovery
        let result = recover(&mut file).expect("recover");

        assert_eq!(result.records_scanned, 2);
        assert_eq!(result.transactions_replayed, 0);
        assert_eq!(result.transactions_discarded, 1);
        assert_eq!(result.operations_applied, 0);

        // Verify the data was NOT applied
        let root_page = file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(&mut file, root_page).expect("open index");
        let record = index.get(&[1u8; 16], &[2u8; 16]).expect("get");
        assert!(record.is_none());
    }

    #[test]
    fn test_recover_multiple_transactions() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);

        // Write multiple transactions
        {
            let mut wal = file.wal().expect("get wal");

            // Transaction 1 - committed
            wal.append(1, hlc, LogRecordPayload::Begin)
                .expect("begin 1");
            let triple1 = TripleRecord::new([1u8; 16], [1u8; 16], 1, hlc, TripleValue::Number(1.0));
            wal.append(1, hlc, LogRecordPayload::insert(&triple1))
                .expect("insert 1");
            wal.append(1, hlc, LogRecordPayload::Commit)
                .expect("commit 1");

            // Transaction 2 - uncommitted
            wal.append(2, hlc, LogRecordPayload::Begin)
                .expect("begin 2");
            let triple2 = TripleRecord::new([2u8; 16], [2u8; 16], 2, hlc, TripleValue::Number(2.0));
            wal.append(2, hlc, LogRecordPayload::insert(&triple2))
                .expect("insert 2");
            // NO COMMIT for txn 2

            // Transaction 3 - committed
            wal.append(3, hlc, LogRecordPayload::Begin)
                .expect("begin 3");
            let triple3 = TripleRecord::new([3u8; 16], [3u8; 16], 3, hlc, TripleValue::Number(3.0));
            wal.append(3, hlc, LogRecordPayload::insert(&triple3))
                .expect("insert 3");
            wal.append(3, hlc, LogRecordPayload::Commit)
                .expect("commit 3");

            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Run recovery
        let result = recover(&mut file).expect("recover");

        assert_eq!(result.transactions_replayed, 2); // txn 1 and 3
        assert_eq!(result.transactions_discarded, 1); // txn 2
        assert_eq!(result.operations_applied, 2);

        // Verify txn 1 and 3 data was applied, txn 2 was not
        let root_page = file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(&mut file, root_page).expect("open index");

        assert!(index.get(&[1u8; 16], &[1u8; 16]).expect("get 1").is_some());
        assert!(index.get(&[2u8; 16], &[2u8; 16]).expect("get 2").is_none());
        assert!(index.get(&[3u8; 16], &[3u8; 16]).expect("get 3").is_some());
    }

    #[test]
    fn test_recover_delete_operation() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);

        // First, insert data directly to index
        {
            let root_page = file.superblock().primary_index_root;
            let mut index = PrimaryIndex::new(&mut file, root_page).expect("open index");
            let triple = TripleRecord::new(
                [1u8; 16],
                [1u8; 16],
                0,
                hlc,
                TripleValue::String("to delete".to_string()),
            );
            index.insert(&triple).expect("insert");
            let new_root = index.root_page();
            let file = index.file_mut();
            file.superblock_mut().primary_index_root = new_root;
        }
        file.write_superblock().expect("write superblock");
        file.sync().expect("sync");

        // Now write a delete to WAL
        {
            let mut wal = file.wal().expect("get wal");

            wal.append(1, hlc, LogRecordPayload::Begin).expect("begin");
            wal.append(1, hlc, LogRecordPayload::delete([1u8; 16], [1u8; 16]))
                .expect("delete");
            wal.append(1, hlc, LogRecordPayload::Commit)
                .expect("commit");

            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Run recovery
        let result = recover(&mut file).expect("recover");

        assert_eq!(result.transactions_replayed, 1);
        assert_eq!(result.operations_applied, 1);

        // Verify the record was marked as deleted
        let root_page = file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(&mut file, root_page).expect("open index");
        let record = index.get(&[1u8; 16], &[1u8; 16]).expect("get");

        // Record exists but is deleted
        assert!(record.is_some());
        assert!(record.unwrap().is_deleted());
    }

    #[test]
    fn test_needs_recovery_empty() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        assert!(!needs_recovery(&mut file).expect("needs_recovery"));
    }

    #[test]
    fn test_needs_recovery_with_uncommitted() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        // Write some WAL records
        {
            let mut wal = file.wal().expect("get wal");
            wal.append(1, HlcTimestamp::new(1000, 0), LogRecordPayload::Begin)
                .expect("append");
            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        assert!(needs_recovery(&mut file).expect("needs_recovery"));
    }

    #[test]
    fn test_recovery_updates_next_txn_id() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);

        // Write transaction with high ID
        {
            let mut wal = file.wal().expect("get wal");
            wal.append(100, hlc, LogRecordPayload::Begin)
                .expect("begin");
            let triple = TripleRecord::new([1u8; 16], [1u8; 16], 100, hlc, TripleValue::Null);
            wal.append(100, hlc, LogRecordPayload::insert(&triple))
                .expect("insert");
            wal.append(100, hlc, LogRecordPayload::Commit)
                .expect("commit");
            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Verify next_txn_id is low before recovery
        assert!(file.superblock().next_txn_id <= 2);

        // Run recovery
        recover(&mut file).expect("recover");

        // Verify next_txn_id was updated
        assert!(file.superblock().next_txn_id > 100);
    }

    #[test]
    fn test_recover_interleaved_transactions() {
        // Test recovery with interleaved transactions (concurrent writes)
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);

        // Write interleaved transactions: BEGIN1, BEGIN2, INSERT1, INSERT2, COMMIT1
        // Only transaction 1 should be committed
        {
            let mut wal = file.wal().expect("get wal");

            // Transaction 1 begins
            wal.append(1, hlc, LogRecordPayload::Begin)
                .expect("begin 1");

            // Transaction 2 begins (interleaved)
            wal.append(2, hlc, LogRecordPayload::Begin)
                .expect("begin 2");

            // Transaction 1 inserts
            let triple1 = TripleRecord::new([1u8; 16], [1u8; 16], 1, hlc, TripleValue::Number(1.0));
            wal.append(1, hlc, LogRecordPayload::insert(&triple1))
                .expect("insert 1");

            // Transaction 2 inserts (uncommitted)
            let triple2 = TripleRecord::new([2u8; 16], [2u8; 16], 2, hlc, TripleValue::Number(2.0));
            wal.append(2, hlc, LogRecordPayload::insert(&triple2))
                .expect("insert 2");

            // Transaction 1 commits
            wal.append(1, hlc, LogRecordPayload::Commit)
                .expect("commit 1");

            // Transaction 2 never commits (simulates crash)

            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Run recovery
        let result = recover(&mut file).expect("recover");

        assert_eq!(result.transactions_replayed, 1); // Only txn 1
        assert_eq!(result.transactions_discarded, 1); // Txn 2
        assert_eq!(result.operations_applied, 1);

        // Verify only transaction 1's data was applied
        let root_page = file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(&mut file, root_page).expect("open index");

        assert!(index.get(&[1u8; 16], &[1u8; 16]).expect("get 1").is_some());
        assert!(index.get(&[2u8; 16], &[2u8; 16]).expect("get 2").is_none());
    }

    #[test]
    fn test_recover_insert_then_delete_same_key() {
        // Test recovery when same key is inserted and deleted in same transaction
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);

        // Write: BEGIN, INSERT, DELETE (same key), COMMIT
        {
            let mut wal = file.wal().expect("get wal");

            wal.append(1, hlc, LogRecordPayload::Begin).expect("begin");

            // Insert a triple
            let triple = TripleRecord::new([1u8; 16], [1u8; 16], 1, hlc, TripleValue::Number(42.0));
            wal.append(1, hlc, LogRecordPayload::insert(&triple))
                .expect("insert");

            // Delete the same triple in the same transaction
            wal.append(1, hlc, LogRecordPayload::delete([1u8; 16], [1u8; 16]))
                .expect("delete");

            wal.append(1, hlc, LogRecordPayload::Commit)
                .expect("commit");

            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Run recovery
        let result = recover(&mut file).expect("recover");

        assert_eq!(result.transactions_replayed, 1);
        // The insert was cancelled by the delete, only delete should be applied
        assert_eq!(result.operations_applied, 1);

        // Verify the record doesn't exist (was deleted)
        let root_page = file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(&mut file, root_page).expect("open index");

        // The record should not exist because it was deleted after insert
        let record = index.get(&[1u8; 16], &[1u8; 16]).expect("get");
        assert!(record.is_none() || record.unwrap().is_deleted());
    }

    #[test]
    fn test_recover_multiple_inserts_same_key() {
        // Test recovery when same key is inserted multiple times (update scenario)
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);

        // Write: BEGIN, INSERT(value=1), INSERT(value=2 same key), COMMIT
        // The last insert should win
        {
            let mut wal = file.wal().expect("get wal");

            wal.append(1, hlc, LogRecordPayload::Begin).expect("begin");

            // First insert
            let triple1 = TripleRecord::new([1u8; 16], [1u8; 16], 1, hlc, TripleValue::Number(1.0));
            wal.append(1, hlc, LogRecordPayload::insert(&triple1))
                .expect("insert 1");

            // Second insert (same key, different value)
            let triple2 = TripleRecord::new([1u8; 16], [1u8; 16], 1, hlc, TripleValue::Number(2.0));
            wal.append(1, hlc, LogRecordPayload::insert(&triple2))
                .expect("insert 2");

            wal.append(1, hlc, LogRecordPayload::Commit)
                .expect("commit");

            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Run recovery
        let result = recover(&mut file).expect("recover");

        assert_eq!(result.transactions_replayed, 1);
        // Only one insert should be applied (the last one wins in the hashmap)
        assert_eq!(result.operations_applied, 1);

        // Verify the final value
        let root_page = file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(&mut file, root_page).expect("open index");

        let record = index.get(&[1u8; 16], &[1u8; 16]).expect("get").unwrap();
        assert_eq!(record.value, TripleValue::Number(2.0));
    }

    #[test]
    fn test_recover_checkpoint_record() {
        // Test that checkpoint records don't affect recovery
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);

        // Write: BEGIN, INSERT, CHECKPOINT, COMMIT
        {
            let mut wal = file.wal().expect("get wal");

            wal.append(1, hlc, LogRecordPayload::Begin).expect("begin");

            let triple = TripleRecord::new([1u8; 16], [1u8; 16], 1, hlc, TripleValue::Number(42.0));
            wal.append(1, hlc, LogRecordPayload::insert(&triple))
                .expect("insert");

            // Checkpoint in the middle of transaction
            wal.append(0, hlc, LogRecordPayload::checkpoint(1, 1))
                .expect("checkpoint");

            wal.append(1, hlc, LogRecordPayload::Commit)
                .expect("commit");

            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Run recovery
        let result = recover(&mut file).expect("recover");

        assert_eq!(result.records_scanned, 4); // BEGIN, INSERT, CHECKPOINT, COMMIT
        assert_eq!(result.transactions_replayed, 1);
        assert_eq!(result.operations_applied, 1);

        // Verify the data was applied
        let root_page = file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(&mut file, root_page).expect("open index");
        assert!(index.get(&[1u8; 16], &[1u8; 16]).expect("get").is_some());
    }

    #[test]
    fn test_recover_short_insert_record_ignored() {
        // Test that insert records with bytes < 32 are silently ignored
        // This tests the safety check at line 139: if bytes.len() >= 32
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let hlc = HlcTimestamp::new(1000, 0);

        // Write a transaction with a malformed insert (bytes too short)
        {
            let mut wal = file.wal().expect("get wal");

            wal.append(1, hlc, LogRecordPayload::Begin).expect("begin");

            // Manually create an insert with too few bytes
            // This simulates corruption or a bug that produced a short record
            let short_bytes = vec![0u8; 16]; // Only 16 bytes, need at least 32
            wal.append(1, hlc, LogRecordPayload::Insert(short_bytes))
                .expect("insert");

            wal.append(1, hlc, LogRecordPayload::Commit)
                .expect("commit");

            wal.sync().expect("sync");
            let head = wal.head();
            let last_lsn = wal.last_lsn();
            #[allow(clippy::drop_non_drop)]
            drop(wal);
            file.update_wal_head(head, last_lsn);
        }
        file.write_superblock().expect("write superblock");

        // Run recovery - should not error, just skip the malformed record
        let result = recover(&mut file).expect("recover");

        assert_eq!(result.transactions_replayed, 1);
        // The short insert should be ignored, so 0 operations
        assert_eq!(result.operations_applied, 0);
    }
}
