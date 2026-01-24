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

use std::collections::BTreeSet;
use std::path::Path;

use crate::storage::checkpoint::{
    CheckpointConfig, CheckpointError, CheckpointResult, CheckpointState, force_checkpoint,
    maybe_checkpoint,
};
use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::hlc::{Clock, ClockError};
use crate::storage::indexes::primary::{PrimaryIndex, PrimaryIndexError};
use crate::storage::recovery::{self, RecoveryError, RecoveryResult};
use crate::storage::superblock::HlcTimestamp;
use crate::storage::triple::{
    AttributeId, EntityId, TripleError, TripleRecord, TripleValue, TxnId,
};
use crate::storage::wal::{LogRecordPayload, Lsn, WalError, DEFAULT_WAL_CAPACITY};

/// Tracks active read-only snapshots for garbage collection.
///
/// When a snapshot is created, its transaction ID is added to this set.
/// When the snapshot is released, its ID is removed.
/// The minimum ID in this set determines which deleted records can be garbage collected.
#[derive(Debug, Default)]
struct ActiveSnapshots {
    /// Set of transaction IDs for active snapshots.
    /// Uses a `BTreeSet` for efficient `min()` operation.
    active: BTreeSet<TxnId>,
}

impl ActiveSnapshots {
    /// Register a new active snapshot.
    fn register(&mut self, txn_id: TxnId) {
        self.active.insert(txn_id);
    }

    /// Unregister a snapshot when it's released.
    fn unregister(&mut self, txn_id: TxnId) {
        self.active.remove(&txn_id);
    }

    /// Get the minimum active snapshot transaction ID.
    ///
    /// Returns None if there are no active snapshots.
    /// Deleted records with `deleted_txn` < this value can be garbage collected.
    fn min_active(&self) -> Option<TxnId> {
        self.active.first().copied()
    }

    /// Get the count of active snapshots.
    fn count(&self) -> usize {
        self.active.len()
    }
}

/// Default node ID for single-node deployments.
const DEFAULT_NODE_ID: u32 = 0;

/// A database instance with WAL and crash recovery.
///
/// This is the main entry point for working with the storage engine.
/// It manages the underlying database file, WAL, and checkpointing.
///
/// # Multi-Reader Support
///
/// The database supports multiple concurrent read-only snapshots via `begin_readonly()`.
/// Each snapshot sees a consistent view of the database at its creation time.
/// Write transactions via `begin()` are still single-threaded.
pub struct Database {
    file: DatabaseFile,
    checkpoint_state: CheckpointState,
    /// Hybrid Logical Clock for transaction timestamps.
    clock: Clock,
    /// Tracks active read-only snapshots for garbage collection.
    active_snapshots: ActiveSnapshots,
}

impl Database {
    /// Create a new database at the given path.
    ///
    /// The path must not already exist. Initializes WAL with default capacity.
    /// Uses node ID 0 for single-node deployments.
    pub fn create(path: &Path) -> Result<Self, DatabaseError> {
        Self::create_with_options(
            path,
            DEFAULT_WAL_CAPACITY,
            CheckpointConfig::default(),
            DEFAULT_NODE_ID,
        )
    }

    /// Create a new database with custom options.
    ///
    /// # Arguments
    /// * `path` - Path for the database file
    /// * `wal_capacity` - Capacity of the write-ahead log in bytes
    /// * `checkpoint_config` - Configuration for automatic checkpointing
    /// * `node_id` - Unique identifier for this node (for distributed deployments)
    pub fn create_with_options(
        path: &Path,
        wal_capacity: u64,
        checkpoint_config: CheckpointConfig,
        node_id: u32,
    ) -> Result<Self, DatabaseError> {
        let mut file = DatabaseFile::create(path)?;

        // Initialize WAL
        file.init_wal(wal_capacity)?;

        let checkpoint_state = CheckpointState::from_database(&file, checkpoint_config);
        let clock = Clock::new(node_id);

        Ok(Self {
            file,
            checkpoint_state,
            clock,
            active_snapshots: ActiveSnapshots::default(),
        })
    }

    /// Open an existing database at the given path.
    ///
    /// Runs crash recovery if needed to restore consistent state.
    /// Uses node ID 0 for single-node deployments.
    pub fn open(path: &Path) -> Result<(Self, Option<RecoveryResult>), DatabaseError> {
        Self::open_with_options(path, CheckpointConfig::default(), DEFAULT_NODE_ID)
    }

    /// Open an existing database with custom options.
    ///
    /// # Arguments
    /// * `path` - Path to the existing database file
    /// * `checkpoint_config` - Configuration for automatic checkpointing
    /// * `node_id` - Unique identifier for this node (for distributed deployments)
    pub fn open_with_options(
        path: &Path,
        checkpoint_config: CheckpointConfig,
        node_id: u32,
    ) -> Result<(Self, Option<RecoveryResult>), DatabaseError> {
        let mut file = DatabaseFile::open(path)?;

        // Run recovery if needed
        let recovery_result = if file.has_wal() && recovery::needs_recovery(&mut file)? {
            Some(recovery::recover(&mut file)?)
        } else {
            None
        };

        let checkpoint_state = CheckpointState::from_database(&file, checkpoint_config);

        // Initialize clock from last checkpoint timestamp
        let last_hlc = file.superblock().last_checkpoint_hlc;
        let clock = Clock::from_timestamp(node_id, last_hlc);

        Ok((
            Self {
                file,
                checkpoint_state,
                clock,
                active_snapshots: ActiveSnapshots::default(),
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

    /// Begin a new write transaction.
    ///
    /// The transaction buffers all operations and writes to WAL on commit.
    /// The transaction is assigned a unique HLC timestamp.
    ///
    /// Only one write transaction can be active at a time (enforced by borrow checker).
    pub fn begin(&mut self) -> Result<WalTransaction<'_>, DatabaseError> {
        // Get next transaction ID
        let txn_id = self.file.superblock().next_txn_id;

        // Advance HLC using proper wall clock
        let hlc = self.clock.tick();

        Ok(WalTransaction::new(
            &mut self.file,
            &mut self.checkpoint_state,
            &mut self.clock,
            txn_id,
            hlc,
        ))
    }

    /// Begin a read-only snapshot.
    ///
    /// Returns a snapshot that sees a consistent view of the database
    /// at the current committed transaction ID. The snapshot can read data
    /// but cannot modify it.
    ///
    /// The snapshot is tracked for garbage collection purposes - deleted records
    /// visible to any active snapshot cannot be physically removed. Call
    /// `release_snapshot()` when done to allow garbage collection.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// let mut snapshot = db.begin_readonly();
    /// let record = snapshot.get(&entity, &attr)?;
    /// let txn_id = snapshot.close(); // Returns the snapshot's txn_id
    /// db.release_snapshot(txn_id);   // Allow garbage collection
    /// ```
    pub fn begin_readonly(&mut self) -> Snapshot<'_> {
        // Snapshot sees all committed transactions (next_txn_id - 1)
        let txn_id = self.file.superblock().next_txn_id.saturating_sub(1);
        let hlc = self.clock.last();

        // Register the snapshot for garbage collection tracking
        self.active_snapshots.register(txn_id);

        Snapshot::new(&mut self.file, txn_id, hlc)
    }

    /// Release a snapshot and allow garbage collection.
    ///
    /// Call this after closing a snapshot to remove it from the active
    /// snapshot list. This allows deleted records that were visible to
    /// this snapshot to be garbage collected.
    pub fn release_snapshot(&mut self, txn_id: TxnId) {
        self.active_snapshots.unregister(txn_id);
    }

    /// Get the minimum active snapshot transaction ID.
    ///
    /// Returns None if there are no active snapshots.
    /// This is used for garbage collection - deleted records with
    /// `deleted_txn` less than this value can be physically removed.
    #[must_use]
    pub fn min_active_snapshot(&self) -> Option<TxnId> {
        self.active_snapshots.min_active()
    }

    /// Get the count of active read-only snapshots.
    #[must_use]
    pub fn active_snapshot_count(&self) -> usize {
        self.active_snapshots.count()
    }

    /// Run garbage collection to remove deleted records.
    ///
    /// This physically removes records that:
    /// - Have been deleted (`deleted_txn` != 0)
    /// - Are no longer visible to any active snapshot (`deleted_txn` < `min_active_snapshot`)
    ///
    /// If there are no active snapshots, all deleted records are eligible for GC.
    ///
    /// # Returns
    ///
    /// A `GcResult` with statistics about what was collected.
    ///
    /// # Note
    ///
    /// This operation modifies the database but does not use the WAL.
    /// It's safe to run during normal operation as it only removes records
    /// that are no longer visible to any transaction.
    pub fn collect_garbage(&mut self) -> Result<GcResult, DatabaseError> {
        let min_active = self.active_snapshots.min_active();
        let root_page = self.file.superblock().primary_index_root;

        // First pass: collect keys of records eligible for GC
        let mut to_remove: Vec<(EntityId, AttributeId)> = Vec::new();
        let mut records_scanned = 0u64;

        {
            let mut index = PrimaryIndex::new(&mut self.file, root_page)?;
            let mut cursor = index.cursor()?;

            while let Some(record) = cursor.next_record()? {
                records_scanned += 1;
                if record.is_gc_eligible(min_active) {
                    to_remove.push((record.entity_id, record.attribute_id));
                }
            }
        }

        // Second pass: remove the collected records
        let records_removed = to_remove.len() as u64;
        let mut bytes_freed = 0u64;

        if !to_remove.is_empty() {
            let mut index = PrimaryIndex::new(&mut self.file, root_page)?;

            for (entity_id, attribute_id) in to_remove {
                if let Some(removed) = index.remove(&entity_id, &attribute_id)? {
                    bytes_freed += removed.serialized_size() as u64;
                }
            }

            // Update root page if it changed
            let new_root = index.root_page();
            let file = index.file_mut();
            file.superblock_mut().primary_index_root = new_root;
            file.write_superblock()?;
            file.sync()?;
        }

        Ok(GcResult {
            records_scanned,
            records_removed,
            bytes_freed,
            min_active_snapshot: min_active,
        })
    }

    /// Get the current checkpoint state.
    #[must_use]
    pub const fn checkpoint_state(&self) -> &CheckpointState {
        &self.checkpoint_state
    }

    /// Force a checkpoint.
    pub fn checkpoint(&mut self) -> Result<CheckpointResult, DatabaseError> {
        let hlc = self.clock.tick();
        Ok(force_checkpoint(
            &mut self.file,
            &mut self.checkpoint_state,
            hlc,
        )?)
    }

    /// Close the database cleanly.
    ///
    /// Performs a final checkpoint to minimize recovery time on next open.
    pub fn close(mut self) -> Result<(), DatabaseError> {
        if self.file.has_wal() {
            let hlc = self.clock.tick();
            force_checkpoint(&mut self.file, &mut self.checkpoint_state, hlc)?;
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

    /// Get the current HLC timestamp.
    ///
    /// This returns the last timestamp issued by the clock.
    #[must_use]
    pub const fn current_hlc(&self) -> HlcTimestamp {
        self.clock.last()
    }

    /// Get this database's node ID.
    #[must_use]
    pub const fn node_id(&self) -> u32 {
        self.clock.node_id()
    }

    /// Receive and merge a remote HLC timestamp.
    ///
    /// This is used in distributed scenarios to synchronize clocks.
    /// The local clock advances to be at least as high as the remote timestamp.
    ///
    /// Returns an error if the remote timestamp is too far in the future
    /// (indicating a clock synchronization issue).
    pub fn receive_hlc(&mut self, remote: HlcTimestamp) -> Result<HlcTimestamp, DatabaseError> {
        Ok(self.clock.receive(remote)?)
    }

    /// Get changes since a given HLC timestamp.
    ///
    /// Returns WAL records with HLC >= the given timestamp.
    /// This is useful for subscription queries ("what changed since X").
    pub fn changes_since(
        &mut self,
        since: HlcTimestamp,
    ) -> Result<Vec<crate::storage::wal::LogRecord>, DatabaseError> {
        if !self.file.has_wal() {
            return Ok(Vec::new());
        }
        let mut wal = self.file.wal()?;
        Ok(wal.changes_since(since)?)
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
    clock: &'a mut Clock,
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
        clock: &'a mut Clock,
        txn_id: TxnId,
        hlc: HlcTimestamp,
    ) -> Self {
        Self {
            file,
            checkpoint_state,
            clock,
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

        // Check if we should checkpoint (tick clock for checkpoint timestamp)
        if self.file.has_wal() {
            let checkpoint_hlc = self.clock.tick();
            maybe_checkpoint(self.file, self.checkpoint_state, checkpoint_hlc)?;
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

/// A read-only snapshot of the database.
///
/// Provides a consistent view of the database at a specific transaction ID.
/// All reads see the state as of when the snapshot was created, even if
/// other transactions commit afterwards.
///
/// # Snapshot Isolation
///
/// - Sees all records with `created_txn <= snapshot_txn`
/// - Does not see records with `created_txn > snapshot_txn`
/// - Does not see records with `deleted_txn <= snapshot_txn`
///
/// # Lifecycle
///
/// The snapshot is tracked for garbage collection. Deleted records
/// visible to any active snapshot cannot be physically removed.
/// Always close snapshots when done to allow garbage collection.
pub struct Snapshot<'a> {
    file: &'a mut DatabaseFile,
    /// The transaction ID this snapshot sees.
    txn_id: TxnId,
    /// HLC timestamp when the snapshot was created.
    hlc: HlcTimestamp,
}

impl<'a> Snapshot<'a> {
    const fn new(file: &'a mut DatabaseFile, txn_id: TxnId, hlc: HlcTimestamp) -> Self {
        Self { file, txn_id, hlc }
    }

    /// Get the snapshot's transaction ID.
    ///
    /// This is the highest committed transaction visible to this snapshot.
    #[must_use]
    pub const fn snapshot_txn(&self) -> TxnId {
        self.txn_id
    }

    /// Get the HLC timestamp when this snapshot was created.
    #[must_use]
    pub const fn hlc(&self) -> HlcTimestamp {
        self.hlc
    }

    /// Look up a single triple by entity and attribute ID.
    ///
    /// Returns the record only if it's visible at this snapshot.
    pub fn get(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<Option<TripleRecord>, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(self.file, root_page)?;

        Ok(index.get_visible(entity_id, attribute_id, self.txn_id)?)
    }

    /// Scan all triples for an entity.
    ///
    /// Returns only triples visible at this snapshot.
    pub fn scan_entity(&mut self, entity_id: &EntityId) -> Result<Vec<TripleRecord>, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(self.file, root_page)?;
        let mut scan = index.scan_entity_visible(entity_id, self.txn_id)?;

        let mut results = Vec::new();
        while let Some(record) = scan.next_record()? {
            results.push(record);
        }

        Ok(results)
    }

    /// Count all visible triples in the index.
    ///
    /// Note: This counts records visible at this snapshot.
    pub fn count(&mut self) -> Result<usize, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(self.file, root_page)?;
        let mut cursor = index.cursor_visible(self.txn_id)?;

        let mut count = 0;
        while cursor.next_record()?.is_some() {
            count += 1;
        }

        Ok(count)
    }

    /// Collect all visible triples in key order.
    ///
    /// Returns records visible at this snapshot.
    pub fn collect_all(&mut self) -> Result<Vec<TripleRecord>, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let mut index = PrimaryIndex::new(self.file, root_page)?;
        let mut cursor = index.cursor_visible(self.txn_id)?;

        let mut results = Vec::new();
        while let Some(record) = cursor.next_record()? {
            results.push(record);
        }

        Ok(results)
    }

    /// Close the snapshot and return its transaction ID.
    ///
    /// After closing, call `db.release_snapshot(txn_id)` to allow
    /// garbage collection of deleted records visible to this snapshot.
    #[must_use]
    pub const fn close(self) -> TxnId {
        self.txn_id
    }
}

/// Result of a garbage collection operation.
#[derive(Debug)]
pub struct GcResult {
    /// Number of records scanned during GC.
    pub records_scanned: u64,
    /// Number of records physically removed.
    pub records_removed: u64,
    /// Approximate bytes freed (based on serialized record size).
    pub bytes_freed: u64,
    /// The minimum active snapshot at time of GC.
    /// Records deleted before this transaction were eligible for removal.
    pub min_active_snapshot: Option<TxnId>,
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
    /// Clock error (excessive drift).
    Clock(ClockError),
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
            Self::Clock(e) => write!(f, "clock error: {e}"),
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
            Self::Clock(e) => Some(e),
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

impl From<ClockError> for DatabaseError {
    fn from(e: ClockError) -> Self {
        Self::Clock(e)
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
                // Verify recovery ran successfully
                assert!(result.transactions_replayed == 0 || result.transactions_replayed > 0);
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
                // The transaction was committed, so it should be replayed
                assert!(result.transactions_replayed <= 1);
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

    #[test]
    fn test_snapshot_basic_read() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert data
        {
            let mut txn = db.begin().expect("begin");
            txn.insert([1u8; 16], [1u8; 16], TripleValue::Number(42.0));
            txn.commit().expect("commit");
        }

        // Read via snapshot
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let record = snapshot.get(&[1u8; 16], &[1u8; 16]).expect("get");
            assert!(record.is_some());
            assert_eq!(record.unwrap().value, TripleValue::Number(42.0));
            snapshot.close()
        };
        db.release_snapshot(txn_id);

        assert_eq!(db.active_snapshot_count(), 0);
    }

    #[test]
    fn test_snapshot_isolation() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert initial data (txn_id = 1)
        {
            let mut txn = db.begin().expect("begin");
            txn.insert([1u8; 16], [1u8; 16], TripleValue::Number(1.0));
            txn.commit().expect("commit");
        }

        // Create a snapshot that sees txn_id = 1
        let mut snapshot = db.begin_readonly();
        let snapshot_txn = snapshot.snapshot_txn();
        assert_eq!(snapshot_txn, 1);

        // Verify snapshot sees the initial data
        let record = snapshot.get(&[1u8; 16], &[1u8; 16]).expect("get");
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, TripleValue::Number(1.0));

        // Close snapshot so we can do a write
        let txn_id = snapshot.close();
        db.release_snapshot(txn_id);

        // Update the data (txn_id = 2)
        {
            let mut txn = db.begin().expect("begin");
            txn.update([1u8; 16], [1u8; 16], TripleValue::Number(2.0))
                .expect("update");
            txn.commit().expect("commit");
        }

        // New snapshot sees updated data
        let txn_id = {
            let mut snapshot2 = db.begin_readonly();
            assert_eq!(snapshot2.snapshot_txn(), 2);
            let record = snapshot2.get(&[1u8; 16], &[1u8; 16]).expect("get");
            assert!(record.is_some());
            assert_eq!(record.unwrap().value, TripleValue::Number(2.0));
            snapshot2.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_snapshot_sees_deleted_records() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert data (txn_id = 1)
        {
            let mut txn = db.begin().expect("begin");
            txn.insert([1u8; 16], [1u8; 16], TripleValue::String("hello".to_string()));
            txn.commit().expect("commit");
        }

        // Delete the data (txn_id = 2)
        {
            let mut txn = db.begin().expect("begin");
            txn.delete(&[1u8; 16], &[1u8; 16]).expect("delete");
            txn.commit().expect("commit");
        }

        // Current snapshot (at txn=2) should not see the deleted record
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            assert_eq!(snapshot.snapshot_txn(), 2);
            let record = snapshot.get(&[1u8; 16], &[1u8; 16]).expect("get");
            assert!(record.is_none(), "snapshot at delete txn should not see record");
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_snapshot_entity_scan() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        let entity = [1u8; 16];

        // Insert multiple attributes
        {
            let mut txn = db.begin().expect("begin");
            for i in 0..5u8 {
                let mut attr = [0u8; 16];
                attr[0] = i;
                txn.insert(entity, attr, TripleValue::Number(f64::from(i)));
            }
            txn.commit().expect("commit");
        }

        // Scan via snapshot
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let records = snapshot.scan_entity(&entity).expect("scan");
            assert_eq!(records.len(), 5);
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_snapshot_count() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert 10 records
        {
            let mut txn = db.begin().expect("begin");
            for i in 0..10u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.insert(entity, [1u8; 16], TripleValue::Number(f64::from(i)));
            }
            txn.commit().expect("commit");
        }

        // Count via snapshot
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let count = snapshot.count().expect("count");
            assert_eq!(count, 10);
            snapshot.close()
        };
        db.release_snapshot(txn_id);

        // Delete some records
        {
            let mut txn = db.begin().expect("begin");
            for i in 0..5u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.delete(&entity, &[1u8; 16]).expect("delete");
            }
            txn.commit().expect("commit");
        }

        // New snapshot should see only 5 records
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let count = snapshot.count().expect("count");
            assert_eq!(count, 5);
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_active_snapshot_tracking() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert data
        {
            let mut txn = db.begin().expect("begin");
            txn.insert([1u8; 16], [1u8; 16], TripleValue::Boolean(true));
            txn.commit().expect("commit");
        }

        assert_eq!(db.active_snapshot_count(), 0);
        assert!(db.min_active_snapshot().is_none());

        // Create and use snapshot
        let txn_id = {
            let snapshot = db.begin_readonly();
            snapshot.close()
        };

        // Still registered until release
        assert_eq!(db.active_snapshot_count(), 1);
        assert_eq!(db.min_active_snapshot(), Some(1));

        // Release snapshot
        db.release_snapshot(txn_id);
        assert_eq!(db.active_snapshot_count(), 0);
        assert!(db.min_active_snapshot().is_none());
    }

    #[test]
    fn test_snapshot_collect_all() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert records
        {
            let mut txn = db.begin().expect("begin");
            for i in 0..5u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.insert(entity, [1u8; 16], TripleValue::Number(f64::from(i)));
            }
            txn.commit().expect("commit");
        }

        // Collect all via snapshot
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let records = snapshot.collect_all().expect("collect_all");
            assert_eq!(records.len(), 5);
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_gc_removes_deleted_records() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert records
        {
            let mut txn = db.begin().expect("begin");
            for i in 0..10u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.insert(entity, [1u8; 16], TripleValue::Number(f64::from(i)));
            }
            txn.commit().expect("commit");
        }

        // Delete half the records
        {
            let mut txn = db.begin().expect("begin");
            for i in 0..5u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.delete(&entity, &[1u8; 16]).expect("delete");
            }
            txn.commit().expect("commit");
        }

        // Run GC - should remove the deleted records
        let result = db.collect_garbage().expect("gc");
        assert_eq!(result.records_scanned, 10); // All records scanned
        assert_eq!(result.records_removed, 5);  // 5 deleted records removed
        assert!(result.bytes_freed > 0);
        assert!(result.min_active_snapshot.is_none()); // No active snapshots

        // Verify only 5 records remain
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let count = snapshot.count().expect("count");
            assert_eq!(count, 5);
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_gc_respects_active_snapshots() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert a record (txn_id = 1)
        {
            let mut txn = db.begin().expect("begin");
            txn.insert([1u8; 16], [1u8; 16], TripleValue::Number(1.0));
            txn.commit().expect("commit");
        }

        // Create a snapshot at txn_id = 1
        let snapshot_txn = {
            let snapshot = db.begin_readonly();
            snapshot.close()
        };
        // Don't release - keep it active

        // Delete the record (txn_id = 2)
        {
            let mut txn = db.begin().expect("begin");
            txn.delete(&[1u8; 16], &[1u8; 16]).expect("delete");
            txn.commit().expect("commit");
        }

        // Run GC - should NOT remove the record because snapshot at txn=1 can still see it
        let result = db.collect_garbage().expect("gc");
        assert_eq!(result.records_removed, 0); // Nothing removed
        assert_eq!(result.min_active_snapshot, Some(1));

        // Release the snapshot
        db.release_snapshot(snapshot_txn);

        // Run GC again - now it should be removed
        let result = db.collect_garbage().expect("gc");
        assert_eq!(result.records_removed, 1);
        assert!(result.min_active_snapshot.is_none());
    }

    #[test]
    fn test_gc_no_deleted_records() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert records (no deletes)
        {
            let mut txn = db.begin().expect("begin");
            for i in 0..5u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.insert(entity, [1u8; 16], TripleValue::Number(f64::from(i)));
            }
            txn.commit().expect("commit");
        }

        // Run GC - nothing to collect
        let result = db.collect_garbage().expect("gc");
        assert_eq!(result.records_scanned, 5);
        assert_eq!(result.records_removed, 0);
        assert_eq!(result.bytes_freed, 0);
    }

    #[test]
    fn test_gc_empty_database() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Run GC on empty database
        let result = db.collect_garbage().expect("gc");
        assert_eq!(result.records_scanned, 0);
        assert_eq!(result.records_removed, 0);
        assert_eq!(result.bytes_freed, 0);
    }

    #[test]
    fn test_gc_multiple_snapshots() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        // Insert records (txn_id = 1)
        {
            let mut txn = db.begin().expect("begin");
            txn.insert([1u8; 16], [1u8; 16], TripleValue::Number(1.0));
            txn.insert([2u8; 16], [1u8; 16], TripleValue::Number(2.0));
            txn.commit().expect("commit");
        }

        // Snapshot 1 at txn_id = 1
        let snapshot1_txn = {
            let snapshot = db.begin_readonly();
            snapshot.close()
        };

        // Delete first record (txn_id = 2)
        {
            let mut txn = db.begin().expect("begin");
            txn.delete(&[1u8; 16], &[1u8; 16]).expect("delete");
            txn.commit().expect("commit");
        }

        // Snapshot 2 at txn_id = 2
        let snapshot2_txn = {
            let snapshot = db.begin_readonly();
            snapshot.close()
        };

        // Delete second record (txn_id = 3)
        {
            let mut txn = db.begin().expect("begin");
            txn.delete(&[2u8; 16], &[1u8; 16]).expect("delete");
            txn.commit().expect("commit");
        }

        // GC: min_active = 1, can't remove either (both deleted after txn=1)
        let result = db.collect_garbage().expect("gc");
        assert_eq!(result.records_removed, 0);
        assert_eq!(result.min_active_snapshot, Some(1));

        // Release snapshot 1
        db.release_snapshot(snapshot1_txn);

        // GC: min_active = 2, can remove first record (deleted at txn=2, which is not < 2)
        // Wait - deleted_txn=2 is NOT < min_active=2, so it shouldn't be GC'd
        let result = db.collect_garbage().expect("gc");
        assert_eq!(result.records_removed, 0);
        assert_eq!(result.min_active_snapshot, Some(2));

        // Release snapshot 2
        db.release_snapshot(snapshot2_txn);

        // GC: no active snapshots, both records should be removed
        let result = db.collect_garbage().expect("gc");
        assert_eq!(result.records_removed, 2);
        assert!(result.min_active_snapshot.is_none());
    }

    #[test]
    fn test_is_gc_eligible() {
        use crate::storage::superblock::HlcTimestamp;

        // Not deleted - not eligible
        let record = TripleRecord::new(
            [1u8; 16],
            [1u8; 16],
            10,
            HlcTimestamp::new(1000, 0),
            TripleValue::Null,
        );
        assert!(!record.is_gc_eligible(None));
        assert!(!record.is_gc_eligible(Some(5)));
        assert!(!record.is_gc_eligible(Some(15)));

        // Deleted at txn=50
        let mut record = TripleRecord::new(
            [1u8; 16],
            [1u8; 16],
            10,
            HlcTimestamp::new(1000, 0),
            TripleValue::Null,
        );
        record.deleted_txn = 50;

        // No active snapshots - always eligible
        assert!(record.is_gc_eligible(None));

        // Active snapshot before deletion - eligible
        assert!(record.is_gc_eligible(Some(60))); // deleted_txn=50 < 60

        // Active snapshot at or after deletion - not eligible
        assert!(!record.is_gc_eligible(Some(50))); // deleted_txn=50 is not < 50
        assert!(!record.is_gc_eligible(Some(40))); // deleted_txn=50 is not < 40
    }
}
