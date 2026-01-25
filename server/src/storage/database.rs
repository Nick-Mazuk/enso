//! High-level database interface with WAL and crash recovery.
//!
//! This module provides a durable database with:
//! - Write-ahead logging for crash recovery
//! - Automatic checkpointing
//! - Transaction support with proper durability guarantees
//!
//! # Usage
//!
//! ```no_run
//! use server::storage::Database;
//! use std::path::Path;
//!
//! // Create a new database (initializes WAL)
//! let path = Path::new("/tmp/my_database");
//! let mut db = Database::create(path).unwrap();
//!
//! // Begin a transaction
//! let entity_id = [1u8; 16];
//! let attr_id = [2u8; 16];
//! let value = server::storage::TripleValue::String("hello".to_string());
//!
//! let mut txn = db.begin().unwrap();
//! txn.insert(entity_id, attr_id, value); // Buffers the insert
//! txn.commit().unwrap();  // Writes to WAL, then applies to index
//! ```

use std::collections::BTreeSet;
use std::path::Path;

use tokio::sync::broadcast;

use crate::storage::checkpoint::{
    CheckpointConfig, CheckpointError, CheckpointResult, CheckpointState, force_checkpoint,
    maybe_checkpoint,
};
use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::hlc::{Clock, ClockError};
use crate::storage::indexes::attribute::{AttributeIndex, AttributeIndexError};
use crate::storage::indexes::entity_attribute::{EntityAttributeIndex, EntityAttributeIndexError};
use crate::storage::indexes::primary::{PrimaryIndex, PrimaryIndexError};
use crate::storage::recovery::{self, RecoveryError, RecoveryResult};
use crate::storage::superblock::HlcTimestamp;
use crate::storage::time::SystemTimeSource;
use crate::storage::triple::{
    AttributeId, EntityId, TripleError, TripleRecord, TripleValue, TxnId,
};
use crate::storage::wal::{DEFAULT_WAL_CAPACITY, LogRecordPayload, Lsn, WalError};
use crate::storage::{ChangeNotification, ChangeRecord, ChangeType};

/// Trait for applying operations to secondary indexes (attribute and entity-attribute).
///
/// This trait abstracts over the different argument orders used by secondary indexes,
/// allowing a single helper function to apply operations to both index types.
trait SecondaryIndexOps {
    type Error: Into<DatabaseError>;

    /// Apply an insert operation to the index.
    fn apply_insert(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        txn_id: TxnId,
    ) -> Result<(), Self::Error>;

    /// Apply a delete operation to the index.
    fn apply_delete(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        txn_id: TxnId,
    ) -> Result<(), Self::Error>;
}

impl SecondaryIndexOps for AttributeIndex<'_> {
    type Error = AttributeIndexError;

    fn apply_insert(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        txn_id: TxnId,
    ) -> Result<(), Self::Error> {
        // AttributeIndex uses (attribute_id, entity_id) order
        self.insert(attribute_id, entity_id, txn_id)
    }

    fn apply_delete(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        txn_id: TxnId,
    ) -> Result<(), Self::Error> {
        self.mark_deleted(attribute_id, entity_id, txn_id)?;
        Ok(())
    }
}

impl SecondaryIndexOps for EntityAttributeIndex<'_> {
    type Error = EntityAttributeIndexError;

    fn apply_insert(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        txn_id: TxnId,
    ) -> Result<(), Self::Error> {
        // EntityAttributeIndex uses (entity_id, attribute_id) order
        self.insert(entity_id, attribute_id, txn_id)
    }

    fn apply_delete(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
        txn_id: TxnId,
    ) -> Result<(), Self::Error> {
        self.mark_deleted(entity_id, attribute_id, txn_id)?;
        Ok(())
    }
}

/// Apply buffered operations to a secondary index.
///
/// This helper function applies Insert and Delete operations to any index
/// implementing `SecondaryIndexOps`. Update operations are skipped as they
/// don't change the entity-attribute mapping in secondary indexes.
fn apply_ops_to_secondary_index<I: SecondaryIndexOps>(
    index: &mut I,
    operations: &[BufferedOp],
    txn_id: TxnId,
) -> Result<(), DatabaseError> {
    for op in operations {
        match op {
            BufferedOp::Insert {
                entity_id,
                attribute_id,
                ..
            } => {
                index
                    .apply_insert(entity_id, attribute_id, txn_id)
                    .map_err(Into::into)?;
            }
            BufferedOp::Update { .. } => {
                // Updates don't change the entity-attribute mapping in secondary indexes
            }
            BufferedOp::Delete {
                entity_id,
                attribute_id,
            } => {
                index
                    .apply_delete(entity_id, attribute_id, txn_id)
                    .map_err(Into::into)?;
            }
        }
    }
    Ok(())
}

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
    ///
    /// # Panics
    /// Panics if the `txn_id` is already registered (indicates a programming error).
    fn register(&mut self, txn_id: TxnId) {
        let inserted = self.active.insert(txn_id);
        assert!(
            inserted,
            "Snapshot txn_id {txn_id} already registered - duplicate snapshot registration"
        );
    }

    /// Unregister a snapshot when it's released.
    ///
    /// # Panics
    /// Panics if the `txn_id` was not registered (indicates a programming error).
    fn unregister(&mut self, txn_id: TxnId) {
        let removed = self.active.remove(&txn_id);
        assert!(
            removed,
            "Snapshot txn_id {txn_id} was not registered - releasing unregistered snapshot"
        );
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

/// Default capacity for the change notification broadcast channel.
const DEFAULT_BROADCAST_CAPACITY: usize = 1000;

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
///
/// # Change Notifications
///
/// The database broadcasts change notifications when transactions commit.
/// Use `subscribe_to_changes()` to receive notifications of all committed changes.
pub struct Database {
    file: DatabaseFile,
    checkpoint_state: CheckpointState,
    /// Hybrid Logical Clock for transaction timestamps.
    clock: Clock<SystemTimeSource>,
    /// Tracks active read-only snapshots for garbage collection.
    active_snapshots: ActiveSnapshots,
    /// Broadcast sender for change notifications.
    change_tx: broadcast::Sender<ChangeNotification>,
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
        let clock = Clock::new(node_id, SystemTimeSource);

        // Create broadcast channel for change notifications
        let (change_tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);

        Ok(Self {
            file,
            checkpoint_state,
            clock,
            active_snapshots: ActiveSnapshots::default(),
            change_tx,
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
        let clock = Clock::from_timestamp(node_id, last_hlc, SystemTimeSource);

        // Create broadcast channel for change notifications
        let (change_tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);

        Ok((
            Self {
                file,
                checkpoint_state,
                clock,
                active_snapshots: ActiveSnapshots::default(),
                change_tx,
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
    ///
    /// # Panics
    /// Panics if transaction ID is 0 (indicates uninitialized database state).
    #[allow(clippy::disallowed_methods)] // Clone needed for broadcast sender
    pub fn begin(&mut self) -> Result<WalTransaction<'_>, DatabaseError> {
        // Get next transaction ID
        let txn_id = self.file.superblock().next_txn_id;

        // Invariant: transaction IDs must be positive (0 is reserved for "no transaction")
        assert!(
            txn_id > 0,
            "Transaction ID must be positive, got {txn_id} - database may be uninitialized"
        );

        // Advance HLC using proper wall clock
        let hlc = self.clock.tick();

        Ok(WalTransaction::new(
            &mut self.file,
            &mut self.checkpoint_state,
            &mut self.clock,
            txn_id,
            hlc,
            self.change_tx.clone(),
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
    /// ```no_run
    /// use server::storage::Database;
    /// use std::path::Path;
    ///
    /// let path = Path::new("/tmp/my_database");
    /// let mut db = Database::create(path).unwrap();
    ///
    /// let mut snapshot = db.begin_readonly();
    /// let entity = [1u8; 16];
    /// let attr = [2u8; 16];
    /// let record = snapshot.get(&entity, &attr);
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

        // Empty database has root_page = 0, which is valid - nothing to GC
        if root_page == 0 {
            return Ok(GcResult {
                records_scanned: 0,
                records_removed: 0,
                bytes_freed: 0,
                min_active_snapshot: min_active,
            });
        }

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

        // Second pass: remove from all indexes
        let records_removed = to_remove.len() as u64;
        let mut bytes_freed = 0u64;

        if !to_remove.is_empty() {
            // Remove from primary index
            let primary_root = {
                let mut index = PrimaryIndex::new(&mut self.file, root_page)?;
                for (entity_id, attribute_id) in &to_remove {
                    if let Some(removed) = index.remove(entity_id, attribute_id)? {
                        bytes_freed += removed.serialized_size() as u64;
                    }
                }
                index.root_page()
            };

            // Remove from attribute index
            let attribute_root = {
                let attr_root_page = self.file.superblock().attribute_index_root;
                let mut index = AttributeIndex::new(&mut self.file, attr_root_page)?;
                for (entity_id, attribute_id) in &to_remove {
                    index.remove(attribute_id, entity_id)?;
                }
                index.root_page()
            };

            // Remove from entity-attribute index
            let entity_attr_root = {
                let ea_root_page = self.file.superblock().entity_attribute_index_root;
                let mut index = EntityAttributeIndex::new(&mut self.file, ea_root_page)?;
                for (entity_id, attribute_id) in &to_remove {
                    index.remove(entity_id, attribute_id)?;
                }
                index.root_page()
            };

            // Update root pages
            self.file.superblock_mut().primary_index_root = primary_root;
            self.file.superblock_mut().attribute_index_root = attribute_root;
            self.file.superblock_mut().entity_attribute_index_root = entity_attr_root;
            self.file.write_superblock()?;
            self.file.sync()?;
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

    /// Subscribe to change notifications.
    ///
    /// Returns a receiver that will receive all change notifications broadcast
    /// after this call. Use this to implement real-time subscriptions.
    ///
    /// Multiple subscribers can exist simultaneously - each receives all changes.
    #[must_use]
    pub fn subscribe_to_changes(&self) -> broadcast::Receiver<ChangeNotification> {
        self.change_tx.subscribe()
    }
}

/// A buffered operation for WAL transaction.
#[derive(Debug)]
enum BufferedOp {
    Insert {
        entity_id: EntityId,
        attribute_id: AttributeId,
        value: TripleValue,
        /// Optional client-provided HLC for conflict resolution.
        /// If None, uses the transaction's HLC.
        hlc: Option<HlcTimestamp>,
    },
    Update {
        entity_id: EntityId,
        attribute_id: AttributeId,
        value: TripleValue,
        /// Optional client-provided HLC for conflict resolution.
        /// If None, uses the transaction's HLC.
        hlc: Option<HlcTimestamp>,
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
    clock: &'a mut Clock<SystemTimeSource>,
    txn_id: TxnId,
    hlc: HlcTimestamp,
    /// Buffered operations to be written on commit
    operations: Vec<BufferedOp>,
    /// Whether this transaction has been finalized
    finalized: bool,
    /// Broadcast sender for change notifications.
    change_tx: broadcast::Sender<ChangeNotification>,
}

impl<'a> WalTransaction<'a> {
    #[allow(clippy::missing_const_for_fn)] // broadcast::Sender is not const
    fn new(
        file: &'a mut DatabaseFile,
        checkpoint_state: &'a mut CheckpointState,
        clock: &'a mut Clock<SystemTimeSource>,
        txn_id: TxnId,
        hlc: HlcTimestamp,
        change_tx: broadcast::Sender<ChangeNotification>,
    ) -> Self {
        Self {
            file,
            checkpoint_state,
            clock,
            txn_id,
            hlc,
            operations: Vec::new(),
            finalized: false,
            change_tx,
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
    pub fn scan_entity(
        &mut self,
        entity_id: &EntityId,
    ) -> Result<Vec<TripleRecord>, DatabaseError> {
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

    /// Get all entity IDs that have a given attribute.
    ///
    /// Uses the attribute index for efficient lookup.
    /// Note: This reads from committed state, not buffered operations.
    pub fn get_entities_with_attribute(
        &mut self,
        attribute_id: &AttributeId,
    ) -> Result<Vec<EntityId>, DatabaseError> {
        let root_page = self.file.superblock().attribute_index_root;
        let mut index = AttributeIndex::new(self.file, root_page)?;
        let mut scan = index.scan_attribute(attribute_id)?;

        let mut entities = Vec::new();
        while let Some(entity_id) = scan.next_entity()? {
            entities.push(entity_id);
        }

        Ok(entities)
    }

    /// Get all attribute IDs for a given entity.
    ///
    /// Uses the entity-attribute index for efficient lookup.
    /// Note: This reads from committed state, not buffered operations.
    pub fn get_attributes_for_entity(
        &mut self,
        entity_id: &EntityId,
    ) -> Result<Vec<AttributeId>, DatabaseError> {
        let root_page = self.file.superblock().entity_attribute_index_root;
        let mut index = EntityAttributeIndex::new(self.file, root_page)?;
        let mut scan = index.scan_entity(entity_id)?;

        let mut attributes = Vec::new();
        while let Some(attribute_id) = scan.next_attribute()? {
            attributes.push(attribute_id);
        }

        Ok(attributes)
    }

    /// Insert a triple.
    ///
    /// The operation is buffered until commit.
    /// Uses the transaction's HLC timestamp.
    pub fn insert(&mut self, entity_id: EntityId, attribute_id: AttributeId, value: TripleValue) {
        self.operations.push(BufferedOp::Insert {
            entity_id,
            attribute_id,
            value,
            hlc: None,
        });
    }

    /// Insert a triple with a client-provided HLC timestamp.
    ///
    /// The operation is buffered until commit.
    /// Uses the provided HLC instead of the transaction's HLC for conflict resolution.
    pub fn insert_with_hlc(
        &mut self,
        entity_id: EntityId,
        attribute_id: AttributeId,
        value: TripleValue,
        hlc: HlcTimestamp,
    ) {
        self.operations.push(BufferedOp::Insert {
            entity_id,
            attribute_id,
            value,
            hlc: Some(hlc),
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
            hlc: None,
        });
        Ok(())
    }

    /// Update a triple with a specific HLC timestamp.
    ///
    /// Similar to `update()` but uses the provided HLC instead of the
    /// transaction's clock. Used for client-provided updates where the
    /// client's HLC should be preserved.
    ///
    /// The operation is buffered until commit.
    pub fn update_with_hlc(
        &mut self,
        entity_id: EntityId,
        attribute_id: AttributeId,
        value: TripleValue,
        hlc: HlcTimestamp,
    ) {
        self.operations.push(BufferedOp::Update {
            entity_id,
            attribute_id,
            value,
            hlc: Some(hlc),
        });
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
    /// 6. Broadcasts change notifications
    /// 7. Updates superblock
    /// 8. Optionally triggers checkpoint
    ///
    /// # Panics
    /// Panics if the transaction was already finalized.
    pub fn commit(mut self) -> Result<(), DatabaseError> {
        // Invariant: transaction must not already be finalized
        assert!(
            !self.finalized,
            "Transaction already finalized - cannot commit twice"
        );

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

        // Step 6: Broadcast change notifications
        self.broadcast_changes(hlc);

        // Step 7: Update superblock
        self.file.superblock_mut().next_txn_id = txn_id + 1;
        self.file.write_superblock()?;
        self.file.sync()?;

        // Step 8: Update checkpoint state and maybe checkpoint
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
                    hlc: op_hlc,
                } => {
                    // Use per-op HLC if provided, otherwise use transaction HLC
                    let record_hlc = op_hlc.unwrap_or(hlc);
                    let record = TripleRecord::new(
                        *entity_id,
                        *attribute_id,
                        txn_id,
                        record_hlc,
                        value.clone_value(),
                    );
                    let payload = LogRecordPayload::insert(&record);
                    total_bytes += payload.serialized_size() as u64;
                    wal.append(txn_id, record_hlc, payload)?;
                }
                BufferedOp::Update {
                    entity_id,
                    attribute_id,
                    value,
                    hlc: op_hlc,
                } => {
                    // Use per-op HLC if provided, otherwise use transaction HLC
                    let record_hlc = op_hlc.unwrap_or(hlc);
                    let record = TripleRecord::new(
                        *entity_id,
                        *attribute_id,
                        txn_id,
                        record_hlc,
                        value.clone_value(),
                    );
                    let payload = LogRecordPayload::update(&record);
                    total_bytes += payload.serialized_size() as u64;
                    wal.append(txn_id, record_hlc, payload)?;
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

    /// Apply buffered operations to all indexes.
    fn apply_to_index(&mut self, txn_id: TxnId, hlc: HlcTimestamp) -> Result<(), DatabaseError> {
        // Apply to primary index
        let primary_root = {
            let root_page = self.file.superblock().primary_index_root;
            let mut index = PrimaryIndex::new(self.file, root_page)?;

            for op in &self.operations {
                match op {
                    BufferedOp::Insert {
                        entity_id,
                        attribute_id,
                        value,
                        hlc: op_hlc,
                    } => {
                        // Use per-op HLC if provided, otherwise use transaction HLC
                        let record_hlc = op_hlc.unwrap_or(hlc);
                        let record = TripleRecord::new(
                            *entity_id,
                            *attribute_id,
                            txn_id,
                            record_hlc,
                            value.clone_value(),
                        );
                        index.insert(&record)?;
                    }
                    BufferedOp::Update {
                        entity_id,
                        attribute_id,
                        value,
                        hlc: op_hlc,
                    } => {
                        // Use per-op HLC if provided, otherwise use transaction HLC
                        let record_hlc = op_hlc.unwrap_or(hlc);
                        let record = TripleRecord::new(
                            *entity_id,
                            *attribute_id,
                            txn_id,
                            record_hlc,
                            value.clone_value(),
                        );
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

            index.root_page()
        };

        // Apply to attribute index (attribute_id -> entity_id)
        let attribute_root = {
            let root_page = self.file.superblock().attribute_index_root;
            let mut index = AttributeIndex::new(self.file, root_page)?;
            apply_ops_to_secondary_index(&mut index, &self.operations, txn_id)?;
            index.root_page()
        };

        // Apply to entity-attribute index (entity_id -> attribute_id)
        let entity_attribute_root = {
            let root_page = self.file.superblock().entity_attribute_index_root;
            let mut index = EntityAttributeIndex::new(self.file, root_page)?;
            apply_ops_to_secondary_index(&mut index, &self.operations, txn_id)?;
            index.root_page()
        };

        // Invariant: root pages must be valid (non-zero) after operations
        assert!(
            primary_root > 0,
            "Primary index root page is 0 after apply_to_index - index corruption"
        );
        assert!(
            attribute_root > 0,
            "Attribute index root page is 0 after apply_to_index - index corruption"
        );
        assert!(
            entity_attribute_root > 0,
            "Entity-attribute index root page is 0 after apply_to_index - index corruption"
        );

        // Update root pages in superblock
        self.file.superblock_mut().primary_index_root = primary_root;
        self.file.superblock_mut().attribute_index_root = attribute_root;
        self.file.superblock_mut().entity_attribute_index_root = entity_attribute_root;

        Ok(())
    }

    /// Broadcast change notifications to all subscribers.
    fn broadcast_changes(&self, hlc: HlcTimestamp) {
        if self.operations.is_empty() {
            return;
        }

        let changes: Vec<ChangeRecord> = self
            .operations
            .iter()
            .map(|op| match op {
                BufferedOp::Insert {
                    entity_id,
                    attribute_id,
                    value,
                    hlc: op_hlc,
                } => ChangeRecord {
                    change_type: ChangeType::Insert,
                    entity_id: *entity_id,
                    attribute_id: *attribute_id,
                    value: Some(value.clone_value()),
                    hlc: op_hlc.unwrap_or(hlc),
                },
                BufferedOp::Update {
                    entity_id,
                    attribute_id,
                    value,
                    hlc: op_hlc,
                } => ChangeRecord {
                    change_type: ChangeType::Update,
                    entity_id: *entity_id,
                    attribute_id: *attribute_id,
                    value: Some(value.clone_value()),
                    hlc: op_hlc.unwrap_or(hlc),
                },
                BufferedOp::Delete {
                    entity_id,
                    attribute_id,
                } => ChangeRecord {
                    change_type: ChangeType::Delete,
                    entity_id: *entity_id,
                    attribute_id: *attribute_id,
                    value: None,
                    hlc,
                },
            })
            .collect();

        // Ignore send errors - no subscribers is not an error
        let _ = self.change_tx.send(ChangeNotification { changes });
    }

    /// Abort the transaction.
    ///
    /// Discards all buffered operations without writing to WAL.
    ///
    /// # Panics
    /// Panics if the transaction was already finalized.
    pub fn abort(mut self) {
        // Invariant: transaction must not already be finalized
        assert!(
            !self.finalized,
            "Transaction already finalized - cannot abort twice"
        );

        self.finalized = true;
        self.operations.clear();
    }
}

impl Drop for WalTransaction<'_> {
    fn drop(&mut self) {
        assert!(
            self.finalized,
            "WalTransaction dropped without commit() or abort() - this is a programming error. \
             Transactions must be explicitly finalized to ensure data integrity."
        );
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
    pub fn scan_entity(
        &mut self,
        entity_id: &EntityId,
    ) -> Result<Vec<TripleRecord>, DatabaseError> {
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

    /// Get all entity IDs that have a given attribute.
    ///
    /// Uses the attribute index for efficient lookup.
    /// Returns only entities visible at this snapshot.
    pub fn get_entities_with_attribute(
        &mut self,
        attribute_id: &AttributeId,
    ) -> Result<Vec<EntityId>, DatabaseError> {
        let root_page = self.file.superblock().attribute_index_root;
        let mut index = AttributeIndex::new(self.file, root_page)?;
        let mut scan = index.scan_attribute_visible(attribute_id, self.txn_id)?;

        let mut entities = Vec::new();
        while let Some(entity_id) = scan.next_entity()? {
            entities.push(entity_id);
        }

        Ok(entities)
    }

    /// Get all attribute IDs for a given entity.
    ///
    /// Uses the entity-attribute index for efficient lookup.
    /// Returns only attributes visible at this snapshot.
    pub fn get_attributes_for_entity(
        &mut self,
        entity_id: &EntityId,
    ) -> Result<Vec<AttributeId>, DatabaseError> {
        let root_page = self.file.superblock().entity_attribute_index_root;
        let mut index = EntityAttributeIndex::new(self.file, root_page)?;
        let mut scan = index.scan_entity_visible(entity_id, self.txn_id)?;

        let mut attributes = Vec::new();
        while let Some(attribute_id) = scan.next_attribute()? {
            attributes.push(attribute_id);
        }

        Ok(attributes)
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
    /// Primary index error.
    Index(PrimaryIndexError),
    /// Attribute index error.
    AttributeIndex(AttributeIndexError),
    /// Entity-attribute index error.
    EntityAttributeIndex(EntityAttributeIndexError),
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
    /// Mutex lock was poisoned.
    LockPoisoned,
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(e) => write!(f, "file error: {e}"),
            Self::Wal(e) => write!(f, "WAL error: {e}"),
            Self::Index(e) => write!(f, "primary index error: {e}"),
            Self::AttributeIndex(e) => write!(f, "attribute index error: {e}"),
            Self::EntityAttributeIndex(e) => write!(f, "entity-attribute index error: {e}"),
            Self::Triple(e) => write!(f, "triple error: {e}"),
            Self::Recovery(e) => write!(f, "recovery error: {e}"),
            Self::Checkpoint(e) => write!(f, "checkpoint error: {e}"),
            Self::Clock(e) => write!(f, "clock error: {e}"),
            Self::NotFound => write!(f, "triple not found"),
            Self::LockPoisoned => write!(f, "database lock poisoned"),
        }
    }
}

impl std::error::Error for DatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(e) => Some(e),
            Self::Wal(e) => Some(e),
            Self::Index(e) => Some(e),
            Self::AttributeIndex(e) => Some(e),
            Self::EntityAttributeIndex(e) => Some(e),
            Self::Triple(e) => Some(e),
            Self::Recovery(e) => Some(e),
            Self::Checkpoint(e) => Some(e),
            Self::Clock(e) => Some(e),
            Self::NotFound | Self::LockPoisoned => None,
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

impl From<AttributeIndexError> for DatabaseError {
    fn from(e: AttributeIndexError) -> Self {
        Self::AttributeIndex(e)
    }
}

impl From<EntityAttributeIndexError> for DatabaseError {
    fn from(e: EntityAttributeIndexError) -> Self {
        Self::EntityAttributeIndex(e)
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
            txn.insert(
                entity_id,
                attribute_id,
                TripleValue::String("hello".to_string()),
            );
            txn.commit().expect("commit");
        }

        // Reopen and verify (with recovery)
        {
            let (mut db, recovery) = Database::open(&path).expect("open db");

            // Recovery might have run
            if let Some(result) = recovery {
                // Verify recovery ran successfully
                assert!(result.transactions_replayed > 0);
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

            txn.insert(
                [1u8; 16],
                [2u8; 16],
                TripleValue::String("aborted".to_string()),
            );
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
            txn.insert(
                [1u8; 16],
                [1u8; 16],
                TripleValue::String("recovered".to_string()),
            );
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
            txn.insert(
                [1u8; 16],
                [1u8; 16],
                TripleValue::String("hello".to_string()),
            );
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
            assert!(
                record.is_none(),
                "snapshot at delete txn should not see record"
            );
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
        assert_eq!(result.records_removed, 5); // 5 deleted records removed
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

    #[test]
    fn test_secondary_index_entities_with_attribute() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        let attr1 = [1u8; 16];
        let attr2 = [2u8; 16];

        // Insert entities with various attributes
        {
            let mut txn = db.begin().expect("begin");
            // Entity 1 has attr1 and attr2
            txn.insert([1u8; 16], attr1, TripleValue::Number(1.0));
            txn.insert([1u8; 16], attr2, TripleValue::Number(2.0));
            // Entity 2 has only attr1
            txn.insert([2u8; 16], attr1, TripleValue::Number(3.0));
            // Entity 3 has only attr2
            txn.insert([3u8; 16], attr2, TripleValue::Number(4.0));
            txn.commit().expect("commit");
        }

        // Query entities with attr1 via transaction
        {
            let mut txn = db.begin().expect("begin");
            let entities = txn.get_entities_with_attribute(&attr1).expect("query");
            assert_eq!(entities.len(), 2);
            assert!(entities.contains(&[1u8; 16]));
            assert!(entities.contains(&[2u8; 16]));
            txn.abort();
        }

        // Query entities with attr2 via snapshot
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let entities = snapshot.get_entities_with_attribute(&attr2).expect("query");
            assert_eq!(entities.len(), 2);
            assert!(entities.contains(&[1u8; 16]));
            assert!(entities.contains(&[3u8; 16]));
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_secondary_index_attributes_for_entity() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        let entity1 = [1u8; 16];
        let entity2 = [2u8; 16];
        let attr1 = [10u8; 16];
        let attr2 = [20u8; 16];
        let attr3 = [30u8; 16];

        // Insert triples
        {
            let mut txn = db.begin().expect("begin");
            // Entity 1 has 3 attributes
            txn.insert(entity1, attr1, TripleValue::Number(1.0));
            txn.insert(entity1, attr2, TripleValue::Number(2.0));
            txn.insert(entity1, attr3, TripleValue::Number(3.0));
            // Entity 2 has 1 attribute
            txn.insert(entity2, attr1, TripleValue::Number(4.0));
            txn.commit().expect("commit");
        }

        // Query attributes for entity1 via transaction
        {
            let mut txn = db.begin().expect("begin");
            let attributes = txn.get_attributes_for_entity(&entity1).expect("query");
            assert_eq!(attributes.len(), 3);
            assert!(attributes.contains(&attr1));
            assert!(attributes.contains(&attr2));
            assert!(attributes.contains(&attr3));
            txn.abort();
        }

        // Query attributes for entity2 via snapshot
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let attributes = snapshot.get_attributes_for_entity(&entity2).expect("query");
            assert_eq!(attributes.len(), 1);
            assert!(attributes.contains(&attr1));
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_secondary_index_visibility() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        let entity = [1u8; 16];
        let attr1 = [10u8; 16];
        let attr2 = [20u8; 16];

        // Insert first attribute (txn_id = 1)
        {
            let mut txn = db.begin().expect("begin");
            txn.insert(entity, attr1, TripleValue::Number(1.0));
            txn.commit().expect("commit");
        }

        // Create snapshot at txn_id = 1
        let snapshot1_txn = {
            let snapshot = db.begin_readonly();
            snapshot.close()
        };

        // Insert second attribute (txn_id = 2)
        {
            let mut txn = db.begin().expect("begin");
            txn.insert(entity, attr2, TripleValue::Number(2.0));
            txn.commit().expect("commit");
        }

        // Delete first attribute (txn_id = 3)
        {
            let mut txn = db.begin().expect("begin");
            txn.delete(&entity, &attr1).expect("delete");
            txn.commit().expect("commit");
        }

        // Current snapshot should see only attr2
        let current_txn = {
            let mut snapshot = db.begin_readonly();
            let attributes = snapshot.get_attributes_for_entity(&entity).expect("query");
            assert_eq!(attributes.len(), 1);
            assert!(attributes.contains(&attr2));
            snapshot.close()
        };
        db.release_snapshot(current_txn);

        // Release snapshot1
        db.release_snapshot(snapshot1_txn);
    }

    #[test]
    fn test_secondary_index_gc_integration() {
        let (_dir, path) = create_test_db();
        let mut db = Database::create(&path).expect("create db");

        let entity = [1u8; 16];
        let attr = [10u8; 16];

        // Insert
        {
            let mut txn = db.begin().expect("begin");
            txn.insert(entity, attr, TripleValue::Number(1.0));
            txn.commit().expect("commit");
        }

        // Delete
        {
            let mut txn = db.begin().expect("begin");
            txn.delete(&entity, &attr).expect("delete");
            txn.commit().expect("commit");
        }

        // GC should clean up from all indexes
        let result = db.collect_garbage().expect("gc");
        assert_eq!(result.records_removed, 1);

        // Verify the record is gone from secondary indexes too
        {
            let mut txn = db.begin().expect("begin");
            let entities = txn.get_entities_with_attribute(&attr).expect("query");
            assert!(entities.is_empty());
            let attrs = txn.get_attributes_for_entity(&entity).expect("query");
            assert!(attrs.is_empty());
            txn.abort();
        }
    }
}
