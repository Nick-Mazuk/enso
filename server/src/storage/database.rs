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
//! use server::storage::buffer_pool::BufferPool;
//! use server::types::{EntityId, AttributeId, TripleValue};
//! use std::path::Path;
//!
//! // Create a buffer pool and new database (initializes WAL)
//! let pool = BufferPool::new(100);
//! let path = Path::new("/tmp/my_database");
//! let mut db = Database::create(path, pool).unwrap();
//!
//! // Begin a transaction
//! let entity_id = EntityId([1u8; 16]);
//! let attr_id = AttributeId([2u8; 16]);
//! let value = TripleValue::String("hello".to_string());
//!
//! let mut txn = db.begin(0).unwrap(); // 0 = connection ID
//! txn.insert(entity_id, attr_id, value); // Buffers the insert
//! txn.commit().unwrap();  // Writes to WAL, then applies to index
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::storage::FilteredChangeReceiver;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::checkpoint::{
    CheckpointConfig, CheckpointError, CheckpointResult, CheckpointState, force_checkpoint,
    maybe_checkpoint,
};
use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::hlc::{Clock, ClockError};
#[cfg(unix)]
use crate::storage::indexes::attribute::AttributeIndexReader;
use crate::storage::indexes::attribute::{AttributeIndex, AttributeIndexError};
#[cfg(unix)]
use crate::storage::indexes::entity_attribute::EntityAttributeIndexReader;
use crate::storage::indexes::entity_attribute::{EntityAttributeIndex, EntityAttributeIndexError};
#[cfg(unix)]
use crate::storage::indexes::primary::PrimaryIndexReader;
use crate::storage::indexes::primary::{PrimaryIndex, PrimaryIndexError};
use crate::storage::recovery::{self, RecoveryError, RecoveryResult};
use crate::storage::time::SystemTimeSource;
use crate::storage::tombstone::{Tombstone, TombstoneError, TombstoneList};
use crate::storage::wal::{DEFAULT_WAL_CAPACITY, LogRecordPayload, Lsn, WalError};
use crate::types::{
    AttributeId, ChangeNotification, ChangeRecord, ChangeType, ConnectionId, EntityId,
    HlcTimestamp, PendingTriple, TripleError, TripleRecord, TripleValue, TxnId,
};

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
    operations: &[PendingTriple],
    txn_id: TxnId,
) -> Result<(), DatabaseError> {
    for op in operations {
        match op {
            PendingTriple::Insert(record) => {
                index
                    .apply_insert(&record.entity_id, &record.attribute_id, txn_id)
                    .map_err(Into::into)?;
            }
            PendingTriple::Update(_) => {
                // Updates don't change the entity-attribute mapping in secondary indexes
            }
            PendingTriple::Delete {
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
/// When a snapshot is created, its transaction ID is added to this map with a reference count.
/// When the snapshot is released, the count is decremented (or removed when it reaches 0).
/// The minimum ID in this map determines which deleted records can be garbage collected.
///
/// Uses interior mutability via `Mutex` to allow concurrent snapshot registration
/// without requiring exclusive access to the containing `Database`.
#[derive(Debug, Default)]
struct ActiveSnapshots {
    /// Map of transaction IDs to reference counts for active snapshots.
    /// Uses a `BTreeMap` for efficient `min()` operation.
    /// Wrapped in `Mutex` to allow concurrent access.
    active: Mutex<BTreeMap<TxnId, usize>>,
}

impl ActiveSnapshots {
    /// Register a new active snapshot.
    ///
    /// Multiple snapshots can be registered at the same `txn_id` (concurrent reads).
    ///
    /// # Panics
    /// Panics if the mutex is poisoned.
    fn register(&self, txn_id: TxnId) {
        let Ok(mut active) = self.active.lock() else {
            panic!("ActiveSnapshots mutex poisoned");
        };
        *active.entry(txn_id).or_insert(0) += 1;
    }

    /// Unregister a snapshot when it's released.
    ///
    /// Decrements the reference count, removing the entry when it reaches 0.
    ///
    /// # Panics
    /// Panics if the `txn_id` was not registered (indicates a programming error).
    /// Panics if the mutex is poisoned.
    fn unregister(&self, txn_id: TxnId) {
        let Ok(mut active) = self.active.lock() else {
            panic!("ActiveSnapshots mutex poisoned");
        };
        match active.get_mut(&txn_id) {
            Some(count) if *count > 1 => {
                *count -= 1;
            }
            Some(_) => {
                active.remove(&txn_id);
            }
            None => {
                panic!(
                    "Snapshot txn_id {txn_id} was not registered - releasing unregistered snapshot"
                );
            }
        }
    }

    /// Get the minimum active snapshot transaction ID.
    ///
    /// Returns None if there are no active snapshots.
    /// Deleted records with `deleted_txn` < this value can be garbage collected.
    ///
    /// # Panics
    /// Panics if the mutex is poisoned.
    fn min_active(&self) -> Option<TxnId> {
        let Ok(active) = self.active.lock() else {
            panic!("ActiveSnapshots mutex poisoned");
        };
        active.first_key_value().map(|(&txn_id, _)| txn_id)
    }

    /// Get the count of active snapshots (total across all `txn_id`s).
    ///
    /// # Panics
    /// Panics if the mutex is poisoned.
    fn count(&self) -> usize {
        let Ok(active) = self.active.lock() else {
            panic!("ActiveSnapshots mutex poisoned");
        };
        active.values().sum()
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
///
/// # Incremental Garbage Collection
///
/// Deleted records are tracked in a tombstone list. A background GC task processes
/// tombstones incrementally after each commit, removing records that are no longer
/// visible to any active snapshot. Use `gc_notify()` to get a handle for signaling
/// the GC task.
pub struct Database {
    file: DatabaseFile,
    checkpoint_state: CheckpointState,
    /// Hybrid Logical Clock for transaction timestamps.
    clock: Clock<SystemTimeSource>,
    /// Tracks active read-only snapshots for garbage collection.
    active_snapshots: ActiveSnapshots,
    /// Broadcast sender for change notifications.
    change_tx: broadcast::Sender<ChangeNotification>,
    /// Disk-based linked list of tombstones (deleted records awaiting GC).
    tombstone_list: TombstoneList,
    /// Notifier for signaling the background GC task.
    gc_notify: Arc<tokio::sync::Notify>,
}

impl Database {
    /// Create a new database at the given path.
    ///
    /// The path must not already exist. Initializes WAL with default capacity.
    /// Uses node ID 0 for single-node deployments.
    pub fn create(path: &Path, pool: Arc<BufferPool>) -> Result<Self, DatabaseError> {
        Self::create_with_options(
            path,
            pool,
            DEFAULT_WAL_CAPACITY,
            CheckpointConfig::default(),
            DEFAULT_NODE_ID,
        )
    }

    /// Create a new database with custom options.
    ///
    /// # Arguments
    /// * `path` - Path for the database file
    /// * `pool` - Shared buffer pool for page allocations
    /// * `wal_capacity` - Capacity of the write-ahead log in bytes
    /// * `checkpoint_config` - Configuration for automatic checkpointing
    /// * `node_id` - Unique identifier for this node (for distributed deployments)
    pub fn create_with_options(
        path: &Path,
        pool: Arc<BufferPool>,
        wal_capacity: u64,
        checkpoint_config: CheckpointConfig,
        node_id: u32,
    ) -> Result<Self, DatabaseError> {
        let mut file = DatabaseFile::create(path, pool)?;

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
            tombstone_list: TombstoneList::new(),
            gc_notify: Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Open an existing database at the given path.
    ///
    /// Runs crash recovery if needed to restore consistent state.
    /// Uses node ID 0 for single-node deployments.
    pub fn open(
        path: &Path,
        pool: Arc<BufferPool>,
    ) -> Result<(Self, Option<RecoveryResult>), DatabaseError> {
        Self::open_with_options(path, pool, CheckpointConfig::default(), DEFAULT_NODE_ID)
    }

    /// Open an existing database with custom options.
    ///
    /// # Arguments
    /// * `path` - Path to the existing database file
    /// * `pool` - Shared buffer pool for page allocations
    /// * `checkpoint_config` - Configuration for automatic checkpointing
    /// * `node_id` - Unique identifier for this node (for distributed deployments)
    pub fn open_with_options(
        path: &Path,
        pool: Arc<BufferPool>,
        checkpoint_config: CheckpointConfig,
        node_id: u32,
    ) -> Result<(Self, Option<RecoveryResult>), DatabaseError> {
        let mut file = DatabaseFile::open(path, pool)?;

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

        // Load tombstone list metadata from superblock
        let superblock = file.superblock();
        #[allow(clippy::cast_possible_truncation)] // Slot indices always fit in usize
        let tombstone_tail_slot = superblock.tombstone_tail_slot as usize;
        let mut tombstone_list = TombstoneList::from_persisted(
            superblock.tombstone_head_page,
            0, // head_slot is loaded separately from the head page
            superblock.tombstone_tail_page,
            tombstone_tail_slot,
            superblock.tombstone_count,
        );

        // Load the head slot from the head page (for partial consumption tracking)
        if superblock.tombstone_head_page != 0 {
            tombstone_list.load_head_slot(&mut file)?;
        }

        Ok((
            Self {
                file,
                checkpoint_state,
                clock,
                active_snapshots: ActiveSnapshots::default(),
                change_tx,
                tombstone_list,
                gc_notify: Arc::new(tokio::sync::Notify::new()),
            },
            recovery_result,
        ))
    }

    /// Open an existing database or create a new one if it doesn't exist.
    pub fn open_or_create(
        path: &Path,
        pool: Arc<BufferPool>,
    ) -> Result<(Self, Option<RecoveryResult>), DatabaseError> {
        if path.exists() {
            Self::open(path, pool)
        } else {
            let db = Self::create(path, pool)?;
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
    /// # Arguments
    ///
    /// * `connection_id` - The ID of the connection that is creating this transaction.
    ///   This is included in change notifications so subscribers can filter out
    ///   their own writes.
    ///
    /// # Panics
    /// Panics if transaction ID is 0 (indicates uninitialized database state).
    #[allow(clippy::disallowed_methods)] // Clone needed for broadcast sender
    pub fn begin(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<WalTransaction<'_>, DatabaseError> {
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
            &mut self.tombstone_list,
            Arc::clone(&self.gc_notify),
            txn_id,
            hlc,
            self.change_tx.clone(),
            connection_id,
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
    /// use server::storage::buffer_pool::BufferPool;
    /// use server::types::{EntityId, AttributeId};
    /// use std::path::Path;
    ///
    /// let pool = BufferPool::new(100);
    /// let path = Path::new("/tmp/my_database");
    /// let mut db = Database::create(path, pool).unwrap();
    ///
    /// let mut snapshot = db.begin_readonly();
    /// let entity = EntityId([1u8; 16]);
    /// let attr = AttributeId([2u8; 16]);
    /// let record = snapshot.get(&entity, &attr);
    /// let txn_id = snapshot.close(); // Returns the snapshot's txn_id
    /// db.release_snapshot(txn_id);   // Allow garbage collection
    /// ```
    #[cfg(unix)]
    pub fn begin_readonly(&self) -> Snapshot<'_> {
        // Snapshot sees all committed transactions (next_txn_id - 1)
        let txn_id = self.file.superblock().next_txn_id.saturating_sub(1);
        let hlc = self.clock.last();

        // Register the snapshot for garbage collection tracking
        self.active_snapshots.register(txn_id);

        Snapshot::new(&self.file, txn_id, hlc)
    }

    /// Release a snapshot and allow garbage collection.
    ///
    /// Call this after closing a snapshot to remove it from the active
    /// snapshot list. This allows deleted records that were visible to
    /// this snapshot to be garbage collected.
    pub fn release_snapshot(&self, txn_id: TxnId) {
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
    /// Subscribe to change notifications, filtering out this connection's own writes.
    ///
    /// Returns a filtered receiver that only yields notifications from other connections.
    /// Notifications originating from the specified `connection_id` are automatically skipped.
    pub fn subscribe_to_changes(&self, connection_id: ConnectionId) -> FilteredChangeReceiver {
        FilteredChangeReceiver::new(self.change_tx.subscribe(), connection_id)
    }

    /// Get a clone of the GC notify handle.
    ///
    /// This is used by the background GC task to wait for signals that
    /// there is work to be done.
    #[must_use]
    #[allow(clippy::disallowed_methods)] // Arc::clone is needed for async task
    pub fn gc_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.gc_notify)
    }

    /// Get statistics about pending garbage collection.
    #[must_use]
    pub fn gc_stats(&self) -> GcStats {
        GcStats {
            pending_tombstones: self.tombstone_list.count(),
            min_active_snapshot: self.active_snapshots.min_active(),
        }
    }

    /// Process a batch of eligible tombstones.
    ///
    /// This is called by the background GC task to incrementally process
    /// tombstones. It removes records from all indexes and updates the
    /// tombstone list.
    ///
    /// # Arguments
    /// * `batch_size` - Maximum number of tombstones to process
    ///
    /// # Returns
    /// Statistics about the GC operation.
    pub fn gc_tick(&mut self, batch_size: usize) -> Result<GcTickResult, DatabaseError> {
        let min_active = self.active_snapshots.min_active();

        // Pop eligible tombstones from the list
        let tombstones = self
            .tombstone_list
            .pop_batch(&mut self.file, min_active, batch_size)?;

        if tombstones.is_empty() {
            return Ok(GcTickResult {
                records_removed: 0,
                tombstones_remaining: self.tombstone_list.count(),
            });
        }

        let records_removed = tombstones.len() as u64;

        // Remove from all indexes
        self.remove_tombstoned_records(&tombstones)?;

        // Persist tombstone list state
        self.persist_tombstone_metadata()?;

        Ok(GcTickResult {
            records_removed,
            tombstones_remaining: self.tombstone_list.count(),
        })
    }

    /// Synchronously process all eligible tombstones.
    ///
    /// Unlike the incremental `gc_tick()`, this processes all eligible
    /// tombstones in one call. Use sparingly as it may block other operations.
    pub fn force_gc(&mut self) -> Result<GcStats, DatabaseError> {
        loop {
            let result = self.gc_tick(1000)?;
            if result.records_removed == 0 {
                break;
            }
        }

        Ok(GcStats {
            pending_tombstones: self.tombstone_list.count(),
            min_active_snapshot: self.active_snapshots.min_active(),
        })
    }

    /// Remove tombstoned records from all three indexes.
    fn remove_tombstoned_records(&mut self, tombstones: &[Tombstone]) -> Result<(), DatabaseError> {
        if tombstones.is_empty() {
            return Ok(());
        }

        // Remove from primary index
        let primary_root = {
            let root_page = self.file.superblock().primary_index_root;
            if root_page == 0 {
                0
            } else {
                let mut index = PrimaryIndex::new(&mut self.file, root_page)?;
                for t in tombstones {
                    index.remove(&t.entity_id, &t.attribute_id)?;
                }
                index.root_page()
            }
        };

        // Remove from attribute index
        let attribute_root = {
            let root_page = self.file.superblock().attribute_index_root;
            if root_page == 0 {
                0
            } else {
                let mut index = AttributeIndex::new(&mut self.file, root_page)?;
                for t in tombstones {
                    index.remove(&t.attribute_id, &t.entity_id)?;
                }
                index.root_page()
            }
        };

        // Remove from entity-attribute index
        let entity_attr_root = {
            let root_page = self.file.superblock().entity_attribute_index_root;
            if root_page == 0 {
                0
            } else {
                let mut index = EntityAttributeIndex::new(&mut self.file, root_page)?;
                for t in tombstones {
                    index.remove(&t.entity_id, &t.attribute_id)?;
                }
                index.root_page()
            }
        };

        // Update root pages if they changed
        if primary_root != 0 {
            self.file.superblock_mut().primary_index_root = primary_root;
        }
        if attribute_root != 0 {
            self.file.superblock_mut().attribute_index_root = attribute_root;
        }
        if entity_attr_root != 0 {
            self.file.superblock_mut().entity_attribute_index_root = entity_attr_root;
        }

        self.file.write_superblock()?;
        self.file.sync()?;

        Ok(())
    }

    /// Persist tombstone list metadata to the superblock.
    fn persist_tombstone_metadata(&mut self) -> Result<(), DatabaseError> {
        let sb = self.file.superblock_mut();
        sb.tombstone_head_page = self.tombstone_list.head_page_id();
        sb.tombstone_tail_page = self.tombstone_list.tail_page_id();
        sb.tombstone_tail_slot = self.tombstone_list.tail_slot() as u64;
        sb.tombstone_count = self.tombstone_list.count();

        self.file.write_superblock()?;

        // Also persist the head slot to the head page for recovery
        self.tombstone_list.persist_head_slot(&mut self.file)?;

        Ok(())
    }
}

/// A WAL-backed transaction.
///
/// Operations are buffered and written to WAL on commit, then applied to the index.
/// This ensures crash recovery can replay committed transactions.
pub struct WalTransaction<'a> {
    file: &'a mut DatabaseFile,
    checkpoint_state: &'a mut CheckpointState,
    clock: &'a mut Clock<SystemTimeSource>,
    tombstone_list: &'a mut TombstoneList,
    gc_notify: Arc<tokio::sync::Notify>,
    txn_id: TxnId,
    hlc: HlcTimestamp,
    /// Buffered operations to be written on commit
    operations: Vec<PendingTriple>,
    /// Whether this transaction has been finalized
    finalized: bool,
    /// Broadcast sender for change notifications.
    change_tx: broadcast::Sender<ChangeNotification>,
    /// The connection that created this transaction.
    connection_id: ConnectionId,
}

impl<'a> WalTransaction<'a> {
    #[allow(clippy::missing_const_for_fn)] // broadcast::Sender is not const
    #[allow(clippy::too_many_arguments)] // Transaction needs access to all database state
    fn new(
        file: &'a mut DatabaseFile,
        checkpoint_state: &'a mut CheckpointState,
        clock: &'a mut Clock<SystemTimeSource>,
        tombstone_list: &'a mut TombstoneList,
        gc_notify: Arc<tokio::sync::Notify>,
        txn_id: TxnId,
        hlc: HlcTimestamp,
        change_tx: broadcast::Sender<ChangeNotification>,
        connection_id: ConnectionId,
    ) -> Self {
        Self {
            file,
            checkpoint_state,
            clock,
            tombstone_list,
            gc_notify,
            txn_id,
            hlc,
            operations: Vec::new(),
            finalized: false,
            change_tx,
            connection_id,
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
        let record = TripleRecord::new(entity_id, attribute_id, self.txn_id, self.hlc, value);
        self.operations.push(PendingTriple::Insert(record));
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
        let record = TripleRecord::new(entity_id, attribute_id, self.txn_id, hlc, value);
        self.operations.push(PendingTriple::Insert(record));
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

        let record = TripleRecord::new(entity_id, attribute_id, self.txn_id, self.hlc, value);
        self.operations.push(PendingTriple::Update(record));
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
        let record = TripleRecord::new(entity_id, attribute_id, self.txn_id, hlc, value);
        self.operations.push(PendingTriple::Update(record));
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

        self.operations.push(PendingTriple::Delete {
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

        // Step 5b: Add tombstones for delete operations
        let has_deletes = self.add_tombstones_for_deletes(txn_id)?;

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

        // Step 9: Signal GC task if we added tombstones (non-blocking)
        if has_deletes {
            self.gc_notify.notify_one();
        }

        Ok(())
    }

    /// Write all operations to WAL.
    fn write_to_wal(&mut self, txn_id: TxnId, hlc: HlcTimestamp) -> Result<u64, DatabaseError> {
        let mut total_bytes = 0u64;

        let mut wal = self.file.wal()?;

        // BEGIN
        wal.append(txn_id, hlc, LogRecordPayload::Begin)?;

        // Write each operation - TripleRecord already constructed
        for op in &self.operations {
            match op {
                PendingTriple::Insert(record) => {
                    let payload = LogRecordPayload::insert(record);
                    total_bytes += payload.serialized_size() as u64;
                    wal.append(txn_id, record.created_hlc, payload)?;
                }
                PendingTriple::Update(record) => {
                    let payload = LogRecordPayload::update(record);
                    total_bytes += payload.serialized_size() as u64;
                    wal.append(txn_id, record.created_hlc, payload)?;
                }
                PendingTriple::Delete {
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
    fn apply_to_index(&mut self, txn_id: TxnId, _hlc: HlcTimestamp) -> Result<(), DatabaseError> {
        // Apply to primary index
        let primary_root = {
            let root_page = self.file.superblock().primary_index_root;
            let mut index = PrimaryIndex::new(self.file, root_page)?;

            for op in &self.operations {
                match op {
                    PendingTriple::Insert(record) | PendingTriple::Update(record) => {
                        index.insert(record)?;
                    }
                    PendingTriple::Delete {
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

    /// Add tombstones for delete operations in this transaction.
    ///
    /// Returns `true` if any tombstones were added.
    fn add_tombstones_for_deletes(&mut self, txn_id: TxnId) -> Result<bool, DatabaseError> {
        let mut has_deletes = false;

        for op in &self.operations {
            if let PendingTriple::Delete {
                entity_id,
                attribute_id,
            } = op
            {
                let tombstone = Tombstone::new(*entity_id, *attribute_id, txn_id);
                self.tombstone_list.append(tombstone);
                has_deletes = true;
            }
        }

        // Flush tombstones to disk if any were added
        if has_deletes {
            self.tombstone_list.flush(self.file)?;
            // Update superblock with tombstone metadata
            let sb = self.file.superblock_mut();
            sb.tombstone_head_page = self.tombstone_list.head_page_id();
            sb.tombstone_tail_page = self.tombstone_list.tail_page_id();
            sb.tombstone_tail_slot = self.tombstone_list.tail_slot() as u64;
            sb.tombstone_count = self.tombstone_list.count();
        }

        Ok(has_deletes)
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
                PendingTriple::Insert(record) => ChangeRecord {
                    change_type: ChangeType::Insert,
                    entity_id: record.entity_id,
                    attribute_id: record.attribute_id,
                    value: Some(record.value.clone_value()),
                    hlc: record.created_hlc,
                },
                PendingTriple::Update(record) => ChangeRecord {
                    change_type: ChangeType::Update,
                    entity_id: record.entity_id,
                    attribute_id: record.attribute_id,
                    value: Some(record.value.clone_value()),
                    hlc: record.created_hlc,
                },
                PendingTriple::Delete {
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
        let _ = self.change_tx.send(ChangeNotification {
            source_connection_id: self.connection_id,
            changes,
        });
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
/// Read-only snapshot for concurrent database access.
///
/// Uses position-independent reads to allow concurrent access from multiple threads.
#[cfg(unix)]
pub struct Snapshot<'a> {
    file: &'a DatabaseFile,
    /// The transaction ID this snapshot sees.
    txn_id: TxnId,
    /// HLC timestamp when the snapshot was created.
    hlc: HlcTimestamp,
}

#[cfg(unix)]
impl<'a> Snapshot<'a> {
    const fn new(file: &'a DatabaseFile, txn_id: TxnId, hlc: HlcTimestamp) -> Self {
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
        &self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<Option<TripleRecord>, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let index = PrimaryIndexReader::new(self.file, root_page);

        Ok(index.get_visible(entity_id, attribute_id, self.txn_id)?)
    }

    /// Scan all triples for an entity.
    ///
    /// Returns only triples visible at this snapshot.
    pub fn scan_entity(&self, entity_id: &EntityId) -> Result<Vec<TripleRecord>, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let index = PrimaryIndexReader::new(self.file, root_page);
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
    pub fn count(&self) -> Result<usize, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let index = PrimaryIndexReader::new(self.file, root_page);
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
    pub fn collect_all(&self) -> Result<Vec<TripleRecord>, DatabaseError> {
        let root_page = self.file.superblock().primary_index_root;
        let index = PrimaryIndexReader::new(self.file, root_page);
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
        &self,
        attribute_id: &AttributeId,
    ) -> Result<Vec<EntityId>, DatabaseError> {
        let root_page = self.file.superblock().attribute_index_root;
        let index = AttributeIndexReader::new(self.file, root_page);
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
        &self,
        entity_id: &EntityId,
    ) -> Result<Vec<AttributeId>, DatabaseError> {
        let root_page = self.file.superblock().entity_attribute_index_root;
        let index = EntityAttributeIndexReader::new(self.file, root_page);
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

/// Statistics about pending garbage collection.
#[derive(Debug)]
pub struct GcStats {
    /// Number of tombstones (deleted records) awaiting GC.
    pub pending_tombstones: u64,
    /// The minimum active snapshot transaction ID.
    /// Tombstones with `deleted_txn >= min_active_snapshot` cannot be collected.
    pub min_active_snapshot: Option<TxnId>,
}

/// Result of an incremental GC tick.
#[derive(Debug)]
pub struct GcTickResult {
    /// Number of records physically removed in this tick.
    pub records_removed: u64,
    /// Number of tombstones remaining after this tick.
    pub tombstones_remaining: u64,
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
    /// Tombstone list error.
    Tombstone(TombstoneError),
    /// Triple not found for update/delete.
    NotFound,
    /// Mutex/RwLock was poisoned.
    LockPoisoned,
    /// Connection not established (`ConnectRequest` not yet received).
    NotConnected,
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
            Self::Tombstone(e) => write!(f, "tombstone error: {e}"),
            Self::NotFound => write!(f, "triple not found"),
            Self::LockPoisoned => write!(f, "database lock poisoned"),
            Self::NotConnected => write!(f, "connection not established"),
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
            Self::Tombstone(e) => Some(e),
            Self::NotFound | Self::LockPoisoned | Self::NotConnected => None,
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

impl From<TombstoneError> for DatabaseError {
    fn from(e: TombstoneError) -> Self {
        Self::Tombstone(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_pool::BufferPool;
    use crate::types::{AttributeId, EntityId};
    use tempfile::tempdir;

    fn test_pool() -> Arc<BufferPool> {
        BufferPool::new(100)
    }

    fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_database_create_and_commit() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();

        // Create database
        {
            let mut db = Database::create(&path, Arc::clone(&pool)).expect("create db");
            let mut txn = db.begin(0).expect("begin txn");

            let entity_id = EntityId([1u8; 16]);
            let attribute_id = AttributeId([2u8; 16]);
            txn.insert(
                entity_id,
                attribute_id,
                TripleValue::String("hello".to_string()),
            );
            txn.commit().expect("commit");
        }

        // Reopen and verify (with recovery)
        {
            let (mut db, recovery) = Database::open(&path, Arc::clone(&pool)).expect("open db");

            // Recovery might have run
            if let Some(result) = recovery {
                // Verify recovery ran successfully
                assert!(result.transactions_replayed > 0);
            }

            let mut txn = db.begin(0).expect("begin txn");
            let record = txn
                .get(&EntityId([1u8; 16]), &AttributeId([2u8; 16]))
                .expect("get");
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
        let pool = test_pool();

        // First call creates
        {
            let (db, recovery) =
                Database::open_or_create(&path, Arc::clone(&pool)).expect("open_or_create");
            assert!(recovery.is_none()); // New database, no recovery
            drop(db);
        }

        // Second call opens
        {
            let (mut db, _) =
                Database::open_or_create(&path, Arc::clone(&pool)).expect("open_or_create again");
            let txn = db.begin(0).expect("begin");
            txn.abort();
        }
    }

    #[test]
    fn test_database_abort_no_persist() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();

        // Create and abort transaction
        {
            let mut db = Database::create(&path, Arc::clone(&pool)).expect("create db");
            let mut txn = db.begin(0).expect("begin txn");

            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([2u8; 16]),
                TripleValue::String("aborted".to_string()),
            );
            txn.abort(); // Don't commit
        }

        // Reopen and verify data is NOT there
        {
            let (mut db, _) = Database::open(&path, Arc::clone(&pool)).expect("open db");
            let mut txn = db.begin(0).expect("begin txn");

            let record = txn
                .get(&EntityId([1u8; 16]), &AttributeId([2u8; 16]))
                .expect("get");
            assert!(record.is_none());
            txn.abort();
        }
    }

    #[test]
    fn test_database_update_and_delete() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();

        let mut db = Database::create(&path, pool).expect("create db");

        // Insert
        {
            let mut txn = db.begin(0).expect("begin");
            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Number(1.0),
            );
            txn.commit().expect("commit");
        }

        // Update
        {
            let mut txn = db.begin(0).expect("begin");
            txn.update(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Number(2.0),
            )
            .expect("update");
            txn.commit().expect("commit");
        }

        // Verify update
        {
            let mut txn = db.begin(0).expect("begin");
            let record = txn
                .get(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("get");
            assert_eq!(record.unwrap().value, TripleValue::Number(2.0));
            txn.abort();
        }

        // Delete
        {
            let mut txn = db.begin(0).expect("begin");
            txn.delete(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("delete");
            txn.commit().expect("commit");
        }

        // Verify delete
        {
            let mut txn = db.begin(0).expect("begin");
            let record = txn
                .get(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("get");
            assert!(record.is_none());
            txn.abort();
        }
    }

    #[test]
    fn test_database_not_found_errors() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        let mut txn = db.begin(0).expect("begin");

        // Update non-existent
        let result = txn.update(
            EntityId([1u8; 16]),
            AttributeId([1u8; 16]),
            TripleValue::Null,
        );
        assert!(matches!(result, Err(DatabaseError::NotFound)));

        // Delete non-existent
        let result = txn.delete(&EntityId([1u8; 16]), &AttributeId([1u8; 16]));
        assert!(matches!(result, Err(DatabaseError::NotFound)));

        txn.abort();
    }

    #[test]
    fn test_database_multiple_transactions() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Multiple sequential transactions
        for i in 0..10u8 {
            let mut txn = db.begin(0).expect("begin");
            let mut entity = [0u8; 16];
            entity[0] = i;
            txn.insert(
                EntityId(entity),
                AttributeId([1u8; 16]),
                TripleValue::Number(f64::from(i)),
            );
            txn.commit().expect("commit");
        }

        // Verify all data
        let mut txn = db.begin(0).expect("begin");
        for i in 0..10u8 {
            let mut entity = [0u8; 16];
            entity[0] = i;
            let record = txn
                .get(&EntityId(entity), &AttributeId([1u8; 16]))
                .expect("get");
            assert!(record.is_some());
            assert_eq!(record.unwrap().value, TripleValue::Number(f64::from(i)));
        }
        txn.abort();
    }

    #[test]
    fn test_database_checkpoint() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert some data
        {
            let mut txn = db.begin(0).expect("begin");
            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Boolean(true),
            );
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
        let pool = test_pool();

        // Create database and insert without clean close
        {
            let mut db = Database::create(&path, Arc::clone(&pool)).expect("create db");
            let mut txn = db.begin(0).expect("begin");
            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::String("recovered".to_string()),
            );
            txn.commit().expect("commit");
            // Don't call close() - simulates crash after commit
        }

        // Reopen - recovery should find the committed data
        {
            let (mut db, recovery) = Database::open(&path, Arc::clone(&pool)).expect("open db");

            // Might have run recovery
            if let Some(result) = recovery {
                // The transaction was committed, so it should be replayed
                assert!(result.transactions_replayed <= 1);
            }

            // Verify data is there
            let mut txn = db.begin(0).expect("begin");
            let record = txn
                .get(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("get");
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Empty transaction should commit successfully
        let txn = db.begin(0).expect("begin");
        txn.commit().expect("commit empty");

        db.close().expect("close");
    }

    #[test]
    fn test_snapshot_basic_read() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert data
        {
            let mut txn = db.begin(0).expect("begin");
            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Number(42.0),
            );
            txn.commit().expect("commit");
        }

        // Read via snapshot
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let record = snapshot
                .get(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("get");
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert initial data (txn_id = 1)
        {
            let mut txn = db.begin(0).expect("begin");
            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Number(1.0),
            );
            txn.commit().expect("commit");
        }

        // Create a snapshot that sees txn_id = 1
        let mut snapshot = db.begin_readonly();
        let snapshot_txn = snapshot.snapshot_txn();
        assert_eq!(snapshot_txn, 1);

        // Verify snapshot sees the initial data
        let record = snapshot
            .get(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
            .expect("get");
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, TripleValue::Number(1.0));

        // Close snapshot so we can do a write
        let txn_id = snapshot.close();
        db.release_snapshot(txn_id);

        // Update the data (txn_id = 2)
        {
            let mut txn = db.begin(0).expect("begin");
            txn.update(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Number(2.0),
            )
            .expect("update");
            txn.commit().expect("commit");
        }

        // New snapshot sees updated data
        let txn_id = {
            let mut snapshot2 = db.begin_readonly();
            assert_eq!(snapshot2.snapshot_txn(), 2);
            let record = snapshot2
                .get(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("get");
            assert!(record.is_some());
            assert_eq!(record.unwrap().value, TripleValue::Number(2.0));
            snapshot2.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_snapshot_sees_deleted_records() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert data (txn_id = 1)
        {
            let mut txn = db.begin(0).expect("begin");
            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::String("hello".to_string()),
            );
            txn.commit().expect("commit");
        }

        // Delete the data (txn_id = 2)
        {
            let mut txn = db.begin(0).expect("begin");
            txn.delete(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("delete");
            txn.commit().expect("commit");
        }

        // Current snapshot (at txn=2) should not see the deleted record
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            assert_eq!(snapshot.snapshot_txn(), 2);
            let record = snapshot
                .get(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("get");
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        let entity = EntityId([1u8; 16]);

        // Insert multiple attributes
        {
            let mut txn = db.begin(0).expect("begin");
            for i in 0..5u8 {
                let mut attr = [0u8; 16];
                attr[0] = i;
                txn.insert(entity, AttributeId(attr), TripleValue::Number(f64::from(i)));
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert 10 records
        {
            let mut txn = db.begin(0).expect("begin");
            for i in 0..10u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.insert(
                    EntityId(entity),
                    AttributeId([1u8; 16]),
                    TripleValue::Number(f64::from(i)),
                );
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
            let mut txn = db.begin(0).expect("begin");
            for i in 0..5u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.delete(&EntityId(entity), &AttributeId([1u8; 16]))
                    .expect("delete");
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert data
        {
            let mut txn = db.begin(0).expect("begin");
            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Boolean(true),
            );
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert records
        {
            let mut txn = db.begin(0).expect("begin");
            for i in 0..5u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.insert(
                    EntityId(entity),
                    AttributeId([1u8; 16]),
                    TripleValue::Number(f64::from(i)),
                );
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert records
        {
            let mut txn = db.begin(0).expect("begin");
            for i in 0..10u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.insert(
                    EntityId(entity),
                    AttributeId([1u8; 16]),
                    TripleValue::Number(f64::from(i)),
                );
            }
            txn.commit().expect("commit");
        }

        // Delete half the records
        {
            let mut txn = db.begin(0).expect("begin");
            for i in 0..5u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.delete(&EntityId(entity), &AttributeId([1u8; 16]))
                    .expect("delete");
            }
            txn.commit().expect("commit");
        }

        // Run GC - should remove the deleted records
        let result = db.force_gc().expect("gc");
        // All tombstones should be processed
        assert_eq!(result.pending_tombstones, 0);
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert a record (txn_id = 1)
        {
            let mut txn = db.begin(0).expect("begin");
            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Number(1.0),
            );
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
            let mut txn = db.begin(0).expect("begin");
            txn.delete(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("delete");
            txn.commit().expect("commit");
        }

        // Run GC - should NOT remove the record because snapshot at txn=1 can still see it
        let result = db.force_gc().expect("gc");
        // Tombstone should still be pending (blocked by active snapshot)
        assert_eq!(result.pending_tombstones, 1);
        assert_eq!(result.min_active_snapshot, Some(1));

        // Release the snapshot
        db.release_snapshot(snapshot_txn);

        // Run GC again - now it should be removed
        let result = db.force_gc().expect("gc");
        assert_eq!(result.pending_tombstones, 0);
        assert!(result.min_active_snapshot.is_none());
    }

    #[test]
    fn test_gc_no_deleted_records() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert records (no deletes)
        {
            let mut txn = db.begin(0).expect("begin");
            for i in 0..5u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.insert(
                    EntityId(entity),
                    AttributeId([1u8; 16]),
                    TripleValue::Number(f64::from(i)),
                );
            }
            txn.commit().expect("commit");
        }

        // Run GC - nothing to collect (no tombstones were created)
        let result = db.force_gc().expect("gc");
        assert_eq!(result.pending_tombstones, 0);
        assert!(result.min_active_snapshot.is_none());
    }

    #[test]
    fn test_gc_empty_database() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Run GC on empty database
        let result = db.force_gc().expect("gc");
        assert_eq!(result.pending_tombstones, 0);
        assert!(result.min_active_snapshot.is_none());
    }

    #[test]
    fn test_gc_multiple_snapshots() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert records (txn_id = 1)
        {
            let mut txn = db.begin(0).expect("begin");
            txn.insert(
                EntityId([1u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Number(1.0),
            );
            txn.insert(
                EntityId([2u8; 16]),
                AttributeId([1u8; 16]),
                TripleValue::Number(2.0),
            );
            txn.commit().expect("commit");
        }

        // Snapshot 1 at txn_id = 1
        let snapshot1_txn = {
            let snapshot = db.begin_readonly();
            snapshot.close()
        };

        // Delete first record (txn_id = 2)
        {
            let mut txn = db.begin(0).expect("begin");
            txn.delete(&EntityId([1u8; 16]), &AttributeId([1u8; 16]))
                .expect("delete");
            txn.commit().expect("commit");
        }

        // Snapshot 2 at txn_id = 2
        let snapshot2_txn = {
            let snapshot = db.begin_readonly();
            snapshot.close()
        };

        // Delete second record (txn_id = 3)
        {
            let mut txn = db.begin(0).expect("begin");
            txn.delete(&EntityId([2u8; 16]), &AttributeId([1u8; 16]))
                .expect("delete");
            txn.commit().expect("commit");
        }

        // GC: min_active = 1, can't remove either (both deleted after txn=1)
        let result = db.force_gc().expect("gc");
        // 2 tombstones should still be pending
        assert_eq!(result.pending_tombstones, 2);
        assert_eq!(result.min_active_snapshot, Some(1));

        // Release snapshot 1
        db.release_snapshot(snapshot1_txn);

        // GC: min_active = 2, can remove first record (deleted at txn=2, which is not < 2)
        // Wait - deleted_txn=2 is NOT < min_active=2, so it shouldn't be GC'd
        let result = db.force_gc().expect("gc");
        assert_eq!(result.pending_tombstones, 2);
        assert_eq!(result.min_active_snapshot, Some(2));

        // Release snapshot 2
        db.release_snapshot(snapshot2_txn);

        // GC: no active snapshots, both records should be removed
        let result = db.force_gc().expect("gc");
        assert_eq!(result.pending_tombstones, 0);
        assert!(result.min_active_snapshot.is_none());
    }

    #[test]
    fn test_is_gc_eligible() {
        use crate::types::HlcTimestamp;

        // Not deleted - not eligible
        let record = TripleRecord::new(
            EntityId([1u8; 16]),
            AttributeId([1u8; 16]),
            10,
            HlcTimestamp::new(1000, 0),
            TripleValue::Null,
        );
        assert!(!record.is_gc_eligible(None));
        assert!(!record.is_gc_eligible(Some(5)));
        assert!(!record.is_gc_eligible(Some(15)));

        // Deleted at txn=50
        let mut record = TripleRecord::new(
            EntityId([1u8; 16]),
            AttributeId([1u8; 16]),
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        let attr1 = AttributeId([1u8; 16]);
        let attr2 = AttributeId([2u8; 16]);

        // Insert entities with various attributes
        {
            let mut txn = db.begin(0).expect("begin");
            // Entity 1 has attr1 and attr2
            txn.insert(EntityId([1u8; 16]), attr1, TripleValue::Number(1.0));
            txn.insert(EntityId([1u8; 16]), attr2, TripleValue::Number(2.0));
            // Entity 2 has only attr1
            txn.insert(EntityId([2u8; 16]), attr1, TripleValue::Number(3.0));
            // Entity 3 has only attr2
            txn.insert(EntityId([3u8; 16]), attr2, TripleValue::Number(4.0));
            txn.commit().expect("commit");
        }

        // Query entities with attr1 via transaction
        {
            let mut txn = db.begin(0).expect("begin");
            let entities = txn.get_entities_with_attribute(&attr1).expect("query");
            assert_eq!(entities.len(), 2);
            assert!(entities.contains(&EntityId([1u8; 16])));
            assert!(entities.contains(&EntityId([2u8; 16])));
            txn.abort();
        }

        // Query entities with attr2 via snapshot
        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let entities = snapshot.get_entities_with_attribute(&attr2).expect("query");
            assert_eq!(entities.len(), 2);
            assert!(entities.contains(&EntityId([1u8; 16])));
            assert!(entities.contains(&EntityId([3u8; 16])));
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_secondary_index_attributes_for_entity() {
        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        let entity1 = EntityId([1u8; 16]);
        let entity2 = EntityId([2u8; 16]);
        let attr1 = AttributeId([10u8; 16]);
        let attr2 = AttributeId([20u8; 16]);
        let attr3 = AttributeId([30u8; 16]);

        // Insert triples
        {
            let mut txn = db.begin(0).expect("begin");
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
            let mut txn = db.begin(0).expect("begin");
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        let entity = EntityId([1u8; 16]);
        let attr1 = AttributeId([10u8; 16]);
        let attr2 = AttributeId([20u8; 16]);

        // Insert first attribute (txn_id = 1)
        {
            let mut txn = db.begin(0).expect("begin");
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
            let mut txn = db.begin(0).expect("begin");
            txn.insert(entity, attr2, TripleValue::Number(2.0));
            txn.commit().expect("commit");
        }

        // Delete first attribute (txn_id = 3)
        {
            let mut txn = db.begin(0).expect("begin");
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
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        let entity = EntityId([1u8; 16]);
        let attr = AttributeId([10u8; 16]);

        // Insert
        {
            let mut txn = db.begin(0).expect("begin");
            txn.insert(entity, attr, TripleValue::Number(1.0));
            txn.commit().expect("commit");
        }

        // Delete
        {
            let mut txn = db.begin(0).expect("begin");
            txn.delete(&entity, &attr).expect("delete");
            txn.commit().expect("commit");
        }

        // GC should clean up from all indexes
        let result = db.force_gc().expect("gc");
        assert_eq!(result.pending_tombstones, 0);

        // Verify the record is gone from secondary indexes too
        {
            let mut txn = db.begin(0).expect("begin");
            let entities = txn.get_entities_with_attribute(&attr).expect("query");
            assert!(entities.is_empty());
            let attrs = txn.get_attributes_for_entity(&entity).expect("query");
            assert!(attrs.is_empty());
            txn.abort();
        }
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::RwLock;
        use std::thread;

        let (_dir, path) = create_test_db();
        let pool = test_pool();
        let mut db = Database::create(&path, pool).expect("create db");

        // Insert test data
        {
            let mut txn = db.begin(0).expect("begin");
            for i in 0..100u8 {
                let mut entity = [0u8; 16];
                entity[0] = i;
                txn.insert(
                    EntityId(entity),
                    AttributeId([1u8; 16]),
                    TripleValue::Number(f64::from(i)),
                );
            }
            txn.commit().expect("commit");
        }

        // Wrap in RwLock to test concurrent reads
        let db = Arc::new(RwLock::new(db));

        // Spawn multiple threads that each acquire a read lock and read data
        let handles: Vec<_> = (0..10)
            .map(|thread_id| {
                let db_clone = Arc::clone(&db);
                thread::spawn(move || {
                    // Each thread acquires a read lock (not write lock)
                    let Ok(db_guard) = db_clone.read() else {
                        panic!("Failed to acquire read lock");
                    };

                    // Create snapshot and read data
                    let snapshot = db_guard.begin_readonly();

                    // Verify can read all data
                    for i in 0..100u8 {
                        let mut entity = [0u8; 16];
                        entity[0] = i;
                        let record = snapshot
                            .get(&EntityId(entity), &AttributeId([1u8; 16]))
                            .expect("get");
                        assert!(
                            record.is_some(),
                            "thread {} missing record {}",
                            thread_id,
                            i
                        );
                        assert_eq!(record.unwrap().value, TripleValue::Number(f64::from(i)));
                    }

                    let count = snapshot.count().expect("count");
                    assert_eq!(count, 100);

                    let txn_id = snapshot.close();
                    db_guard.release_snapshot(txn_id);

                    thread_id
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            let thread_id = handle.join().expect("thread panicked");
            assert!(thread_id < 10);
        }

        // Verify database still works after concurrent reads
        let Ok(db_guard) = db.read() else {
            panic!("Failed to acquire read lock");
        };
        assert_eq!(db_guard.active_snapshot_count(), 0);
    }
}
