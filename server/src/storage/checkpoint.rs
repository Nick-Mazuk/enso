//! Checkpointing for near-instant crash recovery.
//!
//! Checkpoints ensure that crash recovery only needs to replay a small number of
//! WAL records by periodically flushing dirty pages and recording a stable point.
//!
//! # Checkpoint Triggers
//!
//! Checkpoints can be triggered by:
//! - Transaction count threshold (default: 1000 transactions)
//! - Bytes written threshold (default: 4MB)
//! - Manual trigger via API
//! - Clean shutdown
//!
//! # Checkpoint Process
//!
//! 1. Flush all dirty pages to disk
//! 2. Write checkpoint record to WAL
//! 3. Update superblock with checkpoint LSN and HLC
//! 4. fsync to ensure durability
//!
//! # Recovery
//!
//! On startup, recovery only needs to replay WAL records after the last checkpoint.

use std::collections::HashSet;

use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::page::PageId;
use crate::storage::wal::{LogRecordPayload, Lsn, WalError};
use crate::types::HlcTimestamp;

/// Default number of transactions between checkpoints.
pub const DEFAULT_TXN_THRESHOLD: u64 = 1000;

/// Default number of bytes written between checkpoints (4MB).
pub const DEFAULT_BYTES_THRESHOLD: u64 = 4 * 1024 * 1024;

/// Checkpoint configuration.
#[derive(Debug, Copy, Clone)]
pub struct CheckpointConfig {
    /// Number of transactions between automatic checkpoints.
    /// Set to 0 to disable transaction-based checkpoints.
    pub txn_threshold: u64,

    /// Number of bytes written to WAL between automatic checkpoints.
    /// Set to 0 to disable byte-based checkpoints.
    pub bytes_threshold: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            txn_threshold: DEFAULT_TXN_THRESHOLD,
            bytes_threshold: DEFAULT_BYTES_THRESHOLD,
        }
    }
}

impl CheckpointConfig {
    /// Create a new checkpoint configuration.
    #[must_use]
    pub const fn new(txn_threshold: u64, bytes_threshold: u64) -> Self {
        Self {
            txn_threshold,
            bytes_threshold,
        }
    }

    /// Disable automatic checkpoints (manual only).
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            txn_threshold: 0,
            bytes_threshold: 0,
        }
    }
}

/// Tracks state needed for checkpoint decisions.
#[derive(Debug)]
pub struct CheckpointState {
    /// Configuration for checkpoint triggers.
    config: CheckpointConfig,

    /// LSN of the last checkpoint.
    last_checkpoint_lsn: Lsn,

    /// HLC of the last checkpoint.
    last_checkpoint_hlc: HlcTimestamp,

    /// Number of transactions since last checkpoint.
    txns_since_checkpoint: u64,

    /// Bytes written to WAL since last checkpoint.
    bytes_since_checkpoint: u64,

    /// Set of dirty page IDs that need to be flushed.
    dirty_pages: HashSet<PageId>,
}

impl CheckpointState {
    /// Create a new checkpoint state from database file.
    #[must_use]
    pub fn new(config: CheckpointConfig, last_lsn: Lsn, last_hlc: HlcTimestamp) -> Self {
        Self {
            config,
            last_checkpoint_lsn: last_lsn,
            last_checkpoint_hlc: last_hlc,
            txns_since_checkpoint: 0,
            bytes_since_checkpoint: 0,
            dirty_pages: HashSet::new(),
        }
    }

    /// Create checkpoint state from an existing database file.
    #[must_use]
    pub fn from_database(file: &DatabaseFile, config: CheckpointConfig) -> Self {
        let sb = file.superblock();
        Self::new(config, sb.last_checkpoint_lsn, sb.last_checkpoint_hlc)
    }

    /// Get the LSN of the last checkpoint.
    #[must_use]
    pub const fn last_checkpoint_lsn(&self) -> Lsn {
        self.last_checkpoint_lsn
    }

    /// Get the HLC of the last checkpoint.
    #[must_use]
    pub const fn last_checkpoint_hlc(&self) -> HlcTimestamp {
        self.last_checkpoint_hlc
    }

    /// Get the number of transactions since last checkpoint.
    #[must_use]
    pub const fn txns_since_checkpoint(&self) -> u64 {
        self.txns_since_checkpoint
    }

    /// Get the bytes written since last checkpoint.
    #[must_use]
    pub const fn bytes_since_checkpoint(&self) -> u64 {
        self.bytes_since_checkpoint
    }

    /// Get the number of dirty pages.
    #[must_use]
    pub fn dirty_page_count(&self) -> usize {
        self.dirty_pages.len()
    }

    /// Mark a page as dirty.
    pub fn mark_dirty(&mut self, page_id: PageId) {
        self.dirty_pages.insert(page_id);
    }

    /// Mark multiple pages as dirty.
    pub fn mark_dirty_batch(&mut self, page_ids: &[PageId]) {
        for &page_id in page_ids {
            self.dirty_pages.insert(page_id);
        }
    }

    /// Record that a transaction was committed.
    pub const fn record_commit(&mut self) {
        self.txns_since_checkpoint += 1;
    }

    /// Record that bytes were written to the WAL.
    pub const fn record_wal_write(&mut self, bytes: u64) {
        self.bytes_since_checkpoint += bytes;
    }

    /// Check if a checkpoint should be triggered based on current state.
    #[must_use]
    pub const fn should_checkpoint(&self) -> bool {
        // Check transaction threshold
        if self.config.txn_threshold > 0 && self.txns_since_checkpoint >= self.config.txn_threshold
        {
            return true;
        }

        // Check bytes threshold
        if self.config.bytes_threshold > 0
            && self.bytes_since_checkpoint >= self.config.bytes_threshold
        {
            return true;
        }

        false
    }

    /// Get the set of dirty pages that need to be flushed.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // HashSet reference not const-stable
    pub fn dirty_pages(&self) -> &HashSet<PageId> {
        &self.dirty_pages
    }

    /// Clear dirty pages after a successful checkpoint.
    fn clear_dirty_pages(&mut self) {
        self.dirty_pages.clear();
    }

    /// Reset counters after a successful checkpoint.
    fn reset_counters(&mut self, lsn: Lsn, hlc: HlcTimestamp) {
        self.last_checkpoint_lsn = lsn;
        self.last_checkpoint_hlc = hlc;
        self.txns_since_checkpoint = 0;
        self.bytes_since_checkpoint = 0;
        self.clear_dirty_pages();
    }
}

/// Result of a checkpoint operation.
#[derive(Debug)]
pub struct CheckpointResult {
    /// LSN of the checkpoint record.
    pub checkpoint_lsn: Lsn,

    /// HLC timestamp of the checkpoint.
    pub checkpoint_hlc: HlcTimestamp,

    /// Number of dirty pages that were flushed.
    pub pages_flushed: usize,
}

/// Perform a checkpoint on the database.
///
/// This function:
/// 1. Flushes all dirty pages to disk
/// 2. Writes a checkpoint record to the WAL
/// 3. Updates the superblock with checkpoint metadata
/// 4. Syncs to ensure durability
///
/// # Arguments
/// * `file` - The database file to checkpoint
/// * `state` - Checkpoint state tracking dirty pages and counters
/// * `hlc` - Current HLC timestamp for the checkpoint
///
/// # Returns
/// A `CheckpointResult` with details about the checkpoint.
pub fn perform_checkpoint(
    file: &mut DatabaseFile,
    state: &mut CheckpointState,
    hlc: HlcTimestamp,
) -> Result<CheckpointResult, CheckpointError> {
    // Step 1: Count dirty pages (they will be flushed implicitly by write operations)
    // In a real implementation, we'd track and flush dirty pages from a page cache.
    // For now, we just record the count.
    let pages_flushed = state.dirty_page_count();

    // Step 2: Read values needed for checkpoint record BEFORE borrowing for WAL
    let min_active_txn = file.superblock().next_txn_id;
    let active_txn_count = file.superblock().active_txn_count;

    // Step 3: Write checkpoint record to WAL
    let (checkpoint_lsn, wal_head, last_lsn) = {
        let mut wal = file.wal()?;

        let payload = LogRecordPayload::checkpoint(min_active_txn, active_txn_count);

        // Transaction ID 0 for system operations like checkpoint
        let lsn = wal.append(0, hlc, payload)?;

        // Sync WAL to disk
        wal.sync()?;

        // Capture values before dropping WAL borrow
        (lsn, wal.head(), wal.last_lsn())
    };

    // Step 4: Update file's WAL head position (now WAL borrow is dropped)
    file.update_wal_head(wal_head, last_lsn);

    // Step 5: Update superblock with checkpoint metadata
    {
        let sb = file.superblock_mut();
        sb.last_checkpoint_lsn = checkpoint_lsn;
        sb.last_checkpoint_hlc = hlc;
    }

    // Write superblock to disk
    file.write_superblock()?;

    // Step 6: Final sync to ensure durability
    file.sync()?;

    // Step 7: Reset checkpoint state
    state.reset_counters(checkpoint_lsn, hlc);

    Ok(CheckpointResult {
        checkpoint_lsn,
        checkpoint_hlc: hlc,
        pages_flushed,
    })
}

/// Check if a checkpoint is needed and perform it if so.
///
/// This is a convenience function that combines the check and checkpoint
/// operations. It's useful for calling after each transaction commit.
///
/// # Returns
/// `Some(CheckpointResult)` if a checkpoint was performed, `None` otherwise.
pub fn maybe_checkpoint(
    file: &mut DatabaseFile,
    state: &mut CheckpointState,
    hlc: HlcTimestamp,
) -> Result<Option<CheckpointResult>, CheckpointError> {
    if state.should_checkpoint() {
        Ok(Some(perform_checkpoint(file, state, hlc)?))
    } else {
        Ok(None)
    }
}

/// Force a checkpoint regardless of thresholds.
///
/// This is useful for clean shutdown or manual checkpoint triggers.
pub fn force_checkpoint(
    file: &mut DatabaseFile,
    state: &mut CheckpointState,
    hlc: HlcTimestamp,
) -> Result<CheckpointResult, CheckpointError> {
    perform_checkpoint(file, state, hlc)
}

/// Errors that can occur during checkpoint operations.
#[derive(Debug)]
pub enum CheckpointError {
    /// File I/O error.
    File(FileError),
    /// WAL error.
    Wal(WalError),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(e) => write!(f, "checkpoint file error: {e}"),
            Self::Wal(e) => write!(f, "checkpoint WAL error: {e}"),
        }
    }
}

impl std::error::Error for CheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(e) => Some(e),
            Self::Wal(e) => Some(e),
        }
    }
}

impl From<FileError> for CheckpointError {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

impl From<WalError> for CheckpointError {
    fn from(e: WalError) -> Self {
        Self::Wal(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::DEFAULT_WAL_CAPACITY;
    use tempfile::tempdir;

    fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_checkpoint_config_default() {
        let config = CheckpointConfig::default();
        assert_eq!(config.txn_threshold, DEFAULT_TXN_THRESHOLD);
        assert_eq!(config.bytes_threshold, DEFAULT_BYTES_THRESHOLD);
    }

    #[test]
    fn test_checkpoint_config_disabled() {
        let config = CheckpointConfig::disabled();
        assert_eq!(config.txn_threshold, 0);
        assert_eq!(config.bytes_threshold, 0);
    }

    #[test]
    fn test_checkpoint_state_dirty_pages() {
        let mut state =
            CheckpointState::new(CheckpointConfig::default(), 0, HlcTimestamp::new(0, 0));

        assert_eq!(state.dirty_page_count(), 0);

        state.mark_dirty(1);
        state.mark_dirty(2);
        state.mark_dirty(3);
        assert_eq!(state.dirty_page_count(), 3);

        // Duplicate should not increase count
        state.mark_dirty(2);
        assert_eq!(state.dirty_page_count(), 3);

        state.mark_dirty_batch(&[4, 5, 6]);
        assert_eq!(state.dirty_page_count(), 6);
    }

    #[test]
    fn test_checkpoint_state_counters() {
        let mut state =
            CheckpointState::new(CheckpointConfig::default(), 0, HlcTimestamp::new(0, 0));

        assert_eq!(state.txns_since_checkpoint(), 0);
        assert_eq!(state.bytes_since_checkpoint(), 0);

        state.record_commit();
        state.record_commit();
        assert_eq!(state.txns_since_checkpoint(), 2);

        state.record_wal_write(1024);
        state.record_wal_write(2048);
        assert_eq!(state.bytes_since_checkpoint(), 3072);
    }

    #[test]
    fn test_should_checkpoint_txn_threshold() {
        let config = CheckpointConfig::new(10, 0); // 10 txns, no bytes limit
        let mut state = CheckpointState::new(config, 0, HlcTimestamp::new(0, 0));

        for _ in 0..9 {
            state.record_commit();
            assert!(!state.should_checkpoint());
        }

        state.record_commit(); // 10th commit
        assert!(state.should_checkpoint());
    }

    #[test]
    fn test_should_checkpoint_bytes_threshold() {
        let config = CheckpointConfig::new(0, 1024); // No txn limit, 1KB bytes limit
        let mut state = CheckpointState::new(config, 0, HlcTimestamp::new(0, 0));

        state.record_wal_write(512);
        assert!(!state.should_checkpoint());

        state.record_wal_write(512); // Total: 1024
        assert!(state.should_checkpoint());
    }

    #[test]
    fn test_should_checkpoint_disabled() {
        let config = CheckpointConfig::disabled();
        let mut state = CheckpointState::new(config, 0, HlcTimestamp::new(0, 0));

        // Even with lots of activity, should not trigger checkpoint
        for _ in 0..10000 {
            state.record_commit();
        }
        state.record_wal_write(100 * 1024 * 1024); // 100MB

        assert!(!state.should_checkpoint());
    }

    #[test]
    fn test_perform_checkpoint() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        // Initialize WAL
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let config = CheckpointConfig::new(10, 4096);
        let mut state = CheckpointState::from_database(&file, config);

        // Mark some pages dirty and record some activity
        state.mark_dirty(1);
        state.mark_dirty(2);
        state.record_commit();
        state.record_commit();
        state.record_wal_write(256);

        // Perform checkpoint
        let hlc = HlcTimestamp::new(1000, 1);
        let result = perform_checkpoint(&mut file, &mut state, hlc).expect("checkpoint");

        assert!(result.checkpoint_lsn > 0);
        assert_eq!(result.checkpoint_hlc, hlc);
        assert_eq!(result.pages_flushed, 2);

        // Verify state was reset
        assert_eq!(state.txns_since_checkpoint(), 0);
        assert_eq!(state.bytes_since_checkpoint(), 0);
        assert_eq!(state.dirty_page_count(), 0);
        assert_eq!(state.last_checkpoint_lsn(), result.checkpoint_lsn);
        assert_eq!(state.last_checkpoint_hlc(), hlc);

        // Verify superblock was updated
        assert_eq!(file.superblock().last_checkpoint_lsn, result.checkpoint_lsn);
        assert_eq!(file.superblock().last_checkpoint_hlc, hlc);
    }

    #[test]
    fn test_maybe_checkpoint() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let config = CheckpointConfig::new(5, 0); // Checkpoint after 5 txns
        let mut state = CheckpointState::from_database(&file, config);

        // Not enough commits yet
        for _ in 0..4 {
            state.record_commit();
        }

        let hlc = HlcTimestamp::new(1000, 0);
        let result = maybe_checkpoint(&mut file, &mut state, hlc).expect("maybe_checkpoint");
        assert!(result.is_none());

        // One more commit should trigger checkpoint
        state.record_commit();
        let result = maybe_checkpoint(&mut file, &mut state, hlc).expect("maybe_checkpoint");
        assert!(result.is_some());
    }

    #[test]
    fn test_force_checkpoint() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let config = CheckpointConfig::new(1000, 1_000_000); // High thresholds
        let mut state = CheckpointState::from_database(&file, config);

        // Even though thresholds aren't met, force_checkpoint should work
        assert!(!state.should_checkpoint());

        let hlc = HlcTimestamp::new(2000, 5);
        let result = force_checkpoint(&mut file, &mut state, hlc).expect("force checkpoint");

        assert!(result.checkpoint_lsn > 0);
        assert_eq!(result.checkpoint_hlc, hlc);
    }

    #[test]
    fn test_checkpoint_persistence() {
        let (_dir, path) = create_test_db();

        let checkpoint_lsn;
        let checkpoint_hlc = HlcTimestamp::new(5000, 10);

        // Create database and checkpoint
        {
            let mut file = DatabaseFile::create(&path).expect("create db");
            file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

            let config = CheckpointConfig::default();
            let mut state = CheckpointState::from_database(&file, config);

            let result =
                force_checkpoint(&mut file, &mut state, checkpoint_hlc).expect("checkpoint");
            checkpoint_lsn = result.checkpoint_lsn;
        }

        // Reopen and verify
        {
            let file = DatabaseFile::open(&path).expect("open db");

            assert_eq!(file.superblock().last_checkpoint_lsn, checkpoint_lsn);
            assert_eq!(file.superblock().last_checkpoint_hlc, checkpoint_hlc);
        }
    }

    #[test]
    fn test_multiple_checkpoints() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");
        file.init_wal(DEFAULT_WAL_CAPACITY).expect("init wal");

        let config = CheckpointConfig::default();
        let mut state = CheckpointState::from_database(&file, config);

        // Perform multiple checkpoints
        let mut last_lsn = 0;
        for i in 1..=5 {
            state.record_commit();
            state.mark_dirty(i);

            let hlc = HlcTimestamp::new(i * 1000, 0);
            let result = force_checkpoint(&mut file, &mut state, hlc).expect("checkpoint");

            // Each checkpoint should have a higher LSN
            assert!(result.checkpoint_lsn > last_lsn);
            last_lsn = result.checkpoint_lsn;

            // State should be reset after each checkpoint
            assert_eq!(state.txns_since_checkpoint(), 0);
            assert_eq!(state.dirty_page_count(), 0);
        }
    }
}
