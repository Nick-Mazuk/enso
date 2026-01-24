//! Simulated in-memory storage for deterministic testing.
//!
//! This module provides an in-memory implementation of the `Storage` trait
//! with support for fault injection at various levels:
//! - Page-level read/write errors
//! - Byte-level corruption (bit flips)
//! - Partial writes
//! - Sync failures

// Simulation code legitimately needs cloning for test data
#![allow(clippy::disallowed_methods)]

use std::collections::HashMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::storage::io::{Storage, StorageError};
use crate::storage::wal::{LogRecord, LogRecordPayload, Lsn};
use crate::storage::{HlcTimestamp, PAGE_SIZE, Page, PageId, Superblock};

/// Configuration for fault injection.
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Probability of a read error (0.0 - 1.0).
    pub read_error_rate: f64,
    /// Probability of a write error (0.0 - 1.0).
    pub write_error_rate: f64,
    /// Probability of a sync error (0.0 - 1.0).
    pub sync_error_rate: f64,
    /// Probability of page corruption on read (0.0 - 1.0).
    pub corruption_rate: f64,
    /// Probability of partial write (0.0 - 1.0).
    pub partial_write_rate: f64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            read_error_rate: 0.0,
            write_error_rate: 0.0,
            sync_error_rate: 0.0,
            corruption_rate: 0.0,
            partial_write_rate: 0.0,
        }
    }
}

impl FaultConfig {
    /// Create a fault config with no faults (for baseline testing).
    #[must_use]
    pub fn no_faults() -> Self {
        Self::default()
    }

    /// Create a fault config with low fault rates (for stress testing).
    #[must_use]
    pub const fn low_faults() -> Self {
        Self {
            read_error_rate: 0.001,
            write_error_rate: 0.001,
            sync_error_rate: 0.001,
            corruption_rate: 0.001,
            partial_write_rate: 0.001,
        }
    }

    /// Create a fault config with high fault rates (for extreme testing).
    #[must_use]
    pub const fn high_faults() -> Self {
        Self {
            read_error_rate: 0.05,
            write_error_rate: 0.05,
            sync_error_rate: 0.05,
            corruption_rate: 0.05,
            partial_write_rate: 0.05,
        }
    }
}

/// A WAL record stored in simulated storage.
#[derive(Debug, Clone)]
struct SimulatedWalRecord {
    lsn: Lsn,
    txn_id: u64,
    hlc: HlcTimestamp,
    payload: LogRecordPayload,
}

/// In-memory storage implementation for deterministic testing.
///
/// This implementation stores pages in memory and supports:
/// - Fault injection based on configurable rates
/// - Deterministic behavior via seeded RNG
/// - Full WAL simulation
///
/// # Thread Safety
///
/// This implementation is not thread-safe. For DST, we run everything
/// in a single thread, so this is fine.
pub struct SimulatedStorage {
    /// In-memory page storage.
    pages: HashMap<PageId, Page>,
    /// Total number of pages (including unwritten ones).
    total_pages: u64,
    /// The superblock.
    superblock: Superblock,

    /// WAL records.
    wal_records: Vec<SimulatedWalRecord>,
    /// Next LSN to assign.
    next_lsn: Lsn,
    /// WAL capacity in bytes (for simulation, we don't enforce this strictly).
    wal_capacity: u64,
    /// Whether the WAL has been initialized.
    wal_initialized: bool,

    /// Fault injection configuration.
    fault_config: FaultConfig,
    /// Random number generator for fault injection.
    rng: StdRng,

    /// Statistics for tracking.
    stats: SimulatedStorageStats,
}

/// Statistics about simulated storage operations.
#[derive(Debug, Default, Clone)]
pub struct SimulatedStorageStats {
    /// Number of page reads.
    pub reads: u64,
    /// Number of page writes.
    pub writes: u64,
    /// Number of syncs.
    pub syncs: u64,
    /// Number of injected read errors.
    pub injected_read_errors: u64,
    /// Number of injected write errors.
    pub injected_write_errors: u64,
    /// Number of injected sync errors.
    pub injected_sync_errors: u64,
    /// Number of corrupted pages returned.
    pub corrupted_reads: u64,
    /// Number of partial writes.
    pub partial_writes: u64,
}

impl SimulatedStorage {
    /// Create a new simulated storage with the given seed.
    ///
    /// The seed ensures deterministic behavior - the same seed will
    /// produce the same sequence of faults.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self::with_config(seed, FaultConfig::default())
    }

    /// Create a new simulated storage with custom fault configuration.
    #[must_use]
    pub fn with_config(seed: u64, fault_config: FaultConfig) -> Self {
        let mut storage = Self {
            pages: HashMap::new(),
            total_pages: 1, // Page 0 is the superblock
            superblock: Superblock::new(),
            wal_records: Vec::new(),
            next_lsn: 1,
            wal_capacity: 0,
            wal_initialized: false,
            fault_config,
            rng: StdRng::seed_from_u64(seed),
            stats: SimulatedStorageStats::default(),
        };

        // Initialize page 0 with the superblock
        let superblock_page = storage.superblock.to_page();
        storage.pages.insert(0, superblock_page);

        storage
    }

    /// Get the current statistics.
    #[must_use]
    pub const fn stats(&self) -> &SimulatedStorageStats {
        &self.stats
    }

    /// Reset statistics.
    pub fn reset_stats(&mut self) {
        self.stats = SimulatedStorageStats::default();
    }

    /// Update the fault configuration.
    pub const fn set_fault_config(&mut self, config: FaultConfig) {
        self.fault_config = config;
    }

    /// Check if a fault should be injected based on the given rate.
    fn should_inject_fault(&mut self, rate: f64) -> bool {
        if rate <= 0.0 {
            return false;
        }
        self.rng.random::<f64>() < rate
    }

    /// Corrupt a page by flipping random bits.
    fn corrupt_page(&mut self, page: &mut Page) {
        // Flip 1-8 random bits
        let num_flips = self.rng.random_range(1..=8);
        for _ in 0..num_flips {
            let byte_offset = self.rng.random_range(0..PAGE_SIZE);
            let bit = self.rng.random_range(0..8u8);
            let bytes = page.as_bytes_mut();
            bytes[byte_offset] ^= 1 << bit;
        }
    }

    /// Simulate a partial write by zeroing out part of the page.
    fn make_partial_write(&mut self, page: &mut Page) {
        // Write only a portion of the page
        let cutoff = self.rng.random_range(0..PAGE_SIZE);
        let bytes = page.as_bytes_mut();
        for byte in bytes.iter_mut().skip(cutoff) {
            *byte = 0;
        }
    }
}

impl Storage for SimulatedStorage {
    fn read_page(&mut self, page_id: PageId) -> Result<Page, StorageError> {
        self.stats.reads += 1;

        if page_id >= self.total_pages {
            return Err(StorageError::PageOutOfBounds {
                page_id,
                total_pages: self.total_pages,
            });
        }

        // Check for injected read error
        if self.should_inject_fault(self.fault_config.read_error_rate) {
            self.stats.injected_read_errors += 1;
            return Err(StorageError::InjectedFault(
                "simulated read error".to_string(),
            ));
        }

        // Get or create the page
        let mut page = self.pages.get(&page_id).cloned().unwrap_or_else(Page::new);

        // Check for corruption
        if self.should_inject_fault(self.fault_config.corruption_rate) {
            self.stats.corrupted_reads += 1;
            self.corrupt_page(&mut page);
        }

        Ok(page)
    }

    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<(), StorageError> {
        self.stats.writes += 1;

        if page_id >= self.total_pages {
            return Err(StorageError::PageOutOfBounds {
                page_id,
                total_pages: self.total_pages,
            });
        }

        // Check for injected write error
        if self.should_inject_fault(self.fault_config.write_error_rate) {
            self.stats.injected_write_errors += 1;
            return Err(StorageError::InjectedFault(
                "simulated write error".to_string(),
            ));
        }

        let mut page_to_write = page.clone();

        // Check for partial write
        if self.should_inject_fault(self.fault_config.partial_write_rate) {
            self.stats.partial_writes += 1;
            self.make_partial_write(&mut page_to_write);
        }

        self.pages.insert(page_id, page_to_write);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        self.stats.syncs += 1;

        // Check for injected sync error
        if self.should_inject_fault(self.fault_config.sync_error_rate) {
            self.stats.injected_sync_errors += 1;
            return Err(StorageError::InjectedFault(
                "simulated sync error".to_string(),
            ));
        }

        // In simulated storage, sync is a no-op (writes are already "durable")
        Ok(())
    }

    fn allocate_pages(&mut self, count: u64) -> Result<PageId, StorageError> {
        let first_new_page = self.total_pages;
        self.total_pages += count;

        // Update superblock
        self.superblock.total_page_count = self.total_pages;
        self.superblock.file_size = self.total_pages * (PAGE_SIZE as u64);

        Ok(first_new_page)
    }

    fn total_pages(&self) -> u64 {
        self.total_pages
    }

    fn superblock(&self) -> &Superblock {
        &self.superblock
    }

    fn superblock_mut(&mut self) -> &mut Superblock {
        &mut self.superblock
    }

    fn write_superblock(&mut self) -> Result<(), StorageError> {
        let page = self.superblock.to_page();
        self.pages.insert(0, page);
        Ok(())
    }

    fn has_wal(&self) -> bool {
        self.wal_initialized
    }

    fn init_wal(&mut self, capacity: u64) -> Result<(), StorageError> {
        self.wal_capacity = capacity;
        self.wal_initialized = true;
        self.next_lsn = 1;

        // Update superblock (we don't actually allocate pages for the WAL
        // in simulation - we just store records in a Vec)
        self.superblock.txn_log_capacity = capacity;
        self.superblock.txn_log_start = 0; // Not used in simulation
        self.superblock.txn_log_end = 0;
        self.superblock.last_checkpoint_lsn = 0;

        Ok(())
    }

    fn wal_append(
        &mut self,
        txn_id: u64,
        hlc: HlcTimestamp,
        payload: LogRecordPayload,
    ) -> Result<Lsn, StorageError> {
        if !self.wal_initialized {
            return Err(StorageError::WalNotInitialized);
        }

        let lsn = self.next_lsn;
        self.next_lsn += 1;

        self.wal_records.push(SimulatedWalRecord {
            lsn,
            txn_id,
            hlc,
            payload,
        });

        self.superblock.last_wal_lsn = lsn;

        Ok(lsn)
    }

    fn wal_sync(&mut self) -> Result<(), StorageError> {
        if !self.wal_initialized {
            return Err(StorageError::WalNotInitialized);
        }
        // In simulation, sync is a no-op
        Ok(())
    }

    fn wal_read_all(&mut self) -> Result<Vec<LogRecord>, StorageError> {
        if !self.wal_initialized {
            return Err(StorageError::WalNotInitialized);
        }

        let records = self
            .wal_records
            .iter()
            .map(|r| LogRecord {
                txn_id: r.txn_id,
                lsn: r.lsn,
                hlc: r.hlc,
                payload: r.payload.clone(),
            })
            .collect();

        Ok(records)
    }

    fn wal_changes_since(&mut self, since: HlcTimestamp) -> Result<Vec<LogRecord>, StorageError> {
        if !self.wal_initialized {
            return Err(StorageError::WalNotInitialized);
        }

        let records = self
            .wal_records
            .iter()
            .filter(|r| {
                r.hlc.physical_time > since.physical_time
                    || (r.hlc.physical_time == since.physical_time
                        && r.hlc.logical_counter > since.logical_counter)
            })
            .map(|r| LogRecord {
                txn_id: r.txn_id,
                lsn: r.lsn,
                hlc: r.hlc,
                payload: r.payload.clone(),
            })
            .collect();

        Ok(records)
    }

    fn wal_next_lsn(&self) -> Result<Lsn, StorageError> {
        if !self.wal_initialized {
            return Err(StorageError::WalNotInitialized);
        }
        Ok(self.next_lsn)
    }

    fn wal_head(&self) -> u64 {
        // In simulation, we don't track head as a byte offset
        // Return the number of records as a proxy
        self.wal_records.len() as u64
    }

    fn wal_last_lsn(&self) -> Lsn {
        self.superblock.last_wal_lsn
    }

    fn set_checkpoint_lsn(&mut self, lsn: Lsn) {
        self.superblock.last_checkpoint_lsn = lsn;
    }

    fn set_checkpoint_hlc(&mut self, hlc: HlcTimestamp) {
        self.superblock.last_checkpoint_hlc = hlc;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulated_storage_basic() {
        let mut storage = SimulatedStorage::new(12345);

        // Initially has 1 page (superblock)
        assert_eq!(storage.total_pages(), 1);

        // Allocate some pages
        let first_page = storage.allocate_pages(5).unwrap();
        assert_eq!(first_page, 1);
        assert_eq!(storage.total_pages(), 6);

        // Write and read a page
        let mut page = Page::new();
        page.write_bytes(0, b"hello world");
        storage.write_page(2, &page).unwrap();

        let read_page = storage.read_page(2).unwrap();
        assert_eq!(read_page.read_bytes(0, 11), b"hello world");
    }

    #[test]
    fn test_simulated_storage_superblock() {
        let mut storage = SimulatedStorage::new(12345);

        storage.superblock_mut().next_txn_id = 42;
        storage.write_superblock().unwrap();

        // Verify superblock persisted
        assert_eq!(storage.superblock().next_txn_id, 42);
    }

    #[test]
    fn test_simulated_storage_wal() {
        let mut storage = SimulatedStorage::new(12345);

        assert!(!storage.has_wal());

        storage.init_wal(1024 * 1024).unwrap();
        assert!(storage.has_wal());

        let hlc = HlcTimestamp::new(1000, 0);
        let lsn = storage.wal_append(1, hlc, LogRecordPayload::Begin).unwrap();
        assert_eq!(lsn, 1);

        let records = storage.wal_read_all().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].txn_id, 1);
    }

    #[test]
    fn test_simulated_storage_fault_injection() {
        let config = FaultConfig {
            read_error_rate: 1.0, // Always fail
            ..Default::default()
        };
        let mut storage = SimulatedStorage::with_config(12345, config);

        storage.allocate_pages(1).unwrap();

        // Read should fail
        let result = storage.read_page(1);
        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::InjectedFault(_))));

        // Stats should reflect the error
        assert_eq!(storage.stats().injected_read_errors, 1);
    }

    #[test]
    fn test_simulated_storage_deterministic() {
        // Same seed should produce same behavior
        let config = FaultConfig {
            read_error_rate: 0.5,
            ..Default::default()
        };

        let mut results1 = Vec::new();
        let mut storage1 = SimulatedStorage::with_config(12345, config.clone());
        storage1.allocate_pages(10).unwrap();
        for i in 1..11 {
            results1.push(storage1.read_page(i).is_ok());
        }

        let mut results2 = Vec::new();
        let mut storage2 = SimulatedStorage::with_config(12345, config);
        storage2.allocate_pages(10).unwrap();
        for i in 1..11 {
            results2.push(storage2.read_page(i).is_ok());
        }

        assert_eq!(
            results1, results2,
            "Same seed should produce same fault pattern"
        );
    }

    #[test]
    fn test_simulated_storage_page_out_of_bounds() {
        let mut storage = SimulatedStorage::new(12345);

        let result = storage.read_page(100);
        assert!(matches!(result, Err(StorageError::PageOutOfBounds { .. })));
    }
}
