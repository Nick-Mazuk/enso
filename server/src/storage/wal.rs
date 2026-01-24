//! Write-Ahead Log (WAL) implementation.
//!
//! The WAL provides durability and change tracking for the storage engine.
//! It is implemented as a circular buffer of log records stored in a contiguous
//! region of pages in the database file.
//!
//! # Log Record Format
//!
//! Each record has the following layout:
//! ```text
//! +----------+--------------------------------------------------+
//! | 0-3      | record_length (4 bytes, includes header+payload) |
//! | 4        | record_type (1 byte)                             |
//! | 5-12     | transaction_id (8 bytes)                         |
//! | 13-20    | lsn (8 bytes) - Log Sequence Number              |
//! | 21-36    | hlc_timestamp (16 bytes)                         |
//! | 37-N     | payload (variable, depends on type)              |
//! | N-N+3    | CRC32 checksum (4 bytes)                         |
//! +----------+--------------------------------------------------+
//! ```
//!
//! # Circular Buffer
//!
//! The log is a circular buffer with:
//! - Fixed capacity (configured at database creation, default 64MB)
//! - Head pointer (next write position)
//! - Tail pointer (oldest record still needed)
//! - Records wrap around when reaching the end

// record_length fits in u32, capacity checks use u64
#![allow(clippy::cast_possible_truncation)]

use std::io::{Read, Seek, SeekFrom, Write};

use crate::storage::file::FileError;
use crate::storage::page::PAGE_SIZE;
use crate::storage::superblock::HlcTimestamp;
use crate::storage::triple::{AttributeId, EntityId, TripleError, TripleRecord, TxnId};

/// Default WAL capacity: 64MB
pub const DEFAULT_WAL_CAPACITY: u64 = 64 * 1024 * 1024;

/// Minimum WAL capacity: 1MB
pub const MIN_WAL_CAPACITY: u64 = 1024 * 1024;

/// Log record header size (before payload).
/// `record_length` (4) + `record_type` (1) + `txn_id` (8) + lsn (8) + hlc (16) = 37 bytes
const RECORD_HEADER_SIZE: usize = 37;

/// CRC32 checksum size at end of record.
const CHECKSUM_SIZE: usize = 4;

/// Log Sequence Number - monotonically increasing identifier for log records.
pub type Lsn = u64;

/// Log record types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogRecordType {
    /// Transaction begin marker.
    Begin = 0x01,
    /// Triple insert operation.
    Insert = 0x02,
    /// Triple update operation (contains old value reference + new value).
    Update = 0x03,
    /// Triple delete operation.
    Delete = 0x04,
    /// Transaction commit marker.
    Commit = 0x05,
    /// Checkpoint marker.
    Checkpoint = 0x06,
}

impl TryFrom<u8> for LogRecordType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Begin),
            0x02 => Ok(Self::Insert),
            0x03 => Ok(Self::Update),
            0x04 => Ok(Self::Delete),
            0x05 => Ok(Self::Commit),
            0x06 => Ok(Self::Checkpoint),
            _ => Err(value),
        }
    }
}

/// Payload for different log record types.
///
/// This stores serialized bytes to avoid requiring Clone on complex types.
#[derive(Debug, Clone)]
#[allow(clippy::disallowed_methods)] // Clone needed for simulation testing
pub enum LogRecordPayload {
    /// Begin transaction - no additional data.
    Begin,
    /// Insert a new triple (serialized bytes).
    Insert(Vec<u8>),
    /// Update a triple (serialized bytes of new record).
    Update(Vec<u8>),
    /// Delete a triple.
    Delete {
        /// Entity ID of deleted triple.
        entity_id: EntityId,
        /// Attribute ID of deleted triple.
        attribute_id: AttributeId,
    },
    /// Commit transaction - no additional data.
    Commit,
    /// Checkpoint marker with metadata.
    Checkpoint {
        /// Lowest active transaction ID at checkpoint time.
        min_active_txn: TxnId,
        /// Number of active transactions.
        active_txn_count: u64,
    },
}

impl LogRecordPayload {
    /// Get the record type for this payload.
    #[must_use]
    pub const fn record_type(&self) -> LogRecordType {
        match self {
            Self::Begin => LogRecordType::Begin,
            Self::Insert(_) => LogRecordType::Insert,
            Self::Update(_) => LogRecordType::Update,
            Self::Delete { .. } => LogRecordType::Delete,
            Self::Commit => LogRecordType::Commit,
            Self::Checkpoint { .. } => LogRecordType::Checkpoint,
        }
    }

    /// Create an Insert payload from a `TripleRecord`.
    #[must_use]
    pub fn insert(record: &TripleRecord) -> Self {
        Self::Insert(record.to_bytes())
    }

    /// Create an Update payload from a `TripleRecord`.
    #[must_use]
    pub fn update(new_record: &TripleRecord) -> Self {
        Self::Update(new_record.to_bytes())
    }

    /// Create a Delete payload.
    #[must_use]
    pub const fn delete(entity_id: EntityId, attribute_id: AttributeId) -> Self {
        Self::Delete {
            entity_id,
            attribute_id,
        }
    }

    /// Create a Checkpoint payload.
    #[must_use]
    pub const fn checkpoint(min_active_txn: TxnId, active_txn_count: u64) -> Self {
        Self::Checkpoint {
            min_active_txn,
            active_txn_count,
        }
    }

    /// Calculate the serialized size of this payload.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Vec::len() is not const-stable
    pub fn serialized_size(&self) -> usize {
        match self {
            Self::Begin | Self::Commit => 0,
            Self::Insert(bytes) | Self::Update(bytes) => bytes.len(),
            Self::Delete { .. } => 32, // entity_id (16) + attribute_id (16)
            Self::Checkpoint { .. } => 16, // min_active_txn (8) + active_txn_count (8)
        }
    }

    /// Serialize the payload to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Begin | Self::Commit => Vec::new(),
            Self::Insert(bytes) | Self::Update(bytes) => {
                let mut result = Vec::with_capacity(bytes.len());
                result.extend_from_slice(bytes);
                result
            }
            Self::Delete {
                entity_id,
                attribute_id,
            } => {
                let mut bytes = Vec::with_capacity(32);
                bytes.extend_from_slice(entity_id);
                bytes.extend_from_slice(attribute_id);
                bytes
            }
            Self::Checkpoint {
                min_active_txn,
                active_txn_count,
            } => {
                let mut bytes = Vec::with_capacity(16);
                bytes.extend_from_slice(&min_active_txn.to_le_bytes());
                bytes.extend_from_slice(&active_txn_count.to_le_bytes());
                bytes
            }
        }
    }

    /// Deserialize a payload from bytes.
    pub fn from_bytes(record_type: LogRecordType, bytes: &[u8]) -> Result<Self, WalError> {
        match record_type {
            LogRecordType::Begin => Ok(Self::Begin),
            LogRecordType::Commit => Ok(Self::Commit),
            LogRecordType::Insert => Ok(Self::Insert(bytes.to_vec())),
            LogRecordType::Update => Ok(Self::Update(bytes.to_vec())),
            LogRecordType::Delete => {
                if bytes.len() < 32 {
                    return Err(WalError::CorruptRecord);
                }
                let mut entity_id = [0u8; 16];
                let mut attribute_id = [0u8; 16];
                entity_id.copy_from_slice(&bytes[0..16]);
                attribute_id.copy_from_slice(&bytes[16..32]);
                Ok(Self::Delete {
                    entity_id,
                    attribute_id,
                })
            }
            LogRecordType::Checkpoint => {
                if bytes.len() < 16 {
                    return Err(WalError::CorruptRecord);
                }
                let min_active_txn = u64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                let active_txn_count = u64::from_le_bytes([
                    bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                    bytes[15],
                ]);
                Ok(Self::Checkpoint {
                    min_active_txn,
                    active_txn_count,
                })
            }
        }
    }

    /// Deserialize the triple record from an Insert or Update payload.
    ///
    /// Returns `None` if this is not an Insert or Update payload.
    pub fn triple_record(&self) -> Result<Option<TripleRecord>, TripleError> {
        match self {
            Self::Insert(bytes) | Self::Update(bytes) => Ok(Some(TripleRecord::from_bytes(bytes)?)),
            _ => Ok(None),
        }
    }
}

/// A complete log record.
#[derive(Debug)]
pub struct LogRecord {
    /// Transaction ID that wrote this record.
    pub txn_id: TxnId,
    /// Log Sequence Number (position in the log).
    pub lsn: Lsn,
    /// HLC timestamp when the record was written.
    pub hlc: HlcTimestamp,
    /// The record payload.
    pub payload: LogRecordPayload,
}

impl LogRecord {
    /// Create a new log record.
    #[must_use]
    pub const fn new(
        txn_id: TxnId,
        lsn: Lsn,
        hlc: HlcTimestamp,
        payload: LogRecordPayload,
    ) -> Self {
        Self {
            txn_id,
            lsn,
            hlc,
            payload,
        }
    }

    /// Calculate the total serialized size of this record.
    #[must_use]
    pub fn serialized_size(&self) -> usize {
        RECORD_HEADER_SIZE + self.payload.serialized_size() + CHECKSUM_SIZE
    }

    /// Serialize this record to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let payload_bytes = self.payload.to_bytes();
        let total_len = RECORD_HEADER_SIZE + payload_bytes.len() + CHECKSUM_SIZE;

        let mut bytes = Vec::with_capacity(total_len);

        // Record length (4 bytes)
        bytes.extend_from_slice(&(total_len as u32).to_le_bytes());

        // Record type (1 byte)
        bytes.push(self.payload.record_type() as u8);

        // Transaction ID (8 bytes)
        bytes.extend_from_slice(&self.txn_id.to_le_bytes());

        // LSN (8 bytes)
        bytes.extend_from_slice(&self.lsn.to_le_bytes());

        // HLC timestamp (16 bytes)
        bytes.extend_from_slice(&self.hlc.to_bytes());

        // Payload (variable)
        bytes.extend_from_slice(&payload_bytes);

        // CRC32 checksum (4 bytes) - computed over everything before it
        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());

        bytes
    }

    /// Deserialize a record from bytes.
    ///
    /// Returns the record and the number of bytes consumed.
    pub fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), WalError> {
        if bytes.len() < RECORD_HEADER_SIZE + CHECKSUM_SIZE {
            return Err(WalError::CorruptRecord);
        }

        // Read record length
        let record_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;

        if record_len < RECORD_HEADER_SIZE + CHECKSUM_SIZE || record_len > bytes.len() {
            return Err(WalError::CorruptRecord);
        }

        // Verify checksum
        let stored_checksum = u32::from_le_bytes([
            bytes[record_len - 4],
            bytes[record_len - 3],
            bytes[record_len - 2],
            bytes[record_len - 1],
        ]);
        let computed_checksum = crc32fast::hash(&bytes[..record_len - 4]);
        if stored_checksum != computed_checksum {
            return Err(WalError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }

        // Parse header
        let record_type = LogRecordType::try_from(bytes[4]).map_err(WalError::InvalidRecordType)?;

        let txn_id = u64::from_le_bytes([
            bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
        ]);

        let lsn = u64::from_le_bytes([
            bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19], bytes[20],
        ]);

        let mut hlc_bytes = [0u8; 16];
        hlc_bytes.copy_from_slice(&bytes[21..37]);
        let hlc = HlcTimestamp::from_bytes(&hlc_bytes);

        // Parse payload
        let payload_bytes = &bytes[RECORD_HEADER_SIZE..record_len - CHECKSUM_SIZE];
        let payload = LogRecordPayload::from_bytes(record_type, payload_bytes)?;

        Ok((
            Self {
                txn_id,
                lsn,
                hlc,
                payload,
            },
            record_len,
        ))
    }

    /// Get the `TripleRecord` from this log record if it's an Insert or Update.
    pub fn triple_record(&self) -> Result<Option<TripleRecord>, TripleError> {
        self.payload.triple_record()
    }
}

/// Write-Ahead Log manager.
///
/// Manages a circular buffer of log records in the database file.
pub struct Wal<'a, F: Read + Write + Seek> {
    /// The underlying file handle.
    file: &'a mut F,
    /// Start offset of the WAL region in the file.
    region_start: u64,
    /// Capacity of the WAL region in bytes.
    capacity: u64,
    /// Current write position (offset from `region_start`).
    head: u64,
    /// Oldest record position (offset from `region_start`).
    tail: u64,
    /// Next LSN to assign.
    next_lsn: Lsn,
    /// Whether the buffer has wrapped around.
    wrapped: bool,
}

impl<'a, F: Read + Write + Seek> Wal<'a, F> {
    /// Create a new WAL manager.
    ///
    /// # Arguments
    /// - `file`: The file handle to write to
    /// - `region_start`: Byte offset where the WAL region begins
    /// - `capacity`: Total capacity of the WAL region in bytes
    /// - `head`: Current write position (relative to `region_start`)
    /// - `tail`: Oldest record position (relative to `region_start`)
    /// - `next_lsn`: Next LSN to assign
    #[must_use]
    pub const fn new(
        file: &'a mut F,
        region_start: u64,
        capacity: u64,
        head: u64,
        tail: u64,
        next_lsn: Lsn,
    ) -> Self {
        Self {
            file,
            region_start,
            capacity,
            head,
            tail,
            next_lsn,
            wrapped: head < tail,
        }
    }

    /// Get the current head position (relative to `region_start`).
    #[must_use]
    pub const fn head(&self) -> u64 {
        self.head
    }

    /// Get the current tail position (relative to `region_start`).
    #[must_use]
    pub const fn tail(&self) -> u64 {
        self.tail
    }

    /// Get the next LSN that will be assigned.
    #[must_use]
    pub const fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    /// Get the last assigned LSN (0 if none assigned yet).
    #[must_use]
    pub const fn last_lsn(&self) -> Lsn {
        if self.next_lsn > 0 {
            self.next_lsn - 1
        } else {
            0
        }
    }

    /// Calculate the used space in the log.
    #[must_use]
    pub const fn used_space(&self) -> u64 {
        if self.wrapped {
            // head < tail, so used = (capacity - tail) + head
            (self.capacity - self.tail) + self.head
        } else {
            // head >= tail, used = head - tail
            self.head - self.tail
        }
    }

    /// Calculate the free space in the log.
    #[must_use]
    pub const fn free_space(&self) -> u64 {
        self.capacity - self.used_space()
    }

    /// Check if the log is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.head == self.tail && !self.wrapped
    }

    /// Append a log record to the WAL.
    ///
    /// Returns the LSN assigned to this record.
    pub fn append(
        &mut self,
        txn_id: TxnId,
        hlc: HlcTimestamp,
        payload: LogRecordPayload,
    ) -> Result<Lsn, WalError> {
        let lsn = self.next_lsn;
        let record = LogRecord::new(txn_id, lsn, hlc, payload);
        let bytes = record.to_bytes();
        let record_len = bytes.len() as u64;

        // Check if we have enough space
        if record_len > self.capacity {
            return Err(WalError::RecordTooLarge {
                size: record_len,
                capacity: self.capacity,
            });
        }

        // Check if we need to wrap or advance tail
        let space_to_end = self.capacity - self.head;
        if record_len > space_to_end {
            // Not enough contiguous space at end, wrap to beginning
            // First, advance tail past any records we're overwriting
            self.advance_tail_to(record_len)?;

            // Write at the beginning
            self.file
                .seek(SeekFrom::Start(self.region_start))
                .map_err(WalError::Io)?;
            self.file.write_all(&bytes).map_err(WalError::Io)?;

            self.head = record_len;
            self.wrapped = self.head <= self.tail;
        } else {
            // Enough space at current position
            // Check if we're catching up to tail
            if self.wrapped && self.head + record_len > self.tail {
                self.advance_tail_to(self.head + record_len - self.tail)?;
            }

            self.file
                .seek(SeekFrom::Start(self.region_start + self.head))
                .map_err(WalError::Io)?;
            self.file.write_all(&bytes).map_err(WalError::Io)?;

            self.head += record_len;
            if self.head >= self.capacity {
                self.head = 0;
                self.wrapped = true;
            }
        }

        self.next_lsn += 1;
        Ok(lsn)
    }

    /// Advance the tail pointer to free up space.
    fn advance_tail_to(&mut self, needed: u64) -> Result<(), WalError> {
        let mut freed = 0u64;

        while freed < needed && !self.is_empty() {
            // Read the record length at tail
            self.file
                .seek(SeekFrom::Start(self.region_start + self.tail))
                .map_err(WalError::Io)?;

            let mut len_bytes = [0u8; 4];
            self.file.read_exact(&mut len_bytes).map_err(WalError::Io)?;
            let record_len = u64::from(u32::from_le_bytes(len_bytes));

            if record_len == 0 || record_len > self.capacity {
                // Corrupt or empty record, just advance by minimum
                self.tail = (self.tail + 1) % self.capacity;
                freed += 1;
            } else {
                self.tail += record_len;
                freed += record_len;

                if self.tail >= self.capacity {
                    self.tail = 0;
                    self.wrapped = false;
                }
            }
        }

        Ok(())
    }

    /// Sync the WAL to disk.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.file.flush().map_err(WalError::Io)?;
        Ok(())
    }

    /// Read a record at the given offset (relative to `region_start`).
    pub fn read_at(&mut self, offset: u64) -> Result<(LogRecord, u64), WalError> {
        if offset >= self.capacity {
            return Err(WalError::InvalidOffset(offset));
        }

        self.file
            .seek(SeekFrom::Start(self.region_start + offset))
            .map_err(WalError::Io)?;

        // Read enough bytes for header + checksum to determine full length
        let mut header_buf = [0u8; 4];
        self.file
            .read_exact(&mut header_buf)
            .map_err(WalError::Io)?;

        let record_len = u32::from_le_bytes(header_buf) as usize;

        if record_len < RECORD_HEADER_SIZE + CHECKSUM_SIZE {
            return Err(WalError::CorruptRecord);
        }

        // Read the full record
        self.file
            .seek(SeekFrom::Start(self.region_start + offset))
            .map_err(WalError::Io)?;

        let mut record_buf = vec![0u8; record_len];
        self.file
            .read_exact(&mut record_buf)
            .map_err(WalError::Io)?;

        let (record, consumed) = LogRecord::from_bytes(&record_buf)?;

        // Calculate next offset (with wrap-around)
        let next_offset = offset + consumed as u64;
        let next_offset = if next_offset >= self.capacity {
            0
        } else {
            next_offset
        };

        Ok((record, next_offset))
    }

    /// Read all log records from tail to head.
    ///
    /// This collects all records into a vector. For streaming access,
    /// use `read_at` with manual offset tracking.
    pub fn read_all(&mut self) -> Result<Vec<LogRecord>, WalError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        let mut offset = self.tail;

        loop {
            let (record, next_offset) = self.read_at(offset)?;
            records.push(record);

            // Check if we've reached the head
            if next_offset == self.head {
                break;
            }
            if self.wrapped && offset >= self.head && next_offset <= self.head {
                break;
            }

            offset = next_offset;

            // Safety limit to prevent infinite loops
            if records.len()
                > (self.capacity / (RECORD_HEADER_SIZE + CHECKSUM_SIZE) as u64) as usize
            {
                break;
            }
        }

        Ok(records)
    }

    /// Find the offset of a record with the given LSN.
    ///
    /// Returns the offset (relative to `region_start`) if found.
    pub fn find_lsn(&mut self, target_lsn: Lsn) -> Result<Option<u64>, WalError> {
        if self.is_empty() {
            return Ok(None);
        }

        let mut offset = self.tail;
        let max_iterations = self.capacity / (RECORD_HEADER_SIZE + CHECKSUM_SIZE) as u64;

        for _ in 0..max_iterations {
            let (record, next_offset) = self.read_at(offset)?;

            if record.lsn == target_lsn {
                return Ok(Some(offset));
            }

            if record.lsn > target_lsn {
                // We've passed it, LSN not found
                return Ok(None);
            }

            // Check if we've reached the head
            if next_offset == self.head {
                break;
            }
            if self.wrapped && offset >= self.head && next_offset <= self.head {
                break;
            }

            offset = next_offset;
        }

        Ok(None)
    }

    /// Read all records since a given LSN (inclusive).
    ///
    /// Returns an empty vector if the LSN is not found.
    pub fn read_from_lsn(&mut self, target_lsn: Lsn) -> Result<Vec<LogRecord>, WalError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        let Some(start_offset) = self.find_lsn(target_lsn)? else {
            return Ok(Vec::new());
        };

        let mut records = Vec::new();
        let mut offset = start_offset;

        loop {
            let (record, next_offset) = self.read_at(offset)?;
            records.push(record);

            // Check if we've reached the head
            if next_offset == self.head {
                break;
            }
            if self.wrapped && offset >= self.head && next_offset <= self.head {
                break;
            }

            offset = next_offset;

            // Safety limit
            if records.len()
                > (self.capacity / (RECORD_HEADER_SIZE + CHECKSUM_SIZE) as u64) as usize
            {
                break;
            }
        }

        Ok(records)
    }

    /// Read all change records (INSERT, UPDATE, DELETE) since a given HLC timestamp.
    ///
    /// Returns records where HLC >= the given timestamp.
    pub fn changes_since(&mut self, target_hlc: HlcTimestamp) -> Result<Vec<LogRecord>, WalError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        let mut changes = Vec::new();
        let mut offset = self.tail;
        let max_iterations = self.capacity / (RECORD_HEADER_SIZE + CHECKSUM_SIZE) as u64;

        for _ in 0..max_iterations {
            let (record, next_offset) = self.read_at(offset)?;

            // Check HLC
            let hlc_matches = record.hlc.physical_time > target_hlc.physical_time
                || (record.hlc.physical_time == target_hlc.physical_time
                    && record.hlc.logical_counter >= target_hlc.logical_counter);

            if hlc_matches {
                match &record.payload {
                    LogRecordPayload::Insert(_)
                    | LogRecordPayload::Update(_)
                    | LogRecordPayload::Delete { .. } => {
                        changes.push(record);
                    }
                    _ => {} // Skip BEGIN, COMMIT, CHECKPOINT
                }
            }

            // Check if we've reached the head
            if next_offset == self.head {
                break;
            }
            if self.wrapped && offset >= self.head && next_offset <= self.head {
                break;
            }

            offset = next_offset;
        }

        Ok(changes)
    }
}

/// Errors that can occur during WAL operations.
#[derive(Debug)]
pub enum WalError {
    /// I/O error.
    Io(std::io::Error),
    /// Corrupt log record.
    CorruptRecord,
    /// Invalid record type byte.
    InvalidRecordType(u8),
    /// Checksum mismatch.
    ChecksumMismatch { expected: u32, actual: u32 },
    /// Record too large for WAL capacity.
    RecordTooLarge { size: u64, capacity: u64 },
    /// Invalid offset in WAL.
    InvalidOffset(u64),
    /// LSN not found (may have been overwritten).
    LsnNotFound(Lsn),
    /// WAL region not initialized.
    NotInitialized,
    /// File error.
    File(FileError),
    /// Triple deserialization error.
    Triple(TripleError),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "WAL I/O error: {e}"),
            Self::CorruptRecord => write!(f, "corrupt WAL record"),
            Self::InvalidRecordType(t) => write!(f, "invalid WAL record type: 0x{t:02x}"),
            Self::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "WAL checksum mismatch: expected 0x{expected:08x}, got 0x{actual:08x}"
                )
            }
            Self::RecordTooLarge { size, capacity } => {
                write!(
                    f,
                    "WAL record too large: {size} bytes exceeds capacity of {capacity} bytes"
                )
            }
            Self::InvalidOffset(o) => write!(f, "invalid WAL offset: {o}"),
            Self::LsnNotFound(lsn) => write!(f, "LSN {lsn} not found in WAL (may be overwritten)"),
            Self::NotInitialized => write!(f, "WAL region not initialized"),
            Self::File(e) => write!(f, "WAL file error: {e}"),
            Self::Triple(e) => write!(f, "WAL triple error: {e}"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::File(e) => Some(e),
            Self::Triple(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<FileError> for WalError {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

impl From<TripleError> for WalError {
    fn from(e: TripleError) -> Self {
        Self::Triple(e)
    }
}

/// Calculate the number of pages needed for a given WAL capacity.
#[must_use]
pub const fn pages_for_capacity(capacity: u64) -> u64 {
    capacity.div_ceil(PAGE_SIZE as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::triple::TripleValue;
    use std::io::Cursor;

    fn create_test_cursor(capacity: usize) -> Cursor<Vec<u8>> {
        Cursor::new(vec![0u8; capacity])
    }

    #[test]
    fn test_log_record_roundtrip_begin() {
        let record = LogRecord::new(1, 100, HlcTimestamp::new(1000, 1), LogRecordPayload::Begin);

        let bytes = record.to_bytes();
        let (decoded, consumed) = LogRecord::from_bytes(&bytes).unwrap();

        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.txn_id, 1);
        assert_eq!(decoded.lsn, 100);
        assert_eq!(decoded.hlc.physical_time, 1000);
        assert!(matches!(decoded.payload, LogRecordPayload::Begin));
    }

    #[test]
    fn test_log_record_roundtrip_commit() {
        let record = LogRecord::new(
            42,
            200,
            HlcTimestamp::new(2000, 5),
            LogRecordPayload::Commit,
        );

        let bytes = record.to_bytes();
        let (decoded, _) = LogRecord::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.txn_id, 42);
        assert_eq!(decoded.lsn, 200);
        assert!(matches!(decoded.payload, LogRecordPayload::Commit));
    }

    #[test]
    fn test_log_record_roundtrip_insert() {
        let triple = TripleRecord::new(
            [1u8; 16],
            [2u8; 16],
            10,
            HlcTimestamp::new(500, 0),
            TripleValue::String("test value".to_string()),
        );

        let record = LogRecord::new(
            10,
            300,
            HlcTimestamp::new(3000, 0),
            LogRecordPayload::insert(&triple),
        );

        let bytes = record.to_bytes();
        let (decoded, _) = LogRecord::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.txn_id, 10);
        assert_eq!(decoded.lsn, 300);

        let rec = decoded.triple_record().unwrap().unwrap();
        assert_eq!(rec.entity_id, [1u8; 16]);
        assert_eq!(rec.attribute_id, [2u8; 16]);
        assert_eq!(rec.value, TripleValue::String("test value".to_string()));
    }

    #[test]
    fn test_log_record_roundtrip_delete() {
        let record = LogRecord::new(
            5,
            400,
            HlcTimestamp::new(4000, 2),
            LogRecordPayload::delete([3u8; 16], [4u8; 16]),
        );

        let bytes = record.to_bytes();
        let (decoded, _) = LogRecord::from_bytes(&bytes).unwrap();

        if let LogRecordPayload::Delete {
            entity_id,
            attribute_id,
        } = decoded.payload
        {
            assert_eq!(entity_id, [3u8; 16]);
            assert_eq!(attribute_id, [4u8; 16]);
        } else {
            panic!("expected Delete payload");
        }
    }

    #[test]
    fn test_log_record_roundtrip_checkpoint() {
        let record = LogRecord::new(
            0,
            500,
            HlcTimestamp::new(5000, 0),
            LogRecordPayload::checkpoint(100, 3),
        );

        let bytes = record.to_bytes();
        let (decoded, _) = LogRecord::from_bytes(&bytes).unwrap();

        if let LogRecordPayload::Checkpoint {
            min_active_txn,
            active_txn_count,
        } = decoded.payload
        {
            assert_eq!(min_active_txn, 100);
            assert_eq!(active_txn_count, 3);
        } else {
            panic!("expected Checkpoint payload");
        }
    }

    #[test]
    fn test_checksum_validation() {
        let record = LogRecord::new(1, 100, HlcTimestamp::new(1000, 0), LogRecordPayload::Begin);

        let mut bytes = record.to_bytes();
        // Corrupt a byte
        bytes[5] ^= 0xFF;

        let result = LogRecord::from_bytes(&bytes);
        assert!(matches!(result, Err(WalError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_wal_append_and_read() {
        let mut cursor = create_test_cursor(4096);
        let mut wal = Wal::new(&mut cursor, 0, 4096, 0, 0, 1);

        // Append a record
        let lsn = wal
            .append(1, HlcTimestamp::new(1000, 0), LogRecordPayload::Begin)
            .unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(wal.next_lsn(), 2);

        // Read it back
        let (record, _) = wal.read_at(0).unwrap();
        assert_eq!(record.lsn, 1);
        assert_eq!(record.txn_id, 1);
    }

    #[test]
    fn test_wal_multiple_records() {
        let mut cursor = create_test_cursor(8192);
        let mut wal = Wal::new(&mut cursor, 0, 8192, 0, 0, 1);

        // Append several records
        wal.append(1, HlcTimestamp::new(1000, 0), LogRecordPayload::Begin)
            .unwrap();

        let triple = TripleRecord::new(
            [1u8; 16],
            [2u8; 16],
            1,
            HlcTimestamp::new(1000, 0),
            TripleValue::Number(42.0),
        );
        wal.append(
            1,
            HlcTimestamp::new(1001, 0),
            LogRecordPayload::insert(&triple),
        )
        .unwrap();

        wal.append(1, HlcTimestamp::new(1002, 0), LogRecordPayload::Commit)
            .unwrap();

        assert_eq!(wal.next_lsn(), 4);
        assert!(!wal.is_empty());
    }

    #[test]
    fn test_wal_space_tracking() {
        let mut cursor = create_test_cursor(1024);
        let mut wal = Wal::new(&mut cursor, 0, 1024, 0, 0, 1);

        assert_eq!(wal.used_space(), 0);
        assert_eq!(wal.free_space(), 1024);
        assert!(wal.is_empty());

        wal.append(1, HlcTimestamp::new(1000, 0), LogRecordPayload::Begin)
            .unwrap();

        assert!(wal.used_space() > 0);
        assert!(wal.free_space() < 1024);
        assert!(!wal.is_empty());
    }

    #[test]
    fn test_pages_for_capacity() {
        assert_eq!(pages_for_capacity(8192), 1);
        assert_eq!(pages_for_capacity(8193), 2);
        assert_eq!(pages_for_capacity(16384), 2);
        assert_eq!(pages_for_capacity(DEFAULT_WAL_CAPACITY), 8192);
    }

    #[test]
    fn test_record_type_conversion() {
        assert_eq!(LogRecordType::try_from(0x01), Ok(LogRecordType::Begin));
        assert_eq!(LogRecordType::try_from(0x05), Ok(LogRecordType::Commit));
        assert!(LogRecordType::try_from(0xFF).is_err());
    }

    #[test]
    fn test_payload_serialized_size() {
        assert_eq!(LogRecordPayload::Begin.serialized_size(), 0);
        assert_eq!(LogRecordPayload::Commit.serialized_size(), 0);
        assert_eq!(
            LogRecordPayload::delete([0u8; 16], [0u8; 16]).serialized_size(),
            32
        );
        assert_eq!(LogRecordPayload::checkpoint(0, 0).serialized_size(), 16);
    }

    #[test]
    fn test_wal_read_all() {
        let mut cursor = create_test_cursor(8192);
        let mut wal = Wal::new(&mut cursor, 0, 8192, 0, 0, 1);

        // Append several records
        wal.append(1, HlcTimestamp::new(1000, 0), LogRecordPayload::Begin)
            .unwrap();
        wal.append(1, HlcTimestamp::new(1001, 0), LogRecordPayload::Commit)
            .unwrap();
        wal.append(2, HlcTimestamp::new(1002, 0), LogRecordPayload::Begin)
            .unwrap();

        let records = wal.read_all().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].lsn, 1);
        assert_eq!(records[1].lsn, 2);
        assert_eq!(records[2].lsn, 3);
    }

    #[test]
    fn test_wal_changes_since() {
        let mut cursor = create_test_cursor(8192);
        let mut wal = Wal::new(&mut cursor, 0, 8192, 0, 0, 1);

        let triple = TripleRecord::new(
            [1u8; 16],
            [2u8; 16],
            1,
            HlcTimestamp::new(1000, 0),
            TripleValue::Number(42.0),
        );

        // Append records
        wal.append(1, HlcTimestamp::new(1000, 0), LogRecordPayload::Begin)
            .unwrap();
        wal.append(
            1,
            HlcTimestamp::new(1001, 0),
            LogRecordPayload::insert(&triple),
        )
        .unwrap();
        wal.append(1, HlcTimestamp::new(1002, 0), LogRecordPayload::Commit)
            .unwrap();

        // Get changes since HLC 1000
        let changes = wal.changes_since(HlcTimestamp::new(1000, 0)).unwrap();

        // Should only return the Insert, not Begin or Commit
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0].payload, LogRecordPayload::Insert(_)));
    }
}
