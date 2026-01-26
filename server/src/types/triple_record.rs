//! Triple record types and serialization.
//!
//! Provides `TripleRecord` struct with MVCC metadata and `TripleError`
//! for record-level errors.

use crate::types::ids::{AttributeId, EntityId};
use crate::types::{HlcTimestamp, TripleValue, TripleValueError, ValueType};

/// Transaction ID.
pub type TxnId = u64;

/// Fixed size of triple metadata (without value).
/// `entity_id` (16) + `attribute_id` (16) + `created_txn` (8) + `deleted_txn` (8) + `created_hlc` (16) = 64
const TRIPLE_METADATA_SIZE: usize = 64;

/// A complete triple record with MVCC metadata.
#[derive(Debug)]
pub struct TripleRecord {
    /// Entity ID (16 bytes).
    pub entity_id: EntityId,
    /// Attribute ID (16 bytes).
    pub attribute_id: AttributeId,
    /// Transaction ID that created this triple.
    pub created_txn: TxnId,
    /// Transaction ID that deleted this triple (0 = not deleted).
    pub deleted_txn: TxnId,
    /// HLC timestamp when created.
    pub created_hlc: HlcTimestamp,
    /// The triple's value.
    pub value: TripleValue,
}

impl TripleRecord {
    /// Create a new triple record.
    #[must_use]
    pub const fn new(
        entity_id: EntityId,
        attribute_id: AttributeId,
        created_txn: TxnId,
        created_hlc: HlcTimestamp,
        value: TripleValue,
    ) -> Self {
        Self {
            entity_id,
            attribute_id,
            created_txn,
            deleted_txn: 0,
            created_hlc,
            value,
        }
    }

    /// Check if this triple is deleted.
    #[must_use]
    pub const fn is_deleted(&self) -> bool {
        self.deleted_txn != 0
    }

    /// Check if this triple is visible to a given transaction.
    ///
    /// A triple is visible if:
    /// - `created_txn` <= `snapshot_txn`
    /// - `deleted_txn` == 0 OR `deleted_txn` > `snapshot_txn`
    #[must_use]
    pub const fn is_visible_to(&self, snapshot_txn: TxnId) -> bool {
        self.created_txn <= snapshot_txn
            && (self.deleted_txn == 0 || self.deleted_txn > snapshot_txn)
    }

    /// Check if this triple is eligible for garbage collection.
    ///
    /// A triple can be garbage collected if:
    /// - It has been deleted (`deleted_txn` != 0)
    /// - No active snapshot can see it (`deleted_txn` < `min_active_txn`)
    ///
    /// If `min_active_txn` is None (no active snapshots), any deleted record
    /// can be garbage collected.
    #[must_use]
    pub const fn is_gc_eligible(&self, min_active_txn: Option<TxnId>) -> bool {
        if self.deleted_txn == 0 {
            // Not deleted, cannot GC
            return false;
        }

        match min_active_txn {
            Some(min_txn) => {
                // Can GC if deleted before the oldest active snapshot
                self.deleted_txn < min_txn
            }
            None => {
                // No active snapshots, any deleted record can be GC'd
                true
            }
        }
    }

    /// Calculate the serialized size of this record.
    #[must_use]
    pub fn serialized_size(&self) -> usize {
        TRIPLE_METADATA_SIZE + self.value.serialized_size()
    }

    /// Serialize this record to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.serialized_size());

        bytes.extend_from_slice(&self.entity_id.0);
        bytes.extend_from_slice(&self.attribute_id.0);
        bytes.extend_from_slice(&self.created_txn.to_le_bytes());
        bytes.extend_from_slice(&self.deleted_txn.to_le_bytes());
        bytes.extend_from_slice(&self.created_hlc.to_bytes());
        bytes.extend_from_slice(&self.value.to_bytes());

        bytes
    }

    /// Deserialize a record from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TripleError> {
        if bytes.len() < TRIPLE_METADATA_SIZE + 1 {
            return Err(TripleError::InvalidRecord);
        }

        let mut entity_bytes = [0u8; 16];
        entity_bytes.copy_from_slice(&bytes[0..16]);
        let entity_id = EntityId(entity_bytes);

        let mut attribute_bytes = [0u8; 16];
        attribute_bytes.copy_from_slice(&bytes[16..32]);
        let attribute_id = AttributeId(attribute_bytes);

        let created_txn = u64::from_le_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
        ]);

        let deleted_txn = u64::from_le_bytes([
            bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46], bytes[47],
        ]);

        let mut hlc_bytes = [0u8; 16];
        hlc_bytes.copy_from_slice(&bytes[48..64]);
        let created_hlc = HlcTimestamp::from_bytes(&hlc_bytes);

        let (value, _) = TripleValue::from_bytes(&bytes[64..])?;

        Ok(Self {
            entity_id,
            attribute_id,
            created_txn,
            deleted_txn,
            created_hlc,
            value,
        })
    }
}

/// Errors that can occur with triple operations.
#[derive(Debug)]
pub enum TripleError {
    /// Invalid value format.
    InvalidValue,
    /// Invalid record format.
    InvalidRecord,
    /// Unsupported value type.
    UnsupportedValueType(ValueType),
    /// String value too large for inline storage.
    StringTooLarge(usize),
}

impl std::fmt::Display for TripleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidValue => write!(f, "invalid value format"),
            Self::InvalidRecord => write!(f, "invalid record format"),
            Self::UnsupportedValueType(t) => write!(f, "unsupported value type: {t:?}"),
            Self::StringTooLarge(size) => {
                write!(f, "string too large for inline storage: {size} bytes")
            }
        }
    }
}

impl std::error::Error for TripleError {}

impl From<TripleValueError> for TripleError {
    fn from(err: TripleValueError) -> Self {
        match err {
            TripleValueError::InvalidValue => Self::InvalidValue,
            TripleValueError::UnsupportedValueType(vt) => Self::UnsupportedValueType(vt),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_triple_record_roundtrip() {
        let record = TripleRecord::new(
            EntityId([1u8; 16]),
            AttributeId([2u8; 16]),
            100,
            HlcTimestamp::new(1000, 1),
            TripleValue::String("test value".to_string()),
        );

        let bytes = record.to_bytes();
        let decoded = TripleRecord::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.entity_id, record.entity_id);
        assert_eq!(decoded.attribute_id, record.attribute_id);
        assert_eq!(decoded.created_txn, record.created_txn);
        assert_eq!(decoded.deleted_txn, record.deleted_txn);
        assert_eq!(
            decoded.created_hlc.physical_time,
            record.created_hlc.physical_time
        );
        assert_eq!(
            decoded.created_hlc.logical_counter,
            record.created_hlc.logical_counter
        );
        assert_eq!(decoded.value, record.value);
    }

    #[test]
    fn test_triple_visibility() {
        let mut record = TripleRecord::new(
            EntityId([1u8; 16]),
            AttributeId([2u8; 16]),
            10,
            HlcTimestamp::new(1000, 0),
            TripleValue::Null,
        );

        // Visible to transactions >= 10
        assert!(!record.is_visible_to(9));
        assert!(record.is_visible_to(10));
        assert!(record.is_visible_to(100));

        // Mark as deleted at txn 50
        record.deleted_txn = 50;

        // Now only visible to transactions in [10, 50)
        assert!(!record.is_visible_to(9));
        assert!(record.is_visible_to(10));
        assert!(record.is_visible_to(49));
        assert!(!record.is_visible_to(50));
        assert!(!record.is_visible_to(100));
    }

    #[test]
    fn test_triple_serialized_size() {
        let record = TripleRecord::new(
            EntityId([0u8; 16]),
            AttributeId([0u8; 16]),
            0,
            HlcTimestamp::new(0, 0),
            TripleValue::Null,
        );
        // 64 bytes metadata + 1 byte for null value
        assert_eq!(record.serialized_size(), 65);

        let record = TripleRecord::new(
            EntityId([0u8; 16]),
            AttributeId([0u8; 16]),
            0,
            HlcTimestamp::new(0, 0),
            TripleValue::String("hello".to_string()),
        );
        // 64 bytes metadata + 1 type + 2 len + 5 data = 72
        assert_eq!(record.serialized_size(), 72);
    }

    #[test]
    fn test_gc_eligibility() {
        let mut record = TripleRecord::new(
            EntityId([1u8; 16]),
            AttributeId([2u8; 16]),
            10,
            HlcTimestamp::new(1000, 0),
            TripleValue::Null,
        );

        // Not deleted, not GC eligible
        assert!(!record.is_gc_eligible(None));
        assert!(!record.is_gc_eligible(Some(100)));

        // Mark as deleted at txn 50
        record.deleted_txn = 50;

        // Deleted, GC eligible when no active snapshots
        assert!(record.is_gc_eligible(None));

        // GC eligible when oldest snapshot is after deletion
        assert!(record.is_gc_eligible(Some(51)));
        assert!(record.is_gc_eligible(Some(100)));

        // Not GC eligible when oldest snapshot is before or at deletion
        assert!(!record.is_gc_eligible(Some(50)));
        assert!(!record.is_gc_eligible(Some(49)));
        assert!(!record.is_gc_eligible(Some(10)));
    }
}
