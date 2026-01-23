//! Triple record format for the storage engine.
//!
//! Each triple is stored as a variable-length record with MVCC metadata.

use crate::storage::superblock::HlcTimestamp;

/// Entity ID (16 bytes).
pub type EntityId = [u8; 16];

/// Attribute ID (16 bytes).
pub type AttributeId = [u8; 16];

/// Transaction ID.
pub type TxnId = u64;

/// Value type discriminants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Null = 0x01,
    Boolean = 0x02,
    Number = 0x03,
    StringInline = 0x04,
    StringOverflow = 0x05, // Not implemented in Phase 1
    Date = 0x06,           // Future
    Blob = 0x07,           // Future
}

impl TryFrom<u8> for ValueType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Null),
            0x02 => Ok(Self::Boolean),
            0x03 => Ok(Self::Number),
            0x04 => Ok(Self::StringInline),
            0x05 => Ok(Self::StringOverflow),
            0x06 => Ok(Self::Date),
            0x07 => Ok(Self::Blob),
            _ => Err(value),
        }
    }
}

/// A triple value.
#[derive(Debug, PartialEq)]
pub enum TripleValue {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
}

impl TripleValue {
    /// Get the value type discriminant.
    #[must_use]
    pub const fn value_type(&self) -> ValueType {
        match self {
            Self::Null => ValueType::Null,
            Self::Boolean(_) => ValueType::Boolean,
            Self::Number(_) => ValueType::Number,
            Self::String(_) => ValueType::StringInline,
        }
    }

    /// Create a copy of this value.
    ///
    /// This is used instead of Clone to comply with project policy.
    #[must_use]
    pub fn clone_value(&self) -> Self {
        match self {
            Self::Null => Self::Null,
            Self::Boolean(b) => Self::Boolean(*b),
            Self::Number(n) => Self::Number(*n),
            Self::String(s) => Self::String(s.as_str().to_owned()),
        }
    }

    /// Calculate the serialized size of this value.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // String::len() is not const-stable
    pub fn serialized_size(&self) -> usize {
        match self {
            Self::Null => 1,                    // type only
            Self::Boolean(_) => 1 + 1,          // type + 1 byte
            Self::Number(_) => 1 + 8,           // type + f64
            Self::String(s) => 1 + 2 + s.len(), // type + len (2 bytes) + data
        }
    }

    /// Serialize this value to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.serialized_size());
        bytes.push(self.value_type() as u8);

        match self {
            Self::Null => {}
            Self::Boolean(b) => bytes.push(u8::from(*b)),
            Self::Number(n) => bytes.extend_from_slice(&n.to_le_bytes()),
            Self::String(s) => {
                #[allow(clippy::cast_possible_truncation)]
                let len = s.len() as u16;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(s.as_bytes());
            }
        }

        bytes
    }

    /// Deserialize a value from bytes.
    ///
    /// Returns the value and number of bytes consumed.
    pub fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), TripleError> {
        if bytes.is_empty() {
            return Err(TripleError::InvalidValue);
        }

        let value_type = ValueType::try_from(bytes[0]).map_err(|_| TripleError::InvalidValue)?;

        match value_type {
            ValueType::Null => Ok((Self::Null, 1)),
            ValueType::Boolean => {
                if bytes.len() < 2 {
                    return Err(TripleError::InvalidValue);
                }
                Ok((Self::Boolean(bytes[1] != 0), 2))
            }
            ValueType::Number => {
                if bytes.len() < 9 {
                    return Err(TripleError::InvalidValue);
                }
                let n = f64::from_le_bytes([
                    bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
                ]);
                Ok((Self::Number(n), 9))
            }
            ValueType::StringInline => {
                if bytes.len() < 3 {
                    return Err(TripleError::InvalidValue);
                }
                let len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
                if bytes.len() < 3 + len {
                    return Err(TripleError::InvalidValue);
                }
                let s = String::from_utf8(bytes[3..3 + len].to_vec())
                    .map_err(|_| TripleError::InvalidValue)?;
                Ok((Self::String(s), 3 + len))
            }
            ValueType::StringOverflow | ValueType::Date | ValueType::Blob => {
                Err(TripleError::UnsupportedValueType(value_type))
            }
        }
    }
}

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

/// Fixed size of triple metadata (without value).
/// `entity_id` (16) + `attribute_id` (16) + `created_txn` (8) + `deleted_txn` (8) + `created_hlc` (16) = 64
const TRIPLE_METADATA_SIZE: usize = 64;

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

    /// Calculate the serialized size of this record.
    #[must_use]
    pub fn serialized_size(&self) -> usize {
        TRIPLE_METADATA_SIZE + self.value.serialized_size()
    }

    /// Serialize this record to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.serialized_size());

        bytes.extend_from_slice(&self.entity_id);
        bytes.extend_from_slice(&self.attribute_id);
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

        let mut entity_id = [0u8; 16];
        entity_id.copy_from_slice(&bytes[0..16]);

        let mut attribute_id = [0u8; 16];
        attribute_id.copy_from_slice(&bytes[16..32]);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_null_roundtrip() {
        let value = TripleValue::Null;
        let bytes = value.to_bytes();
        let (decoded, consumed) = TripleValue::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_value_boolean_roundtrip() {
        for b in [true, false] {
            let value = TripleValue::Boolean(b);
            let bytes = value.to_bytes();
            let (decoded, consumed) = TripleValue::from_bytes(&bytes).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn test_value_number_roundtrip() {
        for n in [0.0, 1.0, -1.0, std::f64::consts::PI, f64::MAX, f64::MIN] {
            let value = TripleValue::Number(n);
            let bytes = value.to_bytes();
            let (decoded, consumed) = TripleValue::from_bytes(&bytes).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn test_value_string_roundtrip() {
        for s in ["", "hello", "hello world", "unicode: \u{1F600}"] {
            let value = TripleValue::String(s.to_string());
            let bytes = value.to_bytes();
            let (decoded, consumed) = TripleValue::from_bytes(&bytes).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn test_triple_record_roundtrip() {
        let record = TripleRecord::new(
            [1u8; 16],
            [2u8; 16],
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
            [1u8; 16],
            [2u8; 16],
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
            [0u8; 16],
            [0u8; 16],
            0,
            HlcTimestamp::new(0, 0),
            TripleValue::Null,
        );
        // 64 bytes metadata + 1 byte for null value
        assert_eq!(record.serialized_size(), 65);

        let record = TripleRecord::new(
            [0u8; 16],
            [0u8; 16],
            0,
            HlcTimestamp::new(0, 0),
            TripleValue::String("hello".to_string()),
        );
        // 64 bytes metadata + 1 type + 2 len + 5 data = 72
        assert_eq!(record.serialized_size(), 72);
    }
}
