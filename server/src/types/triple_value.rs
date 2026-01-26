//! Triple value types and proto conversion.
//!
//! Provides `TripleValue` enum and `ValueType` discriminant, along with
//! serialization, deserialization, and proto conversion implementations.

use crate::constants::MAX_TRIPLE_STRING_VALUE_LENGTH;
use crate::proto;
use crate::types::ids::EntityId;
use crate::types::{ProtoDeserializable, ProtoSerializable};

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
    Ref = 0x08,            // Reference to another entity
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
            0x08 => Ok(Self::Ref),
            _ => Err(value),
        }
    }
}

/// A triple value.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::disallowed_methods)] // Clone needed for broadcast channel
pub enum TripleValue {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
    /// Reference to another entity.
    Ref(EntityId),
}

/// Errors that can occur with triple value operations.
#[derive(Debug)]
pub enum TripleValueError {
    /// Invalid value format.
    InvalidValue,
    /// Unsupported value type.
    UnsupportedValueType(ValueType),
}

impl std::fmt::Display for TripleValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidValue => write!(f, "invalid value format"),
            Self::UnsupportedValueType(t) => write!(f, "unsupported value type: {t:?}"),
        }
    }
}

impl std::error::Error for TripleValueError {}

impl std::fmt::Display for TripleValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Number(n) => write!(f, "{n}"),
            Self::String(s) => write!(f, "\"{s}\""),
            Self::Ref(id) => write!(f, "#{id}"),
        }
    }
}

impl TripleValue {
    /// Create a string value.
    #[must_use]
    pub fn string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }

    /// Create a number value.
    #[must_use]
    pub fn number(n: impl Into<f64>) -> Self {
        Self::Number(n.into())
    }

    /// Create a boolean value.
    #[must_use]
    pub const fn boolean(b: bool) -> Self {
        Self::Boolean(b)
    }

    /// Create a reference value.
    #[must_use]
    pub const fn reference(id: EntityId) -> Self {
        Self::Ref(id)
    }

    /// Get the value type discriminant.
    #[must_use]
    pub const fn value_type(&self) -> ValueType {
        match self {
            Self::Null => ValueType::Null,
            Self::Boolean(_) => ValueType::Boolean,
            Self::Number(_) => ValueType::Number,
            Self::String(_) => ValueType::StringInline,
            Self::Ref(_) => ValueType::Ref,
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
            Self::Ref(id) => Self::Ref(*id),
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
            Self::Ref(_) => 1 + 16,             // type + entity ID (16 bytes)
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
            Self::Ref(id) => bytes.extend_from_slice(&id.0),
        }

        bytes
    }

    /// Deserialize a value from bytes.
    ///
    /// Returns the value and number of bytes consumed.
    pub fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), TripleValueError> {
        if bytes.is_empty() {
            return Err(TripleValueError::InvalidValue);
        }

        let value_type =
            ValueType::try_from(bytes[0]).map_err(|_| TripleValueError::InvalidValue)?;

        match value_type {
            ValueType::Null => Ok((Self::Null, 1)),
            ValueType::Boolean => {
                if bytes.len() < 2 {
                    return Err(TripleValueError::InvalidValue);
                }
                Ok((Self::Boolean(bytes[1] != 0), 2))
            }
            ValueType::Number => {
                if bytes.len() < 9 {
                    return Err(TripleValueError::InvalidValue);
                }
                let n = f64::from_le_bytes([
                    bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
                ]);
                Ok((Self::Number(n), 9))
            }
            ValueType::StringInline => {
                if bytes.len() < 3 {
                    return Err(TripleValueError::InvalidValue);
                }
                let len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
                if bytes.len() < 3 + len {
                    return Err(TripleValueError::InvalidValue);
                }
                let s = String::from_utf8(bytes[3..3 + len].to_vec())
                    .map_err(|_| TripleValueError::InvalidValue)?;
                Ok((Self::String(s), 3 + len))
            }
            ValueType::Ref => {
                if bytes.len() < 17 {
                    return Err(TripleValueError::InvalidValue);
                }
                let mut id_bytes = [0u8; 16];
                id_bytes.copy_from_slice(&bytes[1..17]);
                Ok((Self::Ref(EntityId(id_bytes)), 17))
            }
            ValueType::StringOverflow | ValueType::Date | ValueType::Blob => {
                Err(TripleValueError::UnsupportedValueType(value_type))
            }
        }
    }
}

impl ProtoDeserializable<proto::TripleValue> for TripleValue {
    /// Deserialize a `TripleValue` from a proto `TripleValue`.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The proto value is missing (None)
    /// - A string value is empty
    /// - A string value exceeds `MAX_TRIPLE_STRING_VALUE_LENGTH`
    fn from_proto(proto_value: proto::TripleValue) -> Result<Self, String> {
        match proto_value.value {
            Some(proto::triple_value::Value::String(s)) => {
                if s.is_empty() {
                    return Err("Triple string value was empty".into());
                }
                if s.len() > MAX_TRIPLE_STRING_VALUE_LENGTH {
                    return Err(format!(
                        "Triple string value too long. Max: {MAX_TRIPLE_STRING_VALUE_LENGTH}, got: {}",
                        s.len()
                    ));
                }
                Ok(Self::String(s))
            }
            Some(proto::triple_value::Value::Boolean(b)) => Ok(Self::Boolean(b)),
            Some(proto::triple_value::Value::Number(n)) => Ok(Self::Number(n)),
            None => Err("Triple proto did not contain a value".into()),
        }
    }
}

impl ProtoSerializable<Option<proto::TripleValue>> for TripleValue {
    fn to_proto(self) -> Option<proto::TripleValue> {
        match self {
            Self::Null => None,
            Self::Boolean(b) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Boolean(b)),
            }),
            Self::Number(n) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Number(n)),
            }),
            Self::String(s) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String(s)),
            }),
            Self::Ref(id) => {
                // Serialize Ref as a string representation of the entity ID.
                // Try UTF-8 first, fall back to hex encoding.
                let s = std::str::from_utf8(&id.0).map_or_else(
                    |_| {
                        use std::fmt::Write;
                        id.0.iter().fold(String::with_capacity(32), |mut acc, b| {
                            let _ = write!(acc, "{b:02x}");
                            acc
                        })
                    },
                    |s| s.trim_end_matches('\0').to_owned(),
                );
                Some(proto::TripleValue {
                    value: Some(proto::triple_value::Value::String(s)),
                })
            }
        }
    }
}

impl ProtoSerializable<Option<proto::TripleValue>> for &TripleValue {
    #[allow(clippy::disallowed_methods)] // Clone needed for String conversion
    fn to_proto(self) -> Option<proto::TripleValue> {
        match self {
            TripleValue::Null => None,
            TripleValue::Boolean(b) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Boolean(*b)),
            }),
            TripleValue::Number(n) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Number(*n)),
            }),
            TripleValue::String(s) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String(s.as_str().to_owned())),
            }),
            TripleValue::Ref(id) => {
                // Serialize Ref as a string representation of the entity ID.
                let s = std::str::from_utf8(&id.0).map_or_else(
                    |_| {
                        use std::fmt::Write;
                        id.0.iter().fold(String::with_capacity(32), |mut acc, b| {
                            let _ = write!(acc, "{b:02x}");
                            acc
                        })
                    },
                    |s| s.trim_end_matches('\0').to_owned(),
                );
                Some(proto::TripleValue {
                    value: Some(proto::triple_value::Value::String(s)),
                })
            }
        }
    }
}

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
    fn test_null_to_proto() {
        let value = TripleValue::Null;
        let proto_value: Option<proto::TripleValue> = value.to_proto();
        assert!(proto_value.is_none());
    }

    #[test]
    fn test_boolean_to_proto() {
        let value = TripleValue::Boolean(true);
        let proto_value: Option<proto::TripleValue> = value.to_proto();
        assert!(proto_value.is_some());
        match proto_value.expect("should be some").value {
            Some(proto::triple_value::Value::Boolean(b)) => assert!(b),
            _ => panic!("expected Boolean"),
        }
    }

    #[test]
    fn test_number_to_proto() {
        let value = TripleValue::Number(42.5);
        let proto_value: Option<proto::TripleValue> = value.to_proto();
        assert!(proto_value.is_some());
        match proto_value.expect("should be some").value {
            Some(proto::triple_value::Value::Number(n)) => {
                assert!((n - 42.5).abs() < f64::EPSILON);
            }
            _ => panic!("expected Number"),
        }
    }

    #[test]
    fn test_string_to_proto() {
        let value = TripleValue::String("hello".to_string());
        let proto_value: Option<proto::TripleValue> = value.to_proto();
        assert!(proto_value.is_some());
        match proto_value.expect("should be some").value {
            Some(proto::triple_value::Value::String(s)) => assert_eq!(s, "hello"),
            _ => panic!("expected String"),
        }
    }

    #[test]
    fn test_string_ref_to_proto() {
        let value = TripleValue::String("world".to_string());
        let proto_value: Option<proto::TripleValue> = (&value).to_proto();
        assert!(proto_value.is_some());
        match proto_value.expect("should be some").value {
            Some(proto::triple_value::Value::String(s)) => assert_eq!(s, "world"),
            _ => panic!("expected String"),
        }
        // Original still accessible
        assert!(matches!(value, TripleValue::String(_)));
    }

    #[test]
    fn test_value_ref_roundtrip() {
        let id = EntityId::from_string("test_entity");
        let value = TripleValue::Ref(id);
        let bytes = value.to_bytes();
        let (decoded, consumed) = TripleValue::from_bytes(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(consumed, 17); // 1 type byte + 16 entity ID bytes
        match decoded {
            TripleValue::Ref(decoded_id) => assert_eq!(decoded_id, id),
            _ => panic!("expected Ref"),
        }
    }

    #[test]
    fn test_ref_to_proto() {
        let id = EntityId::from_string("user123");
        let value = TripleValue::Ref(id);
        let proto_value: Option<proto::TripleValue> = value.to_proto();
        assert!(proto_value.is_some());
        match proto_value.expect("should be some").value {
            Some(proto::triple_value::Value::String(s)) => assert_eq!(s, "user123"),
            _ => panic!("expected String"),
        }
    }

    #[test]
    fn test_ref_serialized_size() {
        let id = EntityId::from_string("test");
        let value = TripleValue::Ref(id);
        assert_eq!(value.serialized_size(), 17);
    }
}
