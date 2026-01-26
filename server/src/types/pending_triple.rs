//! Pending triple types for proto deserialization.
//!
//! Defines types used for ingesting data from protobuf messages
//! into the storage layer, minimizing intermediate allocations.

use crate::proto;
use crate::types::{
    AttributeId, EntityId, HlcTimestamp, ProtoDeserializable, TripleRecord, TripleValue,
};

const ID_LENGTH: usize = 16;

/// A pending triple operation to be committed.
///
/// This enum represents operations buffered in a `WalTransaction` before commit.
/// For Insert/Update, stores the complete `TripleRecord` to avoid reconstructing
/// it twice (once for WAL, once for index).
#[derive(Debug)]
pub enum PendingTriple {
    /// Insert a new triple.
    Insert(TripleRecord),
    /// Update an existing triple.
    Update(TripleRecord),
    /// Delete an existing triple.
    Delete {
        entity_id: EntityId,
        attribute_id: AttributeId,
    },
}

/// Raw triple data from proto, before Insert/Update determination.
///
/// This struct holds validated data extracted from a `proto::Triple` message.
/// It is used as an intermediate type before determining whether the operation
/// should be an Insert or Update (which requires a database lookup).
///
/// # Invariants
///
/// - `entity_id` is exactly 16 bytes
/// - `attribute_id` is exactly 16 bytes
/// - `value` is a valid, non-null value
/// - String values are non-empty and within `MAX_TRIPLE_STRING_VALUE_LENGTH`
#[derive(Debug)]
pub struct PendingTripleData {
    pub entity_id: EntityId,
    pub attribute_id: AttributeId,
    pub value: TripleValue,
    pub hlc: HlcTimestamp,
}

impl ProtoDeserializable<proto::Triple> for PendingTripleData {
    /// Deserialize a `PendingTripleData` from a proto `Triple`.
    ///
    /// This performs all validation previously done in `types::Triple::from_proto`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `entity_id` is missing or not exactly 16 bytes
    /// - `attribute_id` is missing or not exactly 16 bytes
    /// - `value` is missing, empty, or too long
    /// - `hlc` timestamp is missing
    fn from_proto(proto_triple: proto::Triple) -> Result<Self, String> {
        // Validate entity_id
        let entity_id = validate_proto_id(proto_triple.entity_id, "Triple", "subject")?;

        // Validate attribute_id
        let attribute_id = validate_proto_id(proto_triple.attribute_id, "Triple", "predicate")?;

        // Parse and validate value using storage::TripleValue's ProtoDeserializable
        let proto_value = proto_triple
            .value
            .ok_or("Triple proto did not contain a value.")?;
        let value = TripleValue::from_proto(proto_value)?;

        // Parse HLC timestamp
        let proto_hlc = proto_triple
            .hlc
            .ok_or("Triple proto did not contain an hlc timestamp.")?;
        let hlc = HlcTimestamp {
            physical_time: proto_hlc.physical_time_ms,
            logical_counter: proto_hlc.logical_counter,
            node_id: proto_hlc.node_id,
        };

        Ok(Self {
            entity_id,
            attribute_id,
            value,
            hlc,
        })
    }
}

/// Validate a proto ID field (`entity_id` or `attribute_id`).
///
/// # Pre-conditions
///
/// - `maybe_bytes` may be `None` (missing field)
///
/// # Post-conditions
///
/// - Returns `Ok` with a 16-byte array if valid
/// - Returns `Err` with descriptive message if invalid
///
/// # Errors
///
/// Returns an error if:
/// - The field is missing
/// - The field is not exactly 16 bytes
fn validate_proto_id(
    maybe_bytes: Option<Vec<u8>>,
    proto_name: &'static str,
    field_name: &'static str,
) -> Result<[u8; ID_LENGTH], String> {
    let bytes =
        maybe_bytes.ok_or_else(|| format!("{proto_name} proto did not contain a {field_name}"))?;

    let bytes_length = bytes.len();
    if bytes_length != ID_LENGTH {
        return Err(format!(
            "{proto_name} field {field_name} did not contain the correct number of bytes. Expected {ID_LENGTH}, got {bytes_length}"
        ));
    }

    bytes.try_into().map_err(|_| {
        format!(
            "{proto_name} field {field_name} did not contain the correct number of bytes. Expected {ID_LENGTH}, got {bytes_length}"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::MAX_TRIPLE_STRING_VALUE_LENGTH;

    fn make_test_triple(
        entity: [u8; 16],
        attr: [u8; 16],
        value: &str,
        hlc_time: u64,
    ) -> proto::Triple {
        proto::Triple {
            entity_id: Some(entity.to_vec()),
            attribute_id: Some(attr.to_vec()),
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String(value.to_string())),
            }),
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: hlc_time,
                logical_counter: 0,
                node_id: 1,
            }),
        }
    }

    #[test]
    fn test_pending_triple_data_valid() {
        let proto = make_test_triple([1u8; 16], [2u8; 16], "hello", 1000);
        let result = PendingTripleData::from_proto(proto);
        assert!(result.is_ok());

        let data = result.expect("should be ok");
        assert_eq!(data.entity_id, [1u8; 16]);
        assert_eq!(data.attribute_id, [2u8; 16]);
        assert_eq!(data.value, TripleValue::String("hello".to_string()));
        assert_eq!(data.hlc.physical_time, 1000);
    }

    #[test]
    fn test_pending_triple_data_missing_entity_id() {
        let proto = proto::Triple {
            entity_id: None,
            attribute_id: Some([2u8; 16].to_vec()),
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String("test".to_string())),
            }),
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
            }),
        };
        let result = PendingTripleData::from_proto(proto);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("subject"));
    }

    #[test]
    fn test_pending_triple_data_wrong_length_entity_id() {
        let proto = proto::Triple {
            entity_id: Some(vec![1, 2, 3]), // Only 3 bytes
            attribute_id: Some([2u8; 16].to_vec()),
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String("test".to_string())),
            }),
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
            }),
        };
        let result = PendingTripleData::from_proto(proto);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("16"));
    }

    #[test]
    fn test_pending_triple_data_missing_value() {
        let proto = proto::Triple {
            entity_id: Some([1u8; 16].to_vec()),
            attribute_id: Some([2u8; 16].to_vec()),
            value: None,
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
            }),
        };
        let result = PendingTripleData::from_proto(proto);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("value"));
    }

    #[test]
    fn test_pending_triple_data_empty_string_value() {
        let proto = make_test_triple([1u8; 16], [2u8; 16], "", 1000);
        let result = PendingTripleData::from_proto(proto);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty"));
    }

    #[test]
    fn test_pending_triple_data_string_too_long() {
        let long_string = "x".repeat(MAX_TRIPLE_STRING_VALUE_LENGTH + 1);
        let proto = make_test_triple([1u8; 16], [2u8; 16], &long_string, 1000);
        let result = PendingTripleData::from_proto(proto);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too long"));
    }
}
