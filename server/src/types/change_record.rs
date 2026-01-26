//! Change notification types and proto conversion.
//!
//! Provides `ChangeType`, `ChangeRecord`, and `ChangeNotification` for tracking
//! triple modifications, plus conversion to proto equivalents.

use crate::proto;
use crate::types::{AttributeId, EntityId, HlcTimestamp, ProtoSerializable, TripleValue};

// =============================================================================
// Change Notification Types
// =============================================================================

/// Type of change to a triple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    /// A new triple was created.
    Insert,
    /// An existing triple was modified with a newer HLC.
    Update,
    /// A triple was removed.
    Delete,
}

/// A record of a single triple change.
#[derive(Debug, Clone)]
#[allow(clippy::disallowed_methods)] // Clone needed for broadcast channel
pub struct ChangeRecord {
    /// The type of change.
    pub change_type: ChangeType,
    /// The entity ID of the affected triple.
    pub entity_id: EntityId,
    /// The attribute ID of the affected triple.
    pub attribute_id: AttributeId,
    /// The value of the triple. `None` for Delete operations.
    pub value: Option<TripleValue>,
    /// The HLC timestamp of the change.
    pub hlc: HlcTimestamp,
}

/// Unique identifier for a client connection.
pub type ConnectionId = u64;

/// Notification of changes, broadcast to all subscribers.
///
/// This is sent via the broadcast channel when triples are modified.
/// Subscribers receive this and can convert to protocol-specific formats.
#[derive(Debug, Clone)]
#[allow(clippy::disallowed_methods)] // Clone needed for broadcast channel
pub struct ChangeNotification {
    /// The connection that originated this change.
    /// Subscribers can use this to filter out their own writes.
    pub source_connection_id: ConnectionId,
    /// The changes that occurred in this transaction.
    pub changes: Vec<ChangeRecord>,
}

impl ProtoSerializable<i32> for ChangeType {
    fn to_proto(self) -> i32 {
        match self {
            Self::Insert => proto::ChangeType::Insert.into(),
            Self::Update => proto::ChangeType::Update.into(),
            Self::Delete => proto::ChangeType::Delete.into(),
        }
    }
}

impl ProtoSerializable<proto::ChangeRecord> for ChangeRecord {
    #[allow(clippy::disallowed_methods)] // Clone needed for String conversion
    fn to_proto(self) -> proto::ChangeRecord {
        let value = self.value.and_then(ProtoSerializable::to_proto);

        proto::ChangeRecord {
            change_type: self.change_type.to_proto(),
            triple: Some(proto::Triple {
                entity_id: Some(self.entity_id.to_vec()),
                attribute_id: Some(self.attribute_id.to_vec()),
                value,
                hlc: Some(self.hlc.to_proto()),
            }),
        }
    }
}

/// Convert a storage `ChangeRecord` reference to a proto `ChangeRecord`.
///
/// This is provided for cases where the caller cannot consume the original.
impl ProtoSerializable<proto::ChangeRecord> for &ChangeRecord {
    #[allow(clippy::disallowed_methods)] // Clone needed for String conversion
    fn to_proto(self) -> proto::ChangeRecord {
        let value = self.value.as_ref().and_then(ProtoSerializable::to_proto);

        proto::ChangeRecord {
            change_type: self.change_type.to_proto(),
            triple: Some(proto::Triple {
                entity_id: Some(self.entity_id.to_vec()),
                attribute_id: Some(self.attribute_id.to_vec()),
                value,
                hlc: Some(self.hlc.to_proto()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::HlcTimestamp;
    use crate::types::TripleValue;

    #[test]
    fn test_change_type_to_proto_insert() {
        let proto_type: i32 = ChangeType::Insert.to_proto();
        assert_eq!(proto_type, proto::ChangeType::Insert as i32);
    }

    #[test]
    fn test_change_type_to_proto_update() {
        let proto_type: i32 = ChangeType::Update.to_proto();
        assert_eq!(proto_type, proto::ChangeType::Update as i32);
    }

    #[test]
    fn test_change_type_to_proto_delete() {
        let proto_type: i32 = ChangeType::Delete.to_proto();
        assert_eq!(proto_type, proto::ChangeType::Delete as i32);
    }

    #[test]
    fn test_change_record_to_proto_insert() {
        let change = ChangeRecord {
            change_type: ChangeType::Insert,
            entity_id: [1u8; 16],
            attribute_id: [2u8; 16],
            value: Some(TripleValue::String("hello".to_string())),
            hlc: HlcTimestamp {
                physical_time: 1000,
                logical_counter: 1,
                node_id: 42,
            },
        };

        let proto_change = change.to_proto();
        assert_eq!(proto_change.change_type, proto::ChangeType::Insert as i32);

        let triple = proto_change.triple.expect("triple should be present");
        assert_eq!(triple.entity_id, Some(vec![1u8; 16]));
        assert_eq!(triple.attribute_id, Some(vec![2u8; 16]));

        let hlc = triple.hlc.expect("hlc should be present");
        assert_eq!(hlc.physical_time_ms, 1000);
        assert_eq!(hlc.logical_counter, 1);
        assert_eq!(hlc.node_id, 42);
    }

    #[test]
    fn test_change_record_to_proto_delete() {
        let change = ChangeRecord {
            change_type: ChangeType::Delete,
            entity_id: [1u8; 16],
            attribute_id: [2u8; 16],
            value: None,
            hlc: HlcTimestamp {
                physical_time: 1000,
                logical_counter: 1,
                node_id: 42,
            },
        };

        let proto_change = change.to_proto();
        assert_eq!(proto_change.change_type, proto::ChangeType::Delete as i32);

        let triple = proto_change.triple.expect("triple should be present");
        assert!(triple.value.is_none());
    }

    #[test]
    fn test_change_record_ref_to_proto() {
        let change = ChangeRecord {
            change_type: ChangeType::Update,
            entity_id: [3u8; 16],
            attribute_id: [4u8; 16],
            value: Some(TripleValue::Boolean(true)),
            hlc: HlcTimestamp {
                physical_time: 2000,
                logical_counter: 2,
                node_id: 99,
            },
        };

        let proto_change: proto::ChangeRecord = (&change).to_proto();
        assert_eq!(proto_change.change_type, proto::ChangeType::Update as i32);

        // Original still accessible
        assert_eq!(change.change_type, ChangeType::Update);
    }
}
