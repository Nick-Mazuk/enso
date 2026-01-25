//! HLC timestamp proto conversion.
//!
//! Provides conversion between proto `HlcTimestamp` and storage `HlcTimestamp`.

use crate::proto;
use crate::storage::HlcTimestamp;
use crate::types::{ProtoDeserializable, ProtoSerializable};

impl ProtoDeserializable<proto::HlcTimestamp> for HlcTimestamp {
    fn from_proto(proto_hlc: proto::HlcTimestamp) -> Result<Self, String> {
        Ok(Self {
            physical_time: proto_hlc.physical_time_ms,
            logical_counter: proto_hlc.logical_counter,
            node_id: proto_hlc.node_id,
        })
    }
}

impl ProtoDeserializable<&proto::HlcTimestamp> for HlcTimestamp {
    fn from_proto(proto_hlc: &proto::HlcTimestamp) -> Result<Self, String> {
        Ok(Self {
            physical_time: proto_hlc.physical_time_ms,
            logical_counter: proto_hlc.logical_counter,
            node_id: proto_hlc.node_id,
        })
    }
}

impl ProtoSerializable<proto::HlcTimestamp> for HlcTimestamp {
    fn to_proto(self) -> proto::HlcTimestamp {
        proto::HlcTimestamp {
            physical_time_ms: self.physical_time,
            logical_counter: self.logical_counter,
            node_id: self.node_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_from_proto() {
        let proto_hlc = proto::HlcTimestamp {
            physical_time_ms: 1234,
            logical_counter: 5,
            node_id: 42,
        };
        let storage_hlc = HlcTimestamp::from_proto(proto_hlc).expect("conversion should succeed");
        assert_eq!(storage_hlc.physical_time, 1234);
        assert_eq!(storage_hlc.logical_counter, 5);
        assert_eq!(storage_hlc.node_id, 42);
    }

    #[test]
    fn test_hlc_from_proto_ref() {
        let proto_hlc = proto::HlcTimestamp {
            physical_time_ms: 1000,
            logical_counter: 10,
            node_id: 99,
        };
        let storage_hlc = HlcTimestamp::from_proto(&proto_hlc).expect("conversion should succeed");
        assert_eq!(storage_hlc.physical_time, 1000);
        assert_eq!(storage_hlc.logical_counter, 10);
        assert_eq!(storage_hlc.node_id, 99);
    }

    #[test]
    fn test_hlc_to_proto() {
        let storage_hlc = HlcTimestamp {
            physical_time: 5000,
            logical_counter: 3,
            node_id: 7,
        };
        let proto_hlc = storage_hlc.to_proto();
        assert_eq!(proto_hlc.physical_time_ms, 5000);
        assert_eq!(proto_hlc.logical_counter, 3);
        assert_eq!(proto_hlc.node_id, 7);
    }
}
