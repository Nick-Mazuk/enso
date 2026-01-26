//! Hybrid Logical Clock timestamp type and proto conversion.
//!
//! Provides `HlcTimestamp` struct and conversion between proto `HlcTimestamp`
//! and the Rust type.

use crate::proto;
use crate::types::{ProtoDeserializable, ProtoSerializable};

/// Hybrid Logical Clock timestamp (16 bytes).
///
/// Layout:
/// - `physical_time`: 8 bytes (nanoseconds since Unix epoch)
/// - `logical_counter`: 4 bytes
/// - `node_id`: 4 bytes
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct HlcTimestamp {
    /// Physical time in nanoseconds since Unix epoch.
    pub physical_time: u64,
    /// Logical counter for ordering events at the same physical time.
    pub logical_counter: u32,
    /// Node identifier for distributed uniqueness.
    pub node_id: u32,
}

impl HlcTimestamp {
    /// Size in bytes.
    pub const SIZE: usize = 16;

    /// Create a new HLC timestamp.
    #[must_use]
    pub const fn new(physical_time: u64, logical_counter: u32) -> Self {
        Self {
            physical_time,
            logical_counter,
            node_id: 0,
        }
    }

    /// Serialize to bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; Self::SIZE] {
        let phys = self.physical_time.to_le_bytes();
        let logic = self.logical_counter.to_le_bytes();
        let node = self.node_id.to_le_bytes();
        [
            phys[0], phys[1], phys[2], phys[3], phys[4], phys[5], phys[6], phys[7], logic[0],
            logic[1], logic[2], logic[3], node[0], node[1], node[2], node[3],
        ]
    }

    /// Deserialize from bytes.
    #[must_use]
    pub const fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        Self {
            physical_time: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            logical_counter: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            node_id: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        }
    }
}

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

    #[test]
    fn test_hlc_roundtrip() {
        let hlc = HlcTimestamp {
            physical_time: 0x0102_0304_0506_0708,
            logical_counter: 0x090A_0B0C,
            node_id: 0x0D0E_0F10,
        };

        let bytes = hlc.to_bytes();
        let restored = HlcTimestamp::from_bytes(&bytes);

        assert_eq!(restored, hlc);
    }
}
