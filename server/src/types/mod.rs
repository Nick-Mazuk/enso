pub mod change_record;
pub mod client_message;
pub mod hlc;
pub mod ids;
pub mod pending_triple;
pub mod query;
pub mod triple_record;
pub mod triple_update_request;
pub mod triple_value;

pub use change_record::{ChangeNotification, ChangeRecord, ChangeType, ConnectionId};
pub use hlc::HlcTimestamp;
pub use ids::{AttributeId, EntityId};
pub use pending_triple::{PendingTriple, PendingTripleData};
pub use triple_record::{TripleError, TripleRecord, TxnId};
pub use triple_value::{TripleValue, TripleValueError, ValueType};

pub trait ProtoDeserializable<T> {
    fn from_proto(proto_obj: T) -> Result<Self, String>
    where
        Self: Sized;
}

#[allow(dead_code)]
pub trait ProtoSerializable<T> {
    fn to_proto(self) -> T;
}
