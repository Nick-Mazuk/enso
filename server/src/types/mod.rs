pub mod client_message;
pub mod triple;
pub mod triple_update_request;

pub trait ProtoDeserializable<T> {
    fn from_proto(proto_obj: T) -> Result<Self, String>
    where
        Self: Sized;
}

pub trait ProtoSerializable<T> {
    fn to_proto(self) -> T;
}
