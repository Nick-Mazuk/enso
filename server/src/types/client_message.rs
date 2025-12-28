use crate::{
    proto,
    types::{ProtoDeserializable, triple_update_request::TripleUpdateRequest},
};

#[derive(Debug)]
pub enum ClientMessagePayload {
    TripleUpdateRequest(TripleUpdateRequest),
}

#[derive(Debug)]
pub struct ClientMessage {
    pub payload: ClientMessagePayload,
}

impl ProtoDeserializable<proto::ClientMessage> for ClientMessage {
    fn from_proto(proto_message: proto::ClientMessage) -> Result<Self, String> {
        if proto_message.request_id.is_none() {
            return Err("Client message must have a request_id".to_string());
        }
        let payload = match proto_message.payload {
            Some(proto::client_message::Payload::TripleUpdateRequest(request)) => {
                ClientMessagePayload::TripleUpdateRequest(TripleUpdateRequest::from_proto(request)?)
            }
            None => return Err("Client message must have a payload".to_string()),
        };
        Ok(Self { payload })
    }
}
