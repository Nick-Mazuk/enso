use crate::{
    proto,
    types::{ProtoDeserializable, triple_update_request::TripleUpdateRequest},
};

#[derive(Debug)]
pub enum ClientMessagePayload {
    TripleUpdateRequest(TripleUpdateRequest),
    Query(proto::QueryRequest),
    Subscribe(proto::SubscribeRequest),
    Unsubscribe(proto::UnsubscribeRequest),
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
            Some(proto::client_message::Payload::Query(request)) => {
                ClientMessagePayload::Query(request)
            }
            Some(proto::client_message::Payload::Subscribe(request)) => {
                ClientMessagePayload::Subscribe(request)
            }
            Some(proto::client_message::Payload::Unsubscribe(request)) => {
                ClientMessagePayload::Unsubscribe(request)
            }
            None => return Err("Client message must have a payload".to_string()),
        };
        Ok(Self { payload })
    }
}
