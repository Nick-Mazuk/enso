use crate::{
    proto::{self},
    types::{
        ProtoDeserializable,
        client_message::{ClientMessage, ClientMessagePayload},
        triple_update_request::TripleUpdateRequest,
    },
};

pub struct Server {}

impl Server {
    pub fn handle_message(&self, proto_message: proto::ClientMessage) -> proto::ServerMessage {
        let request_id = proto_message.request_id;
        let message = match ClientMessage::from_proto(proto_message) {
            Ok(message) => message,
            Err(err) => {
                return proto::ServerMessage {
                    response: Some(proto::ServerResponse {
                        request_id,
                        status: Some(proto::google::rpc::Status {
                            code: proto::google::rpc::Code::InvalidArgument.into(),
                            message: err,
                            ..Default::default()
                        }),
                    }),
                };
            }
        };
        match message.payload {
            ClientMessagePayload::TripleUpdateRequest(request) => self.update(request),
        }
        proto::ServerMessage {
            response: Some(proto::ServerResponse {
                request_id,
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Ok.into(),
                    ..Default::default()
                }),
            }),
        }
    }

    fn update(&self, _request: TripleUpdateRequest) {
        // convert / validate triple proto into rust struct
        // save it to the database
        // return ack
    }
}
