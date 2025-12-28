use crate::{
    proto,
    types::{
        ProtoDeserializable,
        client_message::{ClientMessage, ClientMessagePayload},
        triple::TripleValue,
        triple_update_request::TripleUpdateRequest,
    },
};

pub struct ClientConnection {
    database_connection: turso::Connection,
}

impl ClientConnection {
    pub const fn new(database_connection: turso::Connection) -> Self {
        Self {
            database_connection,
        }
    }

    pub async fn handle_message(
        &self,
        proto_message: proto::ClientMessage,
    ) -> proto::ServerMessage {
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
        let mut response = match message.payload {
            ClientMessagePayload::TripleUpdateRequest(request) => self.update(request).await,
        };
        response.request_id = request_id;
        proto::ServerMessage {
            response: Some(response),
        }
    }

    async fn update(&self, request: TripleUpdateRequest) -> proto::ServerResponse {
        let triples = request.triples;
        if triples.is_empty() {
            return proto::ServerResponse {
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Ok.into(),
                    ..Default::default()
                }),
                ..Default::default()
            };
        }

        let base_insert_query = "INSERT INTO triples (entity_id, attribute_id, number_value, string_value, boolean_value) VALUES ";
        let query_params = "(?, ?, ?, ?, ?)";
        let mut query =
            String::with_capacity(base_insert_query.len() + triples.len() * query_params.len());
        query.push_str(base_insert_query);
        let mut params = Vec::with_capacity(triples.len() * 5);

        for (i, triple) in triples.into_iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(query_params);

            params.push(turso::Value::Blob(triple.entity_id.to_vec()));
            params.push(turso::Value::Blob(triple.attribute_id.to_vec()));

            match triple.value {
                TripleValue::Number(n) => {
                    params.push(turso::Value::Real(n));
                    params.push(turso::Value::Null);
                    params.push(turso::Value::Null);
                }
                TripleValue::String(s) => {
                    params.push(turso::Value::Null);
                    params.push(turso::Value::Text(s));
                    params.push(turso::Value::Null);
                }
                TripleValue::Boolean(b) => {
                    params.push(turso::Value::Null);
                    params.push(turso::Value::Null);
                    params.push(turso::Value::Integer(b.into()));
                }
            }
        }

        match self.database_connection.execute(&query, params).await {
            Ok(_) => proto::ServerResponse {
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Ok.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Err(e) => proto::ServerResponse {
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Internal.into(),
                    // TODO: do not expose database error
                    message: format!("Database error: {e}"),
                    ..Default::default()
                }),
                ..Default::default()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto;
    use crate::testing::new_test_database_connection;

    #[tokio::test]
    async fn test_handle_message_insert_string_triple() {
        let database_connection = new_test_database_connection()
            .await
            .expect("Failed to create test db");
        let client_conn = ClientConnection::new(database_connection);

        let entity_id = vec![1u8; 16];
        let attribute_id = vec![2u8; 16];

        let triple = proto::Triple {
            entity_id: Some(entity_id),
            attribute_id: Some(attribute_id),
            value: Some(proto::triple::Value::String("test_value".to_string())),
        };

        let update_request = proto::TripleUpdateRequest {
            triples: vec![triple],
        };

        let client_message = proto::ClientMessage {
            request_id: Some(123),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let response = client_conn.handle_message(client_message).await;

        assert!(response.response.is_some());
        let server_response = response.response.unwrap();
        assert_eq!(server_response.request_id, Some(123));
        assert!(server_response.status.is_some());
        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::Ok as i32
        );

        // TODO: run a query to verify the triple was inserted
    }

    #[tokio::test]
    async fn test_handle_message_insert_boolean_triple() {
        let database_connection = new_test_database_connection()
            .await
            .expect("Failed to create test db");
        let client_conn = ClientConnection::new(database_connection);

        let entity_id = vec![3u8; 16];
        let attribute_id = vec![4u8; 16];

        let triple = proto::Triple {
            entity_id: Some(entity_id),
            attribute_id: Some(attribute_id),
            value: Some(proto::triple::Value::Boolean(true)),
        };

        let update_request = proto::TripleUpdateRequest {
            triples: vec![triple],
        };

        let client_message = proto::ClientMessage {
            request_id: Some(124),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let response = client_conn.handle_message(client_message).await;

        let server_response = response.response.unwrap();
        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::Ok as i32
        );

        // TODO: run a query to verify the triple was inserted
    }

    #[tokio::test]
    async fn test_handle_message_insert_number_triple() {
        let database_connection = new_test_database_connection()
            .await
            .expect("Failed to create test db");
        let client_conn = ClientConnection::new(database_connection);

        let entity_id = vec![5u8; 16];
        let attribute_id = vec![6u8; 16];

        let triple = proto::Triple {
            entity_id: Some(entity_id),
            attribute_id: Some(attribute_id),
            value: Some(proto::triple::Value::Number(123.456)),
        };

        let update_request = proto::TripleUpdateRequest {
            triples: vec![triple],
        };

        let client_message = proto::ClientMessage {
            request_id: Some(125),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let response = client_conn.handle_message(client_message).await;

        let server_response = response.response.unwrap();
        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::Ok as i32
        );

        // TODO: run a query to verify the triple was inserted
    }

    #[tokio::test]
    async fn test_handle_message_empty_triples() {
        let database_connection = new_test_database_connection()
            .await
            .expect("Failed to create test db");
        let client_conn = ClientConnection::new(database_connection);

        let update_request = proto::TripleUpdateRequest { triples: vec![] };

        let client_message = proto::ClientMessage {
            request_id: Some(126),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let response = client_conn.handle_message(client_message).await;

        let server_response = response.response.unwrap();
        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::Ok as i32
        );
    }
}
