use std::sync::Mutex;

use crate::{
    proto,
    query::value_to_storage,
    storage::{Database, TripleRecord},
    types::{
        ProtoDeserializable,
        client_message::{ClientMessage, ClientMessagePayload},
        triple_update_request::TripleUpdateRequest,
    },
};

pub struct ClientConnection {
    database: Mutex<Database>,
}

impl ClientConnection {
    #[must_use]
    pub const fn new(database: Database) -> Self {
        Self {
            database: Mutex::new(database),
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
                        ..Default::default()
                    }),
                };
            }
        };
        let mut response = match message.payload {
            ClientMessagePayload::TripleUpdateRequest(request) => self.update(request),
            ClientMessagePayload::Query(ref request) => self.query(request),
        };
        response.request_id = request_id;
        proto::ServerMessage {
            response: Some(response),
        }
    }

    fn update(&self, request: TripleUpdateRequest) -> proto::ServerResponse {
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

        // Lock the database for the duration of the transaction
        let Ok(mut db) = self.database.lock() else {
            return proto::ServerResponse {
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Internal.into(),
                    message: "Database lock poisoned".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            };
        };

        // Begin a transaction
        let mut txn = match db.begin() {
            Ok(txn) => txn,
            Err(e) => {
                return proto::ServerResponse {
                    status: Some(proto::google::rpc::Status {
                        code: proto::google::rpc::Code::Internal.into(),
                        message: format!("Failed to begin transaction: {e}"),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
            }
        };

        // Insert all triples
        for triple in triples {
            let value = value_to_storage(triple.value);
            txn.insert(triple.entity_id, triple.attribute_id, value);
        }

        // Commit the transaction
        if let Err(e) = txn.commit() {
            return proto::ServerResponse {
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Internal.into(),
                    message: format!("Failed to commit transaction: {e}"),
                    ..Default::default()
                }),
                ..Default::default()
            };
        }

        proto::ServerResponse {
            status: Some(proto::google::rpc::Status {
                code: proto::google::rpc::Code::Ok.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[allow(clippy::too_many_lines)]
    fn query(&self, request: &proto::QueryRequest) -> proto::ServerResponse {
        // Lock the database
        let Ok(mut db) = self.database.lock() else {
            return proto::ServerResponse {
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Internal.into(),
                    message: "Database lock poisoned".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            };
        };

        // Begin a read-only transaction
        let mut txn = match db.begin() {
            Ok(txn) => txn,
            Err(e) => {
                return proto::ServerResponse {
                    status: Some(proto::google::rpc::Status {
                        code: proto::google::rpc::Code::Internal.into(),
                        message: format!("Failed to begin transaction: {e}"),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
            }
        };

        // Process the query patterns
        let mut results = Vec::new();

        for pattern in &request.r#where {
            // Extract entity_id if specified
            let entity_id: Option<[u8; 16]> = match &pattern.entity {
                Some(proto::query_pattern::Entity::EntityId(bytes)) => {
                    if bytes.len() == 16 {
                        let mut arr = [0u8; 16];
                        arr.copy_from_slice(bytes);
                        Some(arr)
                    } else {
                        txn.abort();
                        return proto::ServerResponse {
                            status: Some(proto::google::rpc::Status {
                                code: proto::google::rpc::Code::InvalidArgument.into(),
                                message: "entity_id must be exactly 16 bytes".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                    }
                }
                _ => None,
            };

            // Extract attribute_id if specified
            let attribute_id: Option<[u8; 16]> = match &pattern.attribute {
                Some(proto::query_pattern::Attribute::AttributeId(bytes)) => {
                    if bytes.len() == 16 {
                        let mut arr = [0u8; 16];
                        arr.copy_from_slice(bytes);
                        Some(arr)
                    } else {
                        txn.abort();
                        return proto::ServerResponse {
                            status: Some(proto::google::rpc::Status {
                                code: proto::google::rpc::Code::InvalidArgument.into(),
                                message: "attribute_id must be exactly 16 bytes".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                    }
                }
                _ => None,
            };

            // Execute the appropriate query based on what's specified
            match (entity_id, attribute_id) {
                // Point lookup: both entity_id and attribute_id specified
                (Some(eid), Some(aid)) => {
                    match txn.get(&eid, &aid) {
                        Ok(Some(record)) => {
                            results.push(record_to_proto(&record));
                        }
                        Ok(None) => {
                            // No match, continue
                        }
                        Err(e) => {
                            txn.abort();
                            return proto::ServerResponse {
                                status: Some(proto::google::rpc::Status {
                                    code: proto::google::rpc::Code::Internal.into(),
                                    message: format!("Query failed: {e}"),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            };
                        }
                    }
                }
                // Entity scan: only entity_id specified
                (Some(eid), None) => match txn.scan_entity(&eid) {
                    Ok(records) => {
                        for record in &records {
                            results.push(record_to_proto(record));
                        }
                    }
                    Err(e) => {
                        txn.abort();
                        return proto::ServerResponse {
                            status: Some(proto::google::rpc::Status {
                                code: proto::google::rpc::Code::Internal.into(),
                                message: format!("Query failed: {e}"),
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                    }
                },
                // No entity_id specified - not supported yet
                (None, _) => {
                    txn.abort();
                    return proto::ServerResponse {
                        status: Some(proto::google::rpc::Status {
                            code: proto::google::rpc::Code::Unimplemented.into(),
                            message: "Queries without entity_id are not yet supported".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                }
            }
        }

        txn.abort(); // Read-only, nothing to commit

        proto::ServerResponse {
            status: Some(proto::google::rpc::Status {
                code: proto::google::rpc::Code::Ok.into(),
                ..Default::default()
            }),
            triples: results,
            ..Default::default()
        }
    }
}

/// Convert a storage `TripleRecord` to a proto `Triple`.
fn record_to_proto(record: &TripleRecord) -> proto::Triple {
    proto::Triple {
        entity_id: Some(record.entity_id.to_vec()),
        attribute_id: Some(record.attribute_id.to_vec()),
        value: Some(storage_value_to_proto(&record.value)),
    }
}

/// Convert a storage `TripleValue` to a proto `TripleValue`.
fn storage_value_to_proto(value: &crate::storage::TripleValue) -> proto::TripleValue {
    proto::TripleValue {
        value: match value {
            crate::storage::TripleValue::Null => None,
            crate::storage::TripleValue::Boolean(b) => {
                Some(proto::triple_value::Value::Boolean(*b))
            }
            crate::storage::TripleValue::Number(n) => Some(proto::triple_value::Value::Number(*n)),
            crate::storage::TripleValue::String(s) => {
                Some(proto::triple_value::Value::String(s.to_string()))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto;
    use crate::testing::new_test_database;

    #[tokio::test]
    async fn test_handle_message_insert_string_triple() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        let entity_id = vec![1u8; 16];
        let attribute_id = vec![2u8; 16];

        let triple = proto::Triple {
            entity_id: Some(entity_id.clone()),
            attribute_id: Some(attribute_id.clone()),
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String("test_value".to_string())),
            }),
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

        // Verify the triple was inserted by reading it back

        let mut db = client_conn.database.lock().unwrap();
        let mut txn = db.begin().expect("begin txn");
        let entity_arr: [u8; 16] = entity_id.try_into().unwrap();
        let attr_arr: [u8; 16] = attribute_id.try_into().unwrap();
        let record = txn.get(&entity_arr, &attr_arr).expect("get");
        assert!(record.is_some());
        assert_eq!(
            record.unwrap().value,
            crate::storage::TripleValue::String("test_value".to_string())
        );
        txn.abort();
    }

    #[tokio::test]
    async fn test_handle_message_insert_boolean_triple() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        let entity_id = vec![3u8; 16];
        let attribute_id = vec![4u8; 16];

        let triple = proto::Triple {
            entity_id: Some(entity_id),
            attribute_id: Some(attribute_id),
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Boolean(true)),
            }),
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
    }

    #[tokio::test]
    async fn test_handle_message_insert_number_triple() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        let entity_id = vec![5u8; 16];
        let attribute_id = vec![6u8; 16];

        let triple = proto::Triple {
            entity_id: Some(entity_id),
            attribute_id: Some(attribute_id),
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Number(123.456)),
            }),
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
    }

    #[tokio::test]
    async fn test_handle_message_empty_triples() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

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

    #[tokio::test]
    async fn test_insert_then_query_triple() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        let entity_id = vec![10u8; 16];
        let attribute_id = vec![20u8; 16];

        // Insert a triple
        let triple = proto::Triple {
            entity_id: Some(entity_id.clone()),
            attribute_id: Some(attribute_id.clone()),
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String("query_test".to_string())),
            }),
        };

        let update_request = proto::TripleUpdateRequest {
            triples: vec![triple],
        };

        let insert_message = proto::ClientMessage {
            request_id: Some(200),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let insert_response = client_conn.handle_message(insert_message).await;
        assert_eq!(
            insert_response.response.unwrap().status.unwrap().code,
            proto::google::rpc::Code::Ok as i32
        );

        // Query the triple back using point lookup (entity_id + attribute_id)
        let query_pattern = proto::QueryPattern {
            #[allow(clippy::disallowed_methods)]
            entity: Some(proto::query_pattern::Entity::EntityId(entity_id.clone())),
            #[allow(clippy::disallowed_methods)]
            attribute: Some(proto::query_pattern::Attribute::AttributeId(
                attribute_id.clone(),
            )),
            value_group: None,
        };

        let query_request = proto::QueryRequest {
            find: vec![],
            r#where: vec![query_pattern],
        };

        let query_message = proto::ClientMessage {
            request_id: Some(201),
            payload: Some(proto::client_message::Payload::Query(query_request)),
        };

        let query_response = client_conn.handle_message(query_message).await;
        let server_response = query_response.response.unwrap();

        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::Ok as i32
        );
        assert_eq!(server_response.triples.len(), 1);

        let result_triple = &server_response.triples[0];
        assert_eq!(result_triple.entity_id, Some(entity_id));
        assert_eq!(result_triple.attribute_id, Some(attribute_id));
        assert_eq!(
            result_triple.value,
            Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String("query_test".to_string())),
            })
        );
    }

    #[tokio::test]
    async fn test_query_entity_scan() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        let entity_id = vec![30u8; 16];

        // Insert multiple triples for the same entity
        let mut triples = Vec::new();
        for i in 0..3u8 {
            let mut attr = [0u8; 16];
            attr[0] = i;
            triples.push(proto::Triple {
                entity_id: Some(entity_id.clone()),
                attribute_id: Some(attr.to_vec()),
                value: Some(proto::TripleValue {
                    value: Some(proto::triple_value::Value::Number(f64::from(i))),
                }),
            });
        }

        let update_request = proto::TripleUpdateRequest { triples };

        let insert_message = proto::ClientMessage {
            request_id: Some(300),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let insert_response = client_conn.handle_message(insert_message).await;
        assert_eq!(
            insert_response.response.unwrap().status.unwrap().code,
            proto::google::rpc::Code::Ok as i32
        );

        // Query all triples for the entity (entity scan)
        let query_pattern = proto::QueryPattern {
            entity: Some(proto::query_pattern::Entity::EntityId(entity_id)),
            attribute: None,
            value_group: None,
        };

        let query_request = proto::QueryRequest {
            find: vec![],
            r#where: vec![query_pattern],
        };

        let query_message = proto::ClientMessage {
            request_id: Some(301),
            payload: Some(proto::client_message::Payload::Query(query_request)),
        };

        let query_response = client_conn.handle_message(query_message).await;
        let server_response = query_response.response.unwrap();

        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::Ok as i32
        );
        assert_eq!(server_response.triples.len(), 3);
    }
}
