use std::cmp::Ordering;
use std::sync::Mutex;

use crate::{
    proto,
    query::{Query, QueryEngine, value_from_storage, value_to_storage},
    storage::{Database, HlcClock, SystemTimeSource},
    types::{
        ProtoDeserializable, ProtoSerializable,
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

    #[allow(clippy::too_many_lines)]
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

        // First, read existing values to compare HLCs
        let mut snapshot = db.begin_readonly();
        let mut updates_to_apply = Vec::with_capacity(triples.len());

        for triple in &triples {
            let existing = snapshot.get(&triple.entity_id, &triple.attribute_id);
            let should_update = match existing {
                Ok(Some(record)) => {
                    // Update only if client HLC is strictly newer than stored HLC
                    HlcClock::<SystemTimeSource>::compare(triple.hlc, record.created_hlc)
                        == Ordering::Greater
                }
                // No existing value or error reading - always insert
                Ok(None) | Err(_) => true,
            };
            updates_to_apply.push((triple, should_update));
        }

        let txn_id = snapshot.close();
        db.release_snapshot(txn_id);

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

        // Track the keys we're processing so we can return current values
        let keys: Vec<([u8; 16], [u8; 16])> = triples
            .iter()
            .map(|t| (t.entity_id, t.attribute_id))
            .collect();

        // Insert only triples where client HLC is newer
        for (triple, should_update) in updates_to_apply {
            if should_update {
                let value = value_to_storage(triple.value.clone_value());
                txn.insert_with_hlc(triple.entity_id, triple.attribute_id, value, triple.hlc);
            }
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

        // Read back the current values and return them in the response
        let mut response_triples = Vec::with_capacity(keys.len());

        // Begin a read-only snapshot to get current values
        let mut snapshot = db.begin_readonly();

        for (entity_id, attribute_id) in keys {
            if let Ok(Some(record)) = snapshot.get(&entity_id, &attribute_id) {
                let types_value = value_from_storage(record.value);
                response_triples.push(proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(match types_value {
                            crate::types::triple::TripleValue::String(s) => {
                                proto::triple_value::Value::String(s)
                            }
                            crate::types::triple::TripleValue::Number(n) => {
                                proto::triple_value::Value::Number(n)
                            }
                            crate::types::triple::TripleValue::Boolean(b) => {
                                proto::triple_value::Value::Boolean(b)
                            }
                        }),
                    }),
                    hlc: Some(proto::HlcTimestamp {
                        physical_time_ms: record.created_hlc.physical_time,
                        logical_counter: record.created_hlc.logical_counter,
                        node_id: record.created_hlc.node_id,
                    }),
                });
            }
        }

        let txn_id = snapshot.close();
        db.release_snapshot(txn_id);

        proto::ServerResponse {
            status: Some(proto::google::rpc::Status {
                code: proto::google::rpc::Code::Ok.into(),
                ..Default::default()
            }),
            triples: response_triples,
            ..Default::default()
        }
    }

    fn query(&self, request: &proto::QueryRequest) -> proto::ServerResponse {
        // Lock the database
        let Ok(mut db) = self.database.lock() else {
            return proto::ServerResponse {
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Internal.into(),
                    message: "Database lock poisoned".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            };
        };

        // Convert proto request to internal query using the trait
        let query = match Query::from_proto(request) {
            Ok(q) => q,
            Err(e) => {
                return proto::ServerResponse {
                    status: Some(proto::google::rpc::Status {
                        code: proto::google::rpc::Code::InvalidArgument.into(),
                        message: e,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
            }
        };

        // Begin a read-only snapshot
        let mut snapshot = db.begin_readonly();

        // Execute the query
        let result = {
            let mut engine = QueryEngine::new(&mut snapshot);
            engine.execute(&query)
        };

        // Close the snapshot and release it
        let txn_id = snapshot.close();
        db.release_snapshot(txn_id);

        // Handle the result
        match result {
            Ok(query_result) => {
                let response = query_result.to_proto();
                proto::ServerResponse {
                    status: Some(proto::google::rpc::Status {
                        code: proto::google::rpc::Code::Ok.into(),
                        ..Default::default()
                    }),
                    columns: response.columns,
                    rows: response.rows,
                    ..Default::default()
                }
            }
            Err(e) => proto::ServerResponse {
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Internal.into(),
                    message: format!("Query failed: {e}"),
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
    use crate::testing::new_test_database;

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
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
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
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
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
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
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
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
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
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

        // Query the triple back using point lookup (entity_id + attribute_id) with variable for value
        let query_request = proto::QueryRequest {
            find: vec![proto::QueryPatternVariable {
                label: Some("value".to_owned()),
            }],
            r#where: vec![proto::QueryPattern {
                #[allow(clippy::disallowed_methods)]
                entity: Some(proto::query_pattern::Entity::EntityId(entity_id.clone())),
                #[allow(clippy::disallowed_methods)]
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    attribute_id.clone(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("value".to_owned()),
                    },
                )),
            }],
            optional: vec![],
            where_not: vec![],
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
        assert_eq!(server_response.columns, vec!["value"]);
        assert_eq!(server_response.rows.len(), 1);

        // Check the value in the first row
        let row = &server_response.rows[0];
        assert_eq!(row.values.len(), 1);
        let result_value = &row.values[0];
        assert!(!result_value.is_undefined);
        match &result_value.value {
            Some(proto::query_result_value::Value::TripleValue(tv)) => {
                assert_eq!(
                    tv.value,
                    Some(proto::triple_value::Value::String("query_test".to_owned()))
                );
            }
            _ => panic!("Expected a TripleValue"),
        }
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
                hlc: Some(proto::HlcTimestamp {
                    physical_time_ms: 1000 + u64::from(i),
                    logical_counter: 0,
                    node_id: 1,
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

        // Query all triples for the entity (entity scan) - using variables for attribute and value
        let query_request = proto::QueryRequest {
            find: vec![
                proto::QueryPatternVariable {
                    label: Some("attr".to_owned()),
                },
                proto::QueryPatternVariable {
                    label: Some("value".to_owned()),
                },
            ],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityId(entity_id)),
                attribute: Some(proto::query_pattern::Attribute::AttributeVariable(
                    proto::QueryPatternVariable {
                        label: Some("attr".to_owned()),
                    },
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("value".to_owned()),
                    },
                )),
            }],
            optional: vec![],
            where_not: vec![],
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
        assert_eq!(server_response.columns, vec!["attr", "value"]);
        assert_eq!(server_response.rows.len(), 3);
    }

    // Error path tests

    #[tokio::test]
    async fn test_handle_message_missing_payload() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        // Send a message with no payload
        let client_message = proto::ClientMessage {
            request_id: Some(400),
            payload: None,
        };

        let response = client_conn.handle_message(client_message).await;

        let server_response = response.response.unwrap();
        assert_eq!(server_response.request_id, Some(400));
        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::InvalidArgument as i32
        );
    }

    #[tokio::test]
    async fn test_handle_message_invalid_entity_id_length() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        // Entity ID is wrong length (should be 16 bytes)
        let triple = proto::Triple {
            entity_id: Some(vec![1u8; 10]), // Wrong length
            attribute_id: Some(vec![2u8; 16]),
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String("test".to_string())),
            }),
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
            }),
        };

        let update_request = proto::TripleUpdateRequest {
            triples: vec![triple],
        };

        let client_message = proto::ClientMessage {
            request_id: Some(401),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let response = client_conn.handle_message(client_message).await;

        let server_response = response.response.unwrap();
        assert_eq!(server_response.request_id, Some(401));
        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::InvalidArgument as i32
        );
    }

    #[tokio::test]
    async fn test_handle_message_invalid_attribute_id_length() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        // Attribute ID is wrong length (should be 16 bytes)
        let triple = proto::Triple {
            entity_id: Some(vec![1u8; 16]),
            attribute_id: Some(vec![2u8; 8]), // Wrong length
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String("test".to_string())),
            }),
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
            }),
        };

        let update_request = proto::TripleUpdateRequest {
            triples: vec![triple],
        };

        let client_message = proto::ClientMessage {
            request_id: Some(402),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let response = client_conn.handle_message(client_message).await;

        let server_response = response.response.unwrap();
        assert_eq!(server_response.request_id, Some(402));
        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::InvalidArgument as i32
        );
    }

    #[tokio::test]
    async fn test_handle_message_missing_entity_id() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        // Triple with missing entity_id
        let triple = proto::Triple {
            entity_id: None,
            attribute_id: Some(vec![2u8; 16]),
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String("test".to_string())),
            }),
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
            }),
        };

        let update_request = proto::TripleUpdateRequest {
            triples: vec![triple],
        };

        let client_message = proto::ClientMessage {
            request_id: Some(403),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let response = client_conn.handle_message(client_message).await;

        let server_response = response.response.unwrap();
        assert_eq!(server_response.request_id, Some(403));
        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::InvalidArgument as i32
        );
    }

    #[tokio::test]
    async fn test_handle_message_missing_attribute_id() {
        let database = new_test_database().expect("Failed to create test db");
        let client_conn = ClientConnection::new(database);

        // Triple with missing attribute_id
        let triple = proto::Triple {
            entity_id: Some(vec![1u8; 16]),
            attribute_id: None,
            value: Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String("test".to_string())),
            }),
            hlc: Some(proto::HlcTimestamp {
                physical_time_ms: 1000,
                logical_counter: 0,
                node_id: 1,
            }),
        };

        let update_request = proto::TripleUpdateRequest {
            triples: vec![triple],
        };

        let client_message = proto::ClientMessage {
            request_id: Some(404),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                update_request,
            )),
        };

        let response = client_conn.handle_message(client_message).await;

        let server_response = response.response.unwrap();
        assert_eq!(server_response.request_id, Some(404));
        assert_eq!(
            server_response.status.unwrap().code,
            proto::google::rpc::Code::InvalidArgument as i32
        );
    }
}
