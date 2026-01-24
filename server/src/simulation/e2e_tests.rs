//! End-to-end tests at the proto request/response level.
//!
//! These tests verify the complete request/response cycle through
//! the `ClientConnection` interface using deterministic inputs.

#![cfg(test)]

use std::sync::atomic::{AtomicU64, Ordering};

use crate::client_connection::ClientConnection;
use crate::proto;
use crate::storage::Database;

/// Counter for generating unique test database IDs.
static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Test harness for deterministic end-to-end testing.
struct TestHarness {
    client: ClientConnection,
    db_path: std::path::PathBuf,
    runtime: tokio::runtime::Runtime,
}

impl TestHarness {
    /// Create a new test harness with a fresh database.
    fn new() -> Self {
        let temp_dir = std::env::temp_dir();
        let instance_id = TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!("e2e_test_{instance_id}.db"));

        // Remove if exists
        let _ = std::fs::remove_file(&db_path);

        #[allow(clippy::expect_used)]
        let database = Database::create(&db_path).expect("Failed to create test database");
        let client = ClientConnection::new(database);

        #[allow(clippy::expect_used)]
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");

        Self {
            client,
            db_path,
            runtime,
        }
    }

    /// Send a request and get the response.
    fn send(&self, message: proto::ClientMessage) -> proto::ServerResponse {
        let response = self
            .runtime
            .block_on(async { self.client.handle_message(message).await });

        #[allow(clippy::expect_used)]
        response.response.expect("Response should be present")
    }

    /// Helper to create an update request with a single triple.
    fn update_request(
        request_id: u32,
        entity_id: [u8; 16],
        attribute_id: [u8; 16],
        value: proto::triple_value::Value,
    ) -> proto::ClientMessage {
        proto::ClientMessage {
            request_id: Some(request_id),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                proto::TripleUpdateRequest {
                    triples: vec![proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attribute_id.to_vec()),
                        value: Some(proto::TripleValue { value: Some(value) }),
                    }],
                },
            )),
        }
    }

    /// Helper to create a query request that queries all attributes of an entity.
    /// Returns find variables: [attribute, value]
    fn entity_scan_query(request_id: u32, entity_id: [u8; 16]) -> proto::ClientMessage {
        proto::ClientMessage {
            request_id: Some(request_id),
            payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
                find: vec![
                    proto::QueryPatternVariable {
                        label: Some("a".to_string()),
                    },
                    proto::QueryPatternVariable {
                        label: Some("v".to_string()),
                    },
                ],
                r#where: vec![proto::QueryPattern {
                    entity: Some(proto::query_pattern::Entity::EntityId(entity_id.to_vec())),
                    attribute: Some(proto::query_pattern::Attribute::AttributeVariable(
                        proto::QueryPatternVariable {
                            label: Some("a".to_string()),
                        },
                    )),
                    value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                        proto::QueryPatternVariable {
                            label: Some("v".to_string()),
                        },
                    )),
                }],
                optional: vec![],
                where_not: vec![],
            })),
        }
    }

    /// Helper to create a point query (entity + attribute -> value).
    fn point_query(
        request_id: u32,
        entity_id: [u8; 16],
        attribute_id: [u8; 16],
    ) -> proto::ClientMessage {
        proto::ClientMessage {
            request_id: Some(request_id),
            payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
                find: vec![proto::QueryPatternVariable {
                    label: Some("v".to_string()),
                }],
                r#where: vec![proto::QueryPattern {
                    entity: Some(proto::query_pattern::Entity::EntityId(entity_id.to_vec())),
                    attribute: Some(proto::query_pattern::Attribute::AttributeId(
                        attribute_id.to_vec(),
                    )),
                    value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                        proto::QueryPatternVariable {
                            label: Some("v".to_string()),
                        },
                    )),
                }],
                optional: vec![],
                where_not: vec![],
            })),
        }
    }

    /// Assert that a response is successful (OK status).
    fn assert_ok(response: &proto::ServerResponse) {
        #[allow(clippy::expect_used)]
        let status = response.status.as_ref().expect("Status should be present");
        assert_eq!(
            status.code,
            proto::google::rpc::Code::Ok as i32,
            "Expected OK status, got: {} - {}",
            status.code,
            status.message
        );
    }

    /// Assert that a response has a specific error code.
    fn assert_error(response: &proto::ServerResponse, expected_code: proto::google::rpc::Code) {
        #[allow(clippy::expect_used)]
        let status = response.status.as_ref().expect("Status should be present");
        assert_eq!(
            status.code, expected_code as i32,
            "Expected error code {:?}, got: {} - {}",
            expected_code, status.code, status.message
        );
    }

    /// Extract a TripleValue from a query result value.
    fn extract_triple_value(
        result_value: &proto::QueryResultValue,
    ) -> Option<&proto::triple_value::Value> {
        match &result_value.value {
            Some(proto::query_result_value::Value::TripleValue(tv)) => tv.value.as_ref(),
            _ => None,
        }
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.db_path);
    }
}

// =============================================================================
// Basic Insert and Query Tests
// =============================================================================

#[test]
fn test_insert_string_then_query() {
    let harness = TestHarness::new();

    // Fixed entity and attribute IDs for determinism
    let entity_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let attribute_id: [u8; 16] = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];

    // Insert a string value
    let insert_req = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::String("hello world".to_string()),
    );
    let insert_resp = harness.send(insert_req);
    TestHarness::assert_ok(&insert_resp);
    assert_eq!(insert_resp.request_id, Some(1));

    // Query for the value
    let query_req = TestHarness::point_query(2, entity_id, attribute_id);
    let query_resp = harness.send(query_req);
    TestHarness::assert_ok(&query_resp);
    assert_eq!(query_resp.request_id, Some(2));

    // Verify we got the expected result
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(query_resp.rows[0].values.len(), 1);

    let value = TestHarness::extract_triple_value(&query_resp.rows[0].values[0]);
    assert_eq!(
        value,
        Some(&proto::triple_value::Value::String(
            "hello world".to_string()
        ))
    );
}

#[test]
fn test_insert_number_then_query() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Insert a number value
    let insert_req = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::Number(42.5),
    );
    let insert_resp = harness.send(insert_req);
    TestHarness::assert_ok(&insert_resp);

    // Query for the value
    let query_req = TestHarness::point_query(2, entity_id, attribute_id);
    let query_resp = harness.send(query_req);
    TestHarness::assert_ok(&query_resp);

    assert_eq!(query_resp.rows.len(), 1);
    let value = TestHarness::extract_triple_value(&query_resp.rows[0].values[0]);
    assert_eq!(value, Some(&proto::triple_value::Value::Number(42.5)));
}

#[test]
fn test_insert_boolean_then_query() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Insert a boolean value
    let insert_req = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::Boolean(true),
    );
    let insert_resp = harness.send(insert_req);
    TestHarness::assert_ok(&insert_resp);

    // Query for the value
    let query_req = TestHarness::point_query(2, entity_id, attribute_id);
    let query_resp = harness.send(query_req);
    TestHarness::assert_ok(&query_resp);

    assert_eq!(query_resp.rows.len(), 1);
    let value = TestHarness::extract_triple_value(&query_resp.rows[0].values[0]);
    assert_eq!(value, Some(&proto::triple_value::Value::Boolean(true)));
}

// =============================================================================
// Multiple Triples Tests
// =============================================================================

#[test]
fn test_insert_multiple_triples_single_request() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attr1: [u8; 16] = [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attr2: [u8; 16] = [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attr3: [u8; 16] = [13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Insert multiple triples in a single request
    let insert_req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attr1.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("name".to_string())),
                        }),
                    },
                    proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attr2.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(25.0)),
                        }),
                    },
                    proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attr3.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(false)),
                        }),
                    },
                ],
            },
        )),
    };

    let insert_resp = harness.send(insert_req);
    TestHarness::assert_ok(&insert_resp);

    // Query for all values of the entity
    let query_req = TestHarness::entity_scan_query(2, entity_id);
    let query_resp = harness.send(query_req);
    TestHarness::assert_ok(&query_resp);

    // Should have 3 rows
    assert_eq!(query_resp.rows.len(), 3);
}

#[test]
fn test_insert_multiple_entities() {
    let harness = TestHarness::new();

    let entity1: [u8; 16] = [20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let entity2: [u8; 16] = [21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute: [u8; 16] = [22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Insert for entity1
    let req1 = TestHarness::update_request(
        1,
        entity1,
        attribute,
        proto::triple_value::Value::String("entity one".to_string()),
    );
    TestHarness::assert_ok(&harness.send(req1));

    // Insert for entity2
    let req2 = TestHarness::update_request(
        2,
        entity2,
        attribute,
        proto::triple_value::Value::String("entity two".to_string()),
    );
    TestHarness::assert_ok(&harness.send(req2));

    // Query for entity1
    let resp1 = harness.send(TestHarness::point_query(3, entity1, attribute));
    TestHarness::assert_ok(&resp1);
    assert_eq!(resp1.rows.len(), 1);
    let value1 = TestHarness::extract_triple_value(&resp1.rows[0].values[0]);
    assert_eq!(
        value1,
        Some(&proto::triple_value::Value::String(
            "entity one".to_string()
        ))
    );

    // Query for entity2
    let resp2 = harness.send(TestHarness::point_query(4, entity2, attribute));
    TestHarness::assert_ok(&resp2);
    assert_eq!(resp2.rows.len(), 1);
    let value2 = TestHarness::extract_triple_value(&resp2.rows[0].values[0]);
    assert_eq!(
        value2,
        Some(&proto::triple_value::Value::String(
            "entity two".to_string()
        ))
    );
}

// =============================================================================
// Update (Overwrite) Tests
// =============================================================================

#[test]
fn test_update_overwrites_value() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [31, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Insert initial value
    let req1 = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::String("original".to_string()),
    );
    TestHarness::assert_ok(&harness.send(req1));

    // Update with new value
    let req2 = TestHarness::update_request(
        2,
        entity_id,
        attribute_id,
        proto::triple_value::Value::String("updated".to_string()),
    );
    TestHarness::assert_ok(&harness.send(req2));

    // Query should return updated value
    let resp = harness.send(TestHarness::point_query(3, entity_id, attribute_id));
    TestHarness::assert_ok(&resp);

    assert_eq!(resp.rows.len(), 1);
    let value = TestHarness::extract_triple_value(&resp.rows[0].values[0]);
    assert_eq!(
        value,
        Some(&proto::triple_value::Value::String("updated".to_string()))
    );
}

#[test]
fn test_update_changes_value_type() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Insert as string
    let req1 = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::String("text".to_string()),
    );
    TestHarness::assert_ok(&harness.send(req1));

    // Update to number
    let req2 = TestHarness::update_request(
        2,
        entity_id,
        attribute_id,
        proto::triple_value::Value::Number(123.0),
    );
    TestHarness::assert_ok(&harness.send(req2));

    // Query should return number
    let resp = harness.send(TestHarness::point_query(3, entity_id, attribute_id));
    TestHarness::assert_ok(&resp);

    assert_eq!(resp.rows.len(), 1);
    let value = TestHarness::extract_triple_value(&resp.rows[0].values[0]);
    assert_eq!(value, Some(&proto::triple_value::Value::Number(123.0)));
}

// =============================================================================
// Empty and Edge Case Tests
// =============================================================================

#[test]
fn test_empty_triples_request() {
    let harness = TestHarness::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest { triples: vec![] },
        )),
    };

    let resp = harness.send(req);
    TestHarness::assert_ok(&resp);
    assert_eq!(resp.request_id, Some(1));
}

#[test]
fn test_query_empty_database() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2];

    let req = TestHarness::point_query(1, entity_id, attribute_id);
    let resp = harness.send(req);

    TestHarness::assert_ok(&resp);
    assert_eq!(resp.rows.len(), 0);
}

#[test]
fn test_query_nonexistent_entity() {
    let harness = TestHarness::new();

    // Insert some data
    let entity_id: [u8; 16] = [40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [41, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    let req = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::String("exists".to_string()),
    );
    TestHarness::assert_ok(&harness.send(req));

    // Query for a different entity
    let other_entity: [u8; 16] = [99, 99, 99, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let resp = harness.send(TestHarness::point_query(2, other_entity, attribute_id));

    TestHarness::assert_ok(&resp);
    assert_eq!(resp.rows.len(), 0);
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_invalid_entity_id_length() {
    let harness = TestHarness::new();

    // Entity ID with wrong length (not 16 bytes)
    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(vec![1, 2, 3]), // Only 3 bytes
                    attribute_id: Some(vec![0u8; 16]),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                }],
            },
        )),
    };

    let resp = harness.send(req);
    TestHarness::assert_error(&resp, proto::google::rpc::Code::InvalidArgument);
}

#[test]
fn test_invalid_attribute_id_length() {
    let harness = TestHarness::new();

    // Attribute ID with wrong length
    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(vec![0u8; 16]),
                    attribute_id: Some(vec![1, 2, 3, 4, 5]), // Only 5 bytes
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                }],
            },
        )),
    };

    let resp = harness.send(req);
    TestHarness::assert_error(&resp, proto::google::rpc::Code::InvalidArgument);
}

#[test]
fn test_missing_entity_id() {
    let harness = TestHarness::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: None,
                    attribute_id: Some(vec![0u8; 16]),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                }],
            },
        )),
    };

    let resp = harness.send(req);
    TestHarness::assert_error(&resp, proto::google::rpc::Code::InvalidArgument);
}

#[test]
fn test_missing_attribute_id() {
    let harness = TestHarness::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(vec![0u8; 16]),
                    attribute_id: None,
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                }],
            },
        )),
    };

    let resp = harness.send(req);
    TestHarness::assert_error(&resp, proto::google::rpc::Code::InvalidArgument);
}

#[test]
fn test_missing_value() {
    let harness = TestHarness::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(vec![0u8; 16]),
                    attribute_id: Some(vec![0u8; 16]),
                    value: None,
                }],
            },
        )),
    };

    let resp = harness.send(req);
    TestHarness::assert_error(&resp, proto::google::rpc::Code::InvalidArgument);
}

#[test]
fn test_no_payload() {
    let harness = TestHarness::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: None,
    };

    let resp = harness.send(req);
    TestHarness::assert_error(&resp, proto::google::rpc::Code::InvalidArgument);
}

// =============================================================================
// Request ID Tracking Tests
// =============================================================================

#[test]
fn test_request_id_preserved() {
    let harness = TestHarness::new();

    // Test various request IDs
    for request_id in [1, 100, 999, u32::MAX] {
        let req = proto::ClientMessage {
            request_id: Some(request_id),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                proto::TripleUpdateRequest { triples: vec![] },
            )),
        };

        let resp = harness.send(req);
        assert_eq!(resp.request_id, Some(request_id));
    }
}

#[test]
fn test_request_id_none() {
    let harness = TestHarness::new();

    let req = proto::ClientMessage {
        request_id: None,
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest { triples: vec![] },
        )),
    };

    let resp = harness.send(req);
    assert_eq!(resp.request_id, None);
}

// =============================================================================
// Sequence Tests (Multiple Operations)
// =============================================================================

#[test]
fn test_sequence_insert_query_update_query() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Step 1: Insert
    let req1 = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::Number(1.0),
    );
    TestHarness::assert_ok(&harness.send(req1));

    // Step 2: Query (should see 1.0)
    let resp2 = harness.send(TestHarness::point_query(2, entity_id, attribute_id));
    TestHarness::assert_ok(&resp2);
    assert_eq!(resp2.rows.len(), 1);
    let value2 = TestHarness::extract_triple_value(&resp2.rows[0].values[0]);
    assert_eq!(value2, Some(&proto::triple_value::Value::Number(1.0)));

    // Step 3: Update
    let req3 = TestHarness::update_request(
        3,
        entity_id,
        attribute_id,
        proto::triple_value::Value::Number(2.0),
    );
    TestHarness::assert_ok(&harness.send(req3));

    // Step 4: Query (should see 2.0)
    let resp4 = harness.send(TestHarness::point_query(4, entity_id, attribute_id));
    TestHarness::assert_ok(&resp4);
    assert_eq!(resp4.rows.len(), 1);
    let value4 = TestHarness::extract_triple_value(&resp4.rows[0].values[0]);
    assert_eq!(value4, Some(&proto::triple_value::Value::Number(2.0)));
}

#[test]
fn test_many_sequential_inserts() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Insert 100 different attributes
    for i in 0..100u8 {
        let mut attribute_id = [0u8; 16];
        attribute_id[0] = 61;
        attribute_id[1] = i;

        let req = TestHarness::update_request(
            u32::from(i) + 1,
            entity_id,
            attribute_id,
            proto::triple_value::Value::Number(f64::from(i)),
        );
        TestHarness::assert_ok(&harness.send(req));
    }

    // Query all attributes
    let resp = harness.send(TestHarness::entity_scan_query(101, entity_id));
    TestHarness::assert_ok(&resp);

    assert_eq!(resp.rows.len(), 100);
}

// =============================================================================
// String Value Tests
// =============================================================================

#[test]
fn test_max_length_string_value() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [71, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Create a string at max length (1024 chars)
    let max_string: String = "x".repeat(1024);

    let req = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::String(max_string.clone()),
    );
    TestHarness::assert_ok(&harness.send(req));

    // Query and verify
    let resp = harness.send(TestHarness::point_query(2, entity_id, attribute_id));
    TestHarness::assert_ok(&resp);

    assert_eq!(resp.rows.len(), 1);
    let value = TestHarness::extract_triple_value(&resp.rows[0].values[0]);
    assert_eq!(
        value,
        Some(&proto::triple_value::Value::String(max_string))
    );
}

#[test]
fn test_string_too_long_rejected() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [72, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [73, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Create a string exceeding max length (1025 chars)
    let too_long_string: String = "y".repeat(1025);

    let req = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::String(too_long_string),
    );
    let resp = harness.send(req);

    // Should be rejected
    TestHarness::assert_error(&resp, proto::google::rpc::Code::InvalidArgument);
}

// =============================================================================
// Determinism Test
// =============================================================================

#[test]
fn test_deterministic_sequence() {
    // Run the same sequence twice and verify identical results

    fn run_sequence() -> Vec<proto::ServerResponse> {
        let harness = TestHarness::new();
        let mut responses = Vec::new();

        let entity_id: [u8; 16] = [80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let attr1: [u8; 16] = [81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let attr2: [u8; 16] = [82, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

        // Insert
        responses.push(harness.send(TestHarness::update_request(
            1,
            entity_id,
            attr1,
            proto::triple_value::Value::String("first".to_string()),
        )));

        responses.push(harness.send(TestHarness::update_request(
            2,
            entity_id,
            attr2,
            proto::triple_value::Value::Number(42.0),
        )));

        // Query
        responses.push(harness.send(TestHarness::entity_scan_query(3, entity_id)));

        // Update
        responses.push(harness.send(TestHarness::update_request(
            4,
            entity_id,
            attr1,
            proto::triple_value::Value::String("updated".to_string()),
        )));

        // Query again
        responses.push(harness.send(TestHarness::entity_scan_query(5, entity_id)));

        responses
    }

    let run1 = run_sequence();
    let run2 = run_sequence();

    // Compare all responses
    assert_eq!(run1.len(), run2.len());
    for (i, (r1, r2)) in run1.iter().zip(run2.iter()).enumerate() {
        assert_eq!(r1.request_id, r2.request_id, "request_id mismatch at {i}");
        assert_eq!(
            r1.status.as_ref().map(|s| s.code),
            r2.status.as_ref().map(|s| s.code),
            "status code mismatch at {i}"
        );
        assert_eq!(r1.rows.len(), r2.rows.len(), "row count mismatch at {i}");

        // Compare row contents
        for (j, (row1, row2)) in r1.rows.iter().zip(r2.rows.iter()).enumerate() {
            assert_eq!(
                row1.values.len(),
                row2.values.len(),
                "value count mismatch at {i}:{j}"
            );
            for (k, (v1, v2)) in row1.values.iter().zip(row2.values.iter()).enumerate() {
                assert_eq!(v1.value, v2.value, "value mismatch at {i}:{j}:{k}");
            }
        }
    }
}

// =============================================================================
// Column Names Tests
// =============================================================================

#[test]
fn test_query_returns_correct_columns() {
    let harness = TestHarness::new();

    let entity_id: [u8; 16] = [90, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let attribute_id: [u8; 16] = [91, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    // Insert a value
    let req = TestHarness::update_request(
        1,
        entity_id,
        attribute_id,
        proto::triple_value::Value::String("test".to_string()),
    );
    TestHarness::assert_ok(&harness.send(req));

    // Point query returns column "v"
    let point_resp = harness.send(TestHarness::point_query(2, entity_id, attribute_id));
    TestHarness::assert_ok(&point_resp);
    assert_eq!(point_resp.columns, vec!["v"]);

    // Entity scan returns columns "a" and "v"
    let scan_resp = harness.send(TestHarness::entity_scan_query(3, entity_id));
    TestHarness::assert_ok(&scan_resp);
    assert_eq!(scan_resp.columns, vec!["a", "v"]);
}
