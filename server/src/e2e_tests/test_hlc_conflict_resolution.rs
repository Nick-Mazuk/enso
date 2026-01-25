//! Tests for HLC-based conflict resolution in triple updates.
//!
//! These tests verify that:
//! - Triples with newer HLCs overwrite existing triples
//! - Triples with older or equal HLCs are rejected
//! - Responses include the current HLC for each triple
//! - Queries return the correct version after updates

use crate::e2e_tests::helpers::{TestClient, is_ok, new_attribute_id, new_entity_id};
use crate::proto;

/// Helper to create an HLC timestamp.
fn make_hlc(physical_time_ms: u64, logical_counter: u32, node_id: u32) -> proto::HlcTimestamp {
    proto::HlcTimestamp {
        physical_time_ms,
        logical_counter,
        node_id,
    }
}

/// Helper to create a triple with HLC.
fn make_triple(
    entity_id: [u8; 16],
    attribute_id: [u8; 16],
    value: &str,
    hlc: proto::HlcTimestamp,
) -> proto::Triple {
    proto::Triple {
        entity_id: Some(entity_id.to_vec()),
        attribute_id: Some(attribute_id.to_vec()),
        value: Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String(value.to_string())),
        }),
        hlc: Some(hlc),
    }
}

/// Helper to query a triple and return the response.
fn query_triple(
    test: &TestClient,
    entity_id: [u8; 16],
    attribute_id: [u8; 16],
    request_id: u32,
) -> proto::ServerResponse {
    test.handle_message(proto::ClientMessage {
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
    })
}

/// Helper to extract string value from query response.
fn get_query_string(resp: &proto::ServerResponse) -> Option<&str> {
    resp.rows.first().and_then(|row| {
        row.values.first().and_then(|v| match &v.value {
            Some(proto::query_result_value::Value::TripleValue(tv)) => match &tv.value {
                Some(proto::triple_value::Value::String(s)) => Some(s.as_str()),
                _ => None,
            },
            _ => None,
        })
    })
}

#[test]
fn test_insert_new_triple_succeeds() {
    let test = TestClient::new();

    let entity_id = new_entity_id(100);
    let attribute_id = new_attribute_id(100);
    let hlc = make_hlc(1000, 0, 1);

    let resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "initial", hlc)],
            },
        )),
    });

    assert!(is_ok(&resp));
    assert_eq!(resp.triples.len(), 1);
    assert_eq!(
        resp.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("initial".to_string())),
        })
    );
    // Response should include the HLC
    assert!(resp.triples[0].hlc.is_some());

    // Verify query returns the correct value
    let query_resp = query_triple(&test, entity_id, attribute_id, 2);
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_query_string(&query_resp), Some("initial"));
}

#[test]
fn test_newer_hlc_wins() {
    let test = TestClient::new();

    let entity_id = new_entity_id(101);
    let attribute_id = new_attribute_id(101);

    // Insert initial value with HLC (1000, 0, 1)
    let initial_hlc = make_hlc(1000, 0, 1);
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "initial", initial_hlc)],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Update with newer HLC (2000, 0, 1) - should succeed
    let newer_hlc = make_hlc(2000, 0, 1);
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "updated", newer_hlc)],
            },
        )),
    });

    assert!(is_ok(&resp2));
    assert_eq!(resp2.triples.len(), 1);
    // Value should be updated
    assert_eq!(
        resp2.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("updated".to_string())),
        })
    );

    // Verify query returns the updated value
    let query_resp = query_triple(&test, entity_id, attribute_id, 3);
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_query_string(&query_resp), Some("updated"));
}

#[test]
fn test_older_hlc_loses() {
    let test = TestClient::new();

    let entity_id = new_entity_id(102);
    let attribute_id = new_attribute_id(102);

    // Insert initial value with HLC (2000, 0, 1)
    let initial_hlc = make_hlc(2000, 0, 1);
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "initial", initial_hlc)],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Try to update with older HLC (1000, 0, 1) - should be rejected
    let older_hlc = make_hlc(1000, 0, 1);
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "rejected", older_hlc)],
            },
        )),
    });

    assert!(is_ok(&resp2));
    assert_eq!(resp2.triples.len(), 1);
    // Value should still be the original (update rejected)
    assert_eq!(
        resp2.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("initial".to_string())),
        })
    );

    // Verify query returns the original value (rejected update)
    let query_resp = query_triple(&test, entity_id, attribute_id, 3);
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_query_string(&query_resp), Some("initial"));
}

#[test]
fn test_equal_hlc_rejected() {
    let test = TestClient::new();

    let entity_id = new_entity_id(103);
    let attribute_id = new_attribute_id(103);

    // Insert initial value with HLC (1000, 5, 1)
    let hlc = make_hlc(1000, 5, 1);
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "initial", hlc)],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Try to update with same HLC - should be rejected
    let same_hlc = make_hlc(1000, 5, 1);
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "rejected", same_hlc)],
            },
        )),
    });

    assert!(is_ok(&resp2));
    assert_eq!(resp2.triples.len(), 1);
    // Value should still be the original (update rejected)
    assert_eq!(
        resp2.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("initial".to_string())),
        })
    );

    // Verify query returns the original value (rejected update)
    let query_resp = query_triple(&test, entity_id, attribute_id, 3);
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_query_string(&query_resp), Some("initial"));
}

#[test]
fn test_logical_counter_newer_wins() {
    let test = TestClient::new();

    let entity_id = new_entity_id(104);
    let attribute_id = new_attribute_id(104);

    // Insert with HLC (1000, 5, 1)
    let initial_hlc = make_hlc(1000, 5, 1);
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "initial", initial_hlc)],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Update with same physical time but higher logical counter (1000, 10, 1) - should succeed
    let newer_hlc = make_hlc(1000, 10, 1);
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "updated", newer_hlc)],
            },
        )),
    });

    assert!(is_ok(&resp2));
    assert_eq!(
        resp2.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("updated".to_string())),
        })
    );

    // Verify query returns the updated value
    let query_resp = query_triple(&test, entity_id, attribute_id, 3);
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_query_string(&query_resp), Some("updated"));
}

#[test]
fn test_logical_counter_older_loses() {
    let test = TestClient::new();

    let entity_id = new_entity_id(110);
    let attribute_id = new_attribute_id(110);

    // Insert with HLC (1000, 10, 1)
    let initial_hlc = make_hlc(1000, 10, 1);
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "initial", initial_hlc)],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Try to update with same physical time but lower logical counter (1000, 5, 1) - should be rejected
    let older_hlc = make_hlc(1000, 5, 1);
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "rejected", older_hlc)],
            },
        )),
    });

    assert!(is_ok(&resp2));
    assert_eq!(resp2.triples.len(), 1);
    // Value should still be the original (update rejected)
    assert_eq!(
        resp2.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("initial".to_string())),
        })
    );

    // Verify query returns the original value (rejected update)
    let query_resp = query_triple(&test, entity_id, attribute_id, 3);
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_query_string(&query_resp), Some("initial"));
}

#[test]
fn test_node_id_newer_wins() {
    let test = TestClient::new();

    let entity_id = new_entity_id(111);
    let attribute_id = new_attribute_id(111);

    // Insert with HLC (1000, 5, 1)
    let initial_hlc = make_hlc(1000, 5, 1);
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "initial", initial_hlc)],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Update with same physical time and logical counter but higher node_id (1000, 5, 10) - should succeed
    let newer_hlc = make_hlc(1000, 5, 10);
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "updated", newer_hlc)],
            },
        )),
    });

    assert!(is_ok(&resp2));
    assert_eq!(resp2.triples.len(), 1);
    assert_eq!(
        resp2.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("updated".to_string())),
        })
    );

    // Verify query returns the updated value
    let query_resp = query_triple(&test, entity_id, attribute_id, 3);
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_query_string(&query_resp), Some("updated"));
}

#[test]
fn test_node_id_older_loses() {
    let test = TestClient::new();

    let entity_id = new_entity_id(112);
    let attribute_id = new_attribute_id(112);

    // Insert with HLC (1000, 5, 10)
    let initial_hlc = make_hlc(1000, 5, 10);
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "initial", initial_hlc)],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Try to update with same physical time and logical counter but lower node_id (1000, 5, 1) - should be rejected
    let older_hlc = make_hlc(1000, 5, 1);
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "rejected", older_hlc)],
            },
        )),
    });

    assert!(is_ok(&resp2));
    assert_eq!(resp2.triples.len(), 1);
    // Value should still be the original (update rejected)
    assert_eq!(
        resp2.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("initial".to_string())),
        })
    );

    // Verify query returns the original value (rejected update)
    let query_resp = query_triple(&test, entity_id, attribute_id, 3);
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_query_string(&query_resp), Some("initial"));
}

#[test]
fn test_mixed_batch_some_update_some_reject() {
    let test = TestClient::new();

    let entity1 = new_entity_id(105);
    let attr1 = new_attribute_id(105);
    let entity2 = new_entity_id(106);
    let attr2 = new_attribute_id(106);

    // Insert initial values
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    make_triple(entity1, attr1, "value1", make_hlc(1000, 0, 1)),
                    make_triple(entity2, attr2, "value2", make_hlc(2000, 0, 1)),
                ],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Send batch where:
    // - Triple 1 has newer HLC (should update)
    // - Triple 2 has older HLC (should be rejected)
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    make_triple(entity1, attr1, "updated1", make_hlc(3000, 0, 1)), // Newer
                    make_triple(entity2, attr2, "rejected2", make_hlc(1500, 0, 1)), // Older
                ],
            },
        )),
    });

    assert!(is_ok(&resp2));
    assert_eq!(resp2.triples.len(), 2);

    // First triple should be updated
    assert_eq!(
        resp2.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("updated1".to_string())),
        })
    );

    // Second triple should keep original value
    assert_eq!(
        resp2.triples[1].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("value2".to_string())),
        })
    );

    // Verify queries return the correct values
    let query1 = query_triple(&test, entity1, attr1, 3);
    assert!(is_ok(&query1));
    assert_eq!(query1.rows.len(), 1);
    assert_eq!(get_query_string(&query1), Some("updated1"));

    let query2 = query_triple(&test, entity2, attr2, 4);
    assert!(is_ok(&query2));
    assert_eq!(query2.rows.len(), 1);
    assert_eq!(get_query_string(&query2), Some("value2"));
}

#[test]
fn test_response_includes_hlc() {
    let test = TestClient::new();

    let entity_id = new_entity_id(107);
    let attribute_id = new_attribute_id(107);
    let hlc = make_hlc(5000, 42, 7);

    let resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![make_triple(entity_id, attribute_id, "test", hlc)],
            },
        )),
    });

    assert!(is_ok(&resp));
    assert_eq!(resp.triples.len(), 1);

    // Response should include the HLC
    let response_hlc = resp.triples[0]
        .hlc
        .as_ref()
        .expect("Response should include HLC");
    assert_eq!(response_hlc.physical_time_ms, 5000);
    assert_eq!(response_hlc.logical_counter, 42);
    assert_eq!(response_hlc.node_id, 7);
}

#[test]
fn test_missing_hlc_rejected() {
    let test = TestClient::new();

    let entity_id = new_entity_id(108);
    let attribute_id = new_attribute_id(108);

    // Send triple without HLC
    let resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                    hlc: None, // Missing HLC
                }],
            },
        )),
    });

    // Should return InvalidArgument
    assert!(resp.status.is_some());
    let status = resp.status.as_ref().unwrap();
    assert_eq!(
        status.code,
        proto::google::rpc::Code::InvalidArgument as i32
    );
    assert!(status.message.contains("hlc"));
}
