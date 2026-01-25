//! Tests for HLC-based conflict resolution in triple updates.
//!
//! These tests verify that:
//! - Triples with newer HLCs overwrite existing triples
//! - Triples with older or equal HLCs are rejected
//! - Responses include the current HLC for each triple

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
}

#[test]
fn test_logical_counter_comparison() {
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
