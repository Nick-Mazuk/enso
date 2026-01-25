//! Test that update responses return the current values of written triples.
//!
//! Per the protocol specification, update responses return the most up-to-date
//! values for all triples that were written.

use crate::e2e_tests::helpers::{TestClient, is_ok, new_attribute_id, new_entity_id, new_hlc};
use crate::proto;

#[test]
fn test_insert_response_returns_written_triples() {
    let mut client = TestClient::new();

    let entity_id = new_entity_id(80);
    let attribute_id = new_attribute_id(80);

    // Insert a triple
    let resp = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("hello".to_string())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });

    // Response should be OK
    assert!(is_ok(&resp));

    // Response should contain the written triple
    assert_eq!(
        resp.triples.len(),
        1,
        "Insert response should contain 1 triple, got {}",
        resp.triples.len()
    );

    // Verify the returned triple has correct values
    let triple = &resp.triples[0];
    assert_eq!(triple.entity_id, Some(entity_id.to_vec()));
    assert_eq!(triple.attribute_id, Some(attribute_id.to_vec()));
    assert_eq!(
        triple.value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("hello".to_string())),
        })
    );
}

#[test]
fn test_update_response_returns_current_value() {
    let mut client = TestClient::new();

    let entity_id = new_entity_id(81);
    let attribute_id = new_attribute_id(81);

    // Insert initial value
    let insert_resp = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("original".to_string())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });
    assert!(is_ok(&insert_resp));
    assert_eq!(insert_resp.triples.len(), 1);

    // Update with new value
    let update_resp = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("updated".to_string())),
                    }),
                    hlc: Some(new_hlc(2)),
                }],
            },
        )),
    });

    // Response should be OK
    assert!(is_ok(&update_resp));

    // Response should contain the updated triple
    assert_eq!(
        update_resp.triples.len(),
        1,
        "Update response should contain 1 triple, got {}",
        update_resp.triples.len()
    );

    // Verify the returned triple has the updated value
    let triple = &update_resp.triples[0];
    assert_eq!(triple.entity_id, Some(entity_id.to_vec()));
    assert_eq!(triple.attribute_id, Some(attribute_id.to_vec()));
    assert_eq!(
        triple.value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("updated".to_string())),
        })
    );
}

#[test]
fn test_multi_triple_update_returns_all_values() {
    let mut client = TestClient::new();

    // Insert multiple triples in one request
    let resp = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    proto::Triple {
                        entity_id: Some(new_entity_id(82).to_vec()),
                        attribute_id: Some(new_attribute_id(82).to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("value1".to_string())),
                        }),
                        hlc: Some(new_hlc(1)),
                    },
                    proto::Triple {
                        entity_id: Some(new_entity_id(83).to_vec()),
                        attribute_id: Some(new_attribute_id(83).to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(42.0)),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                    proto::Triple {
                        entity_id: Some(new_entity_id(84).to_vec()),
                        attribute_id: Some(new_attribute_id(84).to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(true)),
                        }),
                        hlc: Some(new_hlc(3)),
                    },
                ],
            },
        )),
    });

    // Response should be OK
    assert!(is_ok(&resp));

    // Response should contain all 3 triples
    assert_eq!(
        resp.triples.len(),
        3,
        "Multi-triple update response should contain 3 triples, got {}",
        resp.triples.len()
    );

    // Verify each triple was returned with correct values
    // Note: Order should match insertion order
    assert_eq!(
        resp.triples[0].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::String("value1".to_string())),
        })
    );
    assert_eq!(
        resp.triples[1].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::Number(42.0)),
        })
    );
    assert_eq!(
        resp.triples[2].value,
        Some(proto::TripleValue {
            value: Some(proto::triple_value::Value::Boolean(true)),
        })
    );
}

#[test]
fn test_empty_update_returns_no_triples() {
    let mut client = TestClient::new();

    // Send empty update request
    let resp = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest { triples: vec![] },
        )),
    });

    // Response should be OK
    assert!(is_ok(&resp));

    // No triples were written, so none should be returned
    assert!(
        resp.triples.is_empty(),
        "Empty update response should contain no triples"
    );
}
