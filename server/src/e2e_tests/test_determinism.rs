//! Test that the same sequence of operations produces identical results.

use crate::e2e_tests::helpers::{
    TestClient, new_attribute_id, new_entity_id, new_hlc, status_code,
};
use crate::proto;

#[allow(clippy::too_many_lines)]
fn run_sequence() -> Vec<proto::ServerResponse> {
    let mut client = TestClient::new();
    let mut responses = Vec::new();

    let entity_id = new_entity_id(80);
    let attribute_id_1 = new_attribute_id(81);
    let attribute_id_2 = new_attribute_id(82);

    // Insert
    responses.push(client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id_1.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("first".to_string())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    }));

    responses.push(client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id_2.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Number(42.0)),
                    }),
                    hlc: Some(new_hlc(2)),
                }],
            },
        )),
    }));

    // Query
    responses.push(client.handle_message(proto::ClientMessage {
        request_id: Some(3),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![
                proto::QueryPatternVariable {
                    label: Some("attribute".to_string()),
                },
                proto::QueryPatternVariable {
                    label: Some("value".to_string()),
                },
            ],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityId(entity_id.to_vec())),
                attribute: Some(proto::query_pattern::Attribute::AttributeVariable(
                    proto::QueryPatternVariable {
                        label: Some("attribute".to_string()),
                    },
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("value".to_string()),
                    },
                )),
            }],
            optional: vec![],
            where_not: vec![],
        })),
    }));

    // Update
    responses.push(client.handle_message(proto::ClientMessage {
        request_id: Some(4),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id_1.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("updated".to_string())),
                    }),
                    hlc: Some(new_hlc(3)),
                }],
            },
        )),
    }));

    // Query again
    responses.push(client.handle_message(proto::ClientMessage {
        request_id: Some(5),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![
                proto::QueryPatternVariable {
                    label: Some("attribute".to_string()),
                },
                proto::QueryPatternVariable {
                    label: Some("value".to_string()),
                },
            ],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityId(entity_id.to_vec())),
                attribute: Some(proto::query_pattern::Attribute::AttributeVariable(
                    proto::QueryPatternVariable {
                        label: Some("attribute".to_string()),
                    },
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("value".to_string()),
                    },
                )),
            }],
            optional: vec![],
            where_not: vec![],
        })),
    }));

    responses
}

#[test]
fn test_deterministic_sequence() {
    let run1 = run_sequence();
    let run2 = run_sequence();

    // Compare all responses
    assert_eq!(run1.len(), run2.len());

    for (i, (r1, r2)) in run1.iter().zip(run2.iter()).enumerate() {
        assert_eq!(r1.request_id, r2.request_id, "request_id mismatch at {i}");
        assert_eq!(
            status_code(r1),
            status_code(r2),
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
