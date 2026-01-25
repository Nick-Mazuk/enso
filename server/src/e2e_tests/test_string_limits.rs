//! Test string length limits.

use crate::e2e_tests::helpers::{
    TestClient, get_string_value, is_ok, new_attribute_id, new_entity_id, new_hlc, status_code,
};
use crate::proto;

#[test]
fn test_max_length_string_value() {
    let test = TestClient::new();

    let entity_id = new_entity_id(70);
    let attribute_id = new_attribute_id(70);

    // Create a string at max length (1024 chars)
    let max_string: String = "x".repeat(1024);

    let resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(max_string.clone())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });
    assert!(is_ok(&resp));

    // Query and verify
    let query_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
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
    });
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_string_value(&query_resp, 0), Some(max_string.as_str()));
}

#[test]
fn test_string_too_long_rejected() {
    let test = TestClient::new();

    let entity_id = new_entity_id(71);
    let attribute_id = new_attribute_id(71);

    // Create a string exceeding max length (1025 chars)
    let too_long_string: String = "y".repeat(1025);

    let resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(too_long_string)),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });

    assert_eq!(
        status_code(&resp),
        proto::google::rpc::Code::InvalidArgument as i32
    );
}
