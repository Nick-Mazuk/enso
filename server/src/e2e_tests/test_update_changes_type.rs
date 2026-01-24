//! Test that updating a triple can change the value type.

use crate::e2e_tests::helpers::{TestClient, attribute_id, entity_id, get_number_value, is_ok};
use crate::proto;

#[test]
fn test_update_changes_value_type() {
    let test = TestClient::new();

    let eid = entity_id(32);
    let aid = attribute_id(32);

    // Insert as string
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(eid.to_vec()),
                    attribute_id: Some(aid.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("text".to_string())),
                    }),
                }],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Update to number
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(eid.to_vec()),
                    attribute_id: Some(aid.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Number(123.0)),
                    }),
                }],
            },
        )),
    });
    assert!(is_ok(&resp2));

    // Query should return number
    let query_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(3),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![proto::QueryPatternVariable {
                label: Some("v".to_string()),
            }],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityId(eid.to_vec())),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(aid.to_vec())),
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
    assert_eq!(get_number_value(&query_resp, 0), Some(123.0));
}
