//! Test inserting and querying a string value.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_insert_string_then_query() {
    let test = TestClient::new();

    let eid = entity_id(1);
    let aid = attribute_id(1);

    // Insert a string value
    let insert_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(eid.to_vec()),
                    attribute_id: Some(aid.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(
                            "hello world".to_string(),
                        )),
                    }),
                }],
            },
        )),
    });

    assert!(is_ok(&insert_resp));
    assert_eq!(insert_resp.request_id, Some(1));

    // Query it back
    let query_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
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
    assert_eq!(query_resp.request_id, Some(2));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_string_value(&query_resp, 0), Some("hello world"));
}
