//! Test inserting and querying a boolean value.

use crate::e2e_tests::helpers::{
    TestClient, get_bool_value, is_ok, new_attribute_id, new_entity_id, new_hlc,
};
use crate::proto;

#[test]
fn test_insert_boolean_then_query() {
    let test = TestClient::new();

    let entity_id = new_entity_id(3);
    let attribute_id = new_attribute_id(3);

    // Insert a boolean value
    let insert_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Boolean(true)),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });

    assert!(is_ok(&insert_resp));

    // Query it back
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
    assert_eq!(get_bool_value(&query_resp, 0), Some(true));
}
