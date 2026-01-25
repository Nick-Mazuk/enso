//! Test that updating a triple overwrites the previous value.

use crate::e2e_tests::helpers::{
    TestClient, get_string_value, is_ok, new_attribute_id, new_entity_id, new_hlc,
};
use crate::proto;

#[test]
fn test_update_overwrites_value() {
    let mut client = TestClient::new();

    let entity_id = new_entity_id(30);
    let attribute_id = new_attribute_id(30);

    // Insert initial value
    let response1 = client.handle_message(proto::ClientMessage {
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
    assert!(is_ok(&response1));

    // Update with new value
    let response2 = client.handle_message(proto::ClientMessage {
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
    assert!(is_ok(&response2));

    // Query should return updated value
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(3),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![proto::QueryPatternVariable {
                label: Some("value".to_string()),
            }],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityId(entity_id.to_vec())),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    attribute_id.to_vec(),
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
    });
    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 1);
    assert_eq!(get_string_value(&query_response, 0), Some("updated"));
}
