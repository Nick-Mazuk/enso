//! Test inserting triples for multiple entities.

use crate::e2e_tests::helpers::{
    TestClient, get_string_value, is_ok, new_attribute_id, new_entity_id, new_hlc,
};
use crate::proto;

#[test]
fn test_insert_multiple_entities() {
    let mut test = TestClient::new();

    let entity_id_1 = new_entity_id(20);
    let entity_id_2 = new_entity_id(21);
    let attribute_id = new_attribute_id(22);

    // Insert for entity 1
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id_1.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("entity one".to_string())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Insert for entity 2
    let resp2 = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id_2.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("entity two".to_string())),
                    }),
                    hlc: Some(new_hlc(2)),
                }],
            },
        )),
    });
    assert!(is_ok(&resp2));

    // Query entity 1
    let query1 = test.handle_message(proto::ClientMessage {
        request_id: Some(3),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![proto::QueryPatternVariable {
                label: Some("v".to_string()),
            }],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityId(entity_id_1.to_vec())),
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
    assert!(is_ok(&query1));
    assert_eq!(query1.rows.len(), 1);
    assert_eq!(get_string_value(&query1, 0), Some("entity one"));

    // Query entity 2
    let query2 = test.handle_message(proto::ClientMessage {
        request_id: Some(4),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![proto::QueryPatternVariable {
                label: Some("v".to_string()),
            }],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityId(entity_id_2.to_vec())),
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
    assert!(is_ok(&query2));
    assert_eq!(query2.rows.len(), 1);
    assert_eq!(get_string_value(&query2, 0), Some("entity two"));
}
