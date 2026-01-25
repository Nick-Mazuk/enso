//! Test inserting multiple triples in a single request.

use crate::e2e_tests::helpers::{TestClient, is_ok, new_attribute_id, new_entity_id, new_hlc};
use crate::proto;

#[test]
fn test_insert_multiple_triples_single_request() {
    let mut client = TestClient::new();

    let entity_id = new_entity_id(10);
    let attribute_id_1 = new_attribute_id(11);
    let attribute_id_2 = new_attribute_id(12);
    let attribute_id_3 = new_attribute_id(13);

    // Insert multiple triples in one request
    let insert_response = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attribute_id_1.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("name".to_string())),
                        }),
                        hlc: Some(new_hlc(1)),
                    },
                    proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attribute_id_2.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(25.0)),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                    proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attribute_id_3.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(false)),
                        }),
                        hlc: Some(new_hlc(3)),
                    },
                ],
            },
        )),
    });

    assert!(is_ok(&insert_response));

    // Query all attributes for the entity
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
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
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 3);
}
