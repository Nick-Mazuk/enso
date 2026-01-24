//! Test inserting multiple triples in a single request.

use crate::e2e_tests::helpers::{TestClient, attribute_id, entity_id, is_ok};
use crate::proto;

#[test]
fn test_insert_multiple_triples_single_request() {
    let test = TestClient::new();

    let eid = entity_id(10);
    let aid1 = attribute_id(11);
    let aid2 = attribute_id(12);
    let aid3 = attribute_id(13);

    // Insert multiple triples in one request
    let insert_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    proto::Triple {
                        entity_id: Some(eid.to_vec()),
                        attribute_id: Some(aid1.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("name".to_string())),
                        }),
                    },
                    proto::Triple {
                        entity_id: Some(eid.to_vec()),
                        attribute_id: Some(aid2.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(25.0)),
                        }),
                    },
                    proto::Triple {
                        entity_id: Some(eid.to_vec()),
                        attribute_id: Some(aid3.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(false)),
                        }),
                    },
                ],
            },
        )),
    });

    assert!(is_ok(&insert_resp));

    // Query all attributes for the entity
    let query_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![
                proto::QueryPatternVariable {
                    label: Some("a".to_string()),
                },
                proto::QueryPatternVariable {
                    label: Some("v".to_string()),
                },
            ],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityId(eid.to_vec())),
                attribute: Some(proto::query_pattern::Attribute::AttributeVariable(
                    proto::QueryPatternVariable {
                        label: Some("a".to_string()),
                    },
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
    assert_eq!(query_resp.rows.len(), 3);
}
