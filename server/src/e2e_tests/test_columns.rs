//! Test that query responses include correct column names.

use crate::e2e_tests::helpers::{TestClient, is_ok, new_attribute_id, new_entity_id, new_hlc};
use crate::proto;

#[test]
fn test_query_returns_correct_columns() {
    let mut client = TestClient::new();

    let entity_id = new_entity_id(90);
    let attribute_id = new_attribute_id(90);

    // Insert a value
    let resp = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });
    assert!(is_ok(&resp));

    // Point query returns column "value"
    let point_resp = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
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
    assert!(is_ok(&point_resp));
    assert_eq!(point_resp.columns, vec!["value"]);

    // Entity scan returns columns "attribute" and "value"
    let scan_resp = client.handle_message(proto::ClientMessage {
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
    });
    assert!(is_ok(&scan_resp));
    assert_eq!(scan_resp.columns, vec!["attribute", "value"]);
}
