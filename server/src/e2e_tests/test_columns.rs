//! Test that query responses include correct column names.

use crate::e2e_tests::helpers::{TestClient, attribute_id, entity_id, is_ok};
use crate::proto;

#[test]
fn test_query_returns_correct_columns() {
    let test = TestClient::new();

    let eid = entity_id(90);
    let aid = attribute_id(90);

    // Insert a value
    let resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(eid.to_vec()),
                    attribute_id: Some(aid.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                }],
            },
        )),
    });
    assert!(is_ok(&resp));

    // Point query returns column "v"
    let point_resp = test.handle_message(proto::ClientMessage {
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
    assert!(is_ok(&point_resp));
    assert_eq!(point_resp.columns, vec!["v"]);

    // Entity scan returns columns "a" and "v"
    let scan_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(3),
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
    assert!(is_ok(&scan_resp));
    assert_eq!(scan_resp.columns, vec!["a", "v"]);
}
