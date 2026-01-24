//! Test a sequence of operations: insert, query, update, query.

use crate::e2e_tests::helpers::{TestClient, attribute_id, entity_id, get_number_value, is_ok};
use crate::proto;

#[test]
fn test_sequence_insert_query_update_query() {
    let test = TestClient::new();

    let eid = entity_id(50);
    let aid = attribute_id(50);

    // Step 1: Insert
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(eid.to_vec()),
                    attribute_id: Some(aid.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Number(1.0)),
                    }),
                }],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Step 2: Query (should see 1.0)
    let resp2 = test.handle_message(proto::ClientMessage {
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
    assert!(is_ok(&resp2));
    assert_eq!(resp2.rows.len(), 1);
    assert_eq!(get_number_value(&resp2, 0), Some(1.0));

    // Step 3: Update
    let resp3 = test.handle_message(proto::ClientMessage {
        request_id: Some(3),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(eid.to_vec()),
                    attribute_id: Some(aid.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Number(2.0)),
                    }),
                }],
            },
        )),
    });
    assert!(is_ok(&resp3));

    // Step 4: Query (should see 2.0)
    let resp4 = test.handle_message(proto::ClientMessage {
        request_id: Some(4),
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
    assert!(is_ok(&resp4));
    assert_eq!(resp4.rows.len(), 1);
    assert_eq!(get_number_value(&resp4, 0), Some(2.0));
}
