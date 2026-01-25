//! Test a sequence of operations: insert, query, update, query.

use crate::e2e_tests::helpers::{
    TestClient, get_number_value, is_ok, new_attribute_id, new_entity_id, new_hlc,
};
use crate::proto;

#[test]
fn test_sequence_insert_query_update_query() {
    let test = TestClient::new();

    let entity_id = new_entity_id(50);
    let attribute_id = new_attribute_id(50);

    // Step 1: Insert
    let resp1 = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Number(1.0)),
                    }),
                    hlc: Some(new_hlc(1)),
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
    assert!(is_ok(&resp2));
    assert_eq!(resp2.rows.len(), 1);
    assert_eq!(get_number_value(&resp2, 0), Some(1.0));

    // Step 3: Update
    let resp3 = test.handle_message(proto::ClientMessage {
        request_id: Some(3),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Number(2.0)),
                    }),
                    hlc: Some(new_hlc(2)),
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
    assert!(is_ok(&resp4));
    assert_eq!(resp4.rows.len(), 1);
    assert_eq!(get_number_value(&resp4, 0), Some(2.0));
}
