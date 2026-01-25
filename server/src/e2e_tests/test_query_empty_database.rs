//! Test querying an empty database returns no results.

use crate::e2e_tests::helpers::{TestClient, is_ok, new_attribute_id, new_entity_id};
use crate::proto;

#[test]
fn test_query_empty_database() {
    let mut test = TestClient::new();

    let entity_id = new_entity_id(99);
    let attribute_id = new_attribute_id(99);

    let resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
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

    assert!(is_ok(&resp));
    assert_eq!(resp.rows.len(), 0);
}
