//! Test inserting many triples sequentially.

use crate::e2e_tests::helpers::{TestClient, is_ok, new_attribute_id, new_entity_id, new_hlc};
use crate::proto;

#[test]
fn test_many_sequential_inserts() {
    let mut client = TestClient::new();

    let entity_id = new_entity_id(60);

    // Insert 100 different attributes
    for i in 0..100u8 {
        let attribute_id = new_attribute_id(i);

        let resp = client.handle_message(proto::ClientMessage {
            request_id: Some(u32::from(i) + 1),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                proto::TripleUpdateRequest {
                    triples: vec![proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attribute_id.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(f64::from(i))),
                        }),
                        hlc: Some(new_hlc(u64::from(i) + 1)),
                    }],
                },
            )),
        });
        assert!(is_ok(&resp));
    }

    // Query all attributes
    let query_resp = client.handle_message(proto::ClientMessage {
        request_id: Some(101),
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
                entity: Some(proto::query_pattern::Entity::EntityId(entity_id.to_vec())),
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
    assert_eq!(query_resp.rows.len(), 100);
}
