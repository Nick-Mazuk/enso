//! Test `where_not` patterns (anti-join semantics).

use crate::e2e_tests::helpers::{
    TestClient, get_string_at, is_ok, new_attribute_id, new_entity_id, new_hlc,
};
use crate::proto;

/// Test `where_not` to exclude entities that have a specific attribute.
///
/// Setup:
/// - Entity 1: name="Alice", active=true
/// - Entity 2: name="Bob", active=false
/// - Entity 3: name="Charlie" (no active field)
///
/// Query: find entities WITHOUT the active attribute
/// Expected: 1 row (Charlie)
#[test]
fn test_query_where_not_excludes_attribute() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let entity2 = new_entity_id(2);
    let entity3 = new_entity_id(3);
    let name_attr = new_attribute_id(10);
    let active_attr = new_attribute_id(11);

    // Insert test data
    let insert_response = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    // Entity 1: Alice with active=true
                    proto::Triple {
                        entity_id: Some(entity1.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Alice".to_string())),
                        }),
                        hlc: Some(new_hlc(1)),
                    },
                    proto::Triple {
                        entity_id: Some(entity1.to_vec()),
                        attribute_id: Some(active_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(true)),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                    // Entity 2: Bob with active=false
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Bob".to_string())),
                        }),
                        hlc: Some(new_hlc(3)),
                    },
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(active_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(false)),
                        }),
                        hlc: Some(new_hlc(4)),
                    },
                    // Entity 3: Charlie with no active field
                    proto::Triple {
                        entity_id: Some(entity3.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Charlie".to_string())),
                        }),
                        hlc: Some(new_hlc(5)),
                    },
                ],
            },
        )),
    });
    assert!(is_ok(&insert_response));

    // Query: entities that do NOT have the active attribute
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![proto::QueryPatternVariable {
                label: Some("name".to_string()),
            }],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    name_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("name".to_string()),
                    },
                )),
            }],
            optional: vec![],
            where_not: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    active_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("_active".to_string()),
                    },
                )),
            }],
        })),
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 1);
    assert_eq!(get_string_at(&query_response, 0, 0), Some("Charlie"));
}

/// Test `where_not` to exclude entities with a specific value.
///
/// Setup:
/// - Entity 1: name="Alice", active=true
/// - Entity 2: name="Bob", active=false
/// - Entity 3: name="Charlie" (no active field)
///
/// Query: find entities where active is NOT true
/// Expected: 2 rows (Bob and Charlie)
#[test]
#[allow(clippy::too_many_lines)]
fn test_query_where_not_excludes_value() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let entity2 = new_entity_id(2);
    let entity3 = new_entity_id(3);
    let name_attr = new_attribute_id(10);
    let active_attr = new_attribute_id(11);

    // Insert test data
    let insert_response = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    proto::Triple {
                        entity_id: Some(entity1.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Alice".to_string())),
                        }),
                        hlc: Some(new_hlc(1)),
                    },
                    proto::Triple {
                        entity_id: Some(entity1.to_vec()),
                        attribute_id: Some(active_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(true)),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Bob".to_string())),
                        }),
                        hlc: Some(new_hlc(3)),
                    },
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(active_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(false)),
                        }),
                        hlc: Some(new_hlc(4)),
                    },
                    proto::Triple {
                        entity_id: Some(entity3.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Charlie".to_string())),
                        }),
                        hlc: Some(new_hlc(5)),
                    },
                ],
            },
        )),
    });
    assert!(is_ok(&insert_response));

    // Query: entities where active is NOT true (using concrete value in where_not)
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![proto::QueryPatternVariable {
                label: Some("name".to_string()),
            }],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    name_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("name".to_string()),
                    },
                )),
            }],
            optional: vec![],
            where_not: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    active_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::Value(
                    proto::TripleValue {
                        value: Some(proto::triple_value::Value::Boolean(true)),
                    },
                )),
            }],
        })),
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 2);

    // Verify we got Bob and Charlie
    let names: Vec<&str> = (0..2)
        .filter_map(|i| get_string_at(&query_response, i, 0))
        .collect();
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
    assert!(!names.contains(&"Alice"));
}

/// Test `where_not` with no matches (all rows pass through).
#[test]
fn test_query_where_not_no_exclusions() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let entity2 = new_entity_id(2);
    let name_attr = new_attribute_id(10);
    let nonexistent_attr = new_attribute_id(99);

    // Insert test data
    let insert_response = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    proto::Triple {
                        entity_id: Some(entity1.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Alice".to_string())),
                        }),
                        hlc: Some(new_hlc(1)),
                    },
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Bob".to_string())),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                ],
            },
        )),
    });
    assert!(is_ok(&insert_response));

    // Query: exclude entities with nonexistent attribute (none have it, so all pass)
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![proto::QueryPatternVariable {
                label: Some("name".to_string()),
            }],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    name_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("name".to_string()),
                    },
                )),
            }],
            optional: vec![],
            where_not: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    nonexistent_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("_val".to_string()),
                    },
                )),
            }],
        })),
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 2);
}

/// Test `where_not` excludes all rows.
#[test]
fn test_query_where_not_excludes_all() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let entity2 = new_entity_id(2);
    let name_attr = new_attribute_id(10);

    // Insert test data
    let insert_response = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    proto::Triple {
                        entity_id: Some(entity1.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Alice".to_string())),
                        }),
                        hlc: Some(new_hlc(1)),
                    },
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Bob".to_string())),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                ],
            },
        )),
    });
    assert!(is_ok(&insert_response));

    // Query: exclude entities with name attribute (all have it, so all excluded)
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![proto::QueryPatternVariable {
                label: Some("name".to_string()),
            }],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    name_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("name".to_string()),
                    },
                )),
            }],
            optional: vec![],
            where_not: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    name_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("_name".to_string()),
                    },
                )),
            }],
        })),
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 0);
}
