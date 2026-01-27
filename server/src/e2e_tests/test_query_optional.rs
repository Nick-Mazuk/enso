//! Test optional patterns (left join semantics).

use crate::e2e_tests::helpers::{
    TestClient, get_number_at, get_string_at, is_ok, is_undefined_at, new_attribute_id,
    new_entity_id, new_hlc,
};
use crate::proto;

/// Test optional pattern where some entities have the optional attribute and some don't.
///
/// Setup:
/// - Entity 1: name="Alice", age=30
/// - Entity 2: name="Bob" (no age)
/// - Entity 3: name="Charlie", age=25
///
/// Query: find all names with optional age
/// Expected: 3 rows, Bob's age should be undefined
#[test]
fn test_query_optional_some_missing() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let entity2 = new_entity_id(2);
    let entity3 = new_entity_id(3);
    let name_attr = new_attribute_id(10);
    let age_attr = new_attribute_id(11);

    // Insert test data
    let insert_response = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    // Entity 1: Alice with age
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
                        attribute_id: Some(age_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(30.0)),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                    // Entity 2: Bob without age
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Bob".to_string())),
                        }),
                        hlc: Some(new_hlc(3)),
                    },
                    // Entity 3: Charlie with age
                    proto::Triple {
                        entity_id: Some(entity3.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Charlie".to_string())),
                        }),
                        hlc: Some(new_hlc(4)),
                    },
                    proto::Triple {
                        entity_id: Some(entity3.to_vec()),
                        attribute_id: Some(age_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(25.0)),
                        }),
                        hlc: Some(new_hlc(5)),
                    },
                ],
            },
        )),
    });
    assert!(is_ok(&insert_response));

    // Query with optional age
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![
                proto::QueryPatternVariable {
                    label: Some("name".to_string()),
                },
                proto::QueryPatternVariable {
                    label: Some("age".to_string()),
                },
            ],
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
            optional: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    age_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("age".to_string()),
                    },
                )),
            }],
            where_not: vec![],
        })),
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 3);
    assert_eq!(query_response.columns, vec!["name", "age"]);

    // Find Bob's row and verify age is undefined
    let bob_row = query_response
        .rows
        .iter()
        .enumerate()
        .find(|(_, row)| {
            row.values.first().is_some_and(|v| {
                matches!(
                    &v.value,
                    Some(proto::query_result_value::Value::TripleValue(tv))
                        if matches!(&tv.value, Some(proto::triple_value::Value::String(s)) if s == "Bob")
                )
            })
        })
        .map(|(i, _)| i);

    assert!(bob_row.is_some());
    let bob_idx = bob_row.unwrap();
    assert!(is_undefined_at(&query_response, bob_idx, 1)); // Bob's age is undefined

    // Verify Alice and Charlie have ages (not undefined)
    let alice_row = query_response
        .rows
        .iter()
        .enumerate()
        .find(|(_, row)| {
            row.values.first().is_some_and(|v| {
                matches!(
                    &v.value,
                    Some(proto::query_result_value::Value::TripleValue(tv))
                        if matches!(&tv.value, Some(proto::triple_value::Value::String(s)) if s == "Alice")
                )
            })
        })
        .map(|(i, _)| i);
    assert!(alice_row.is_some());
    let alice_idx = alice_row.unwrap();
    assert!(!is_undefined_at(&query_response, alice_idx, 1));
    assert_eq!(get_number_at(&query_response, alice_idx, 1), Some(30.0));

    let charlie_row = query_response
        .rows
        .iter()
        .enumerate()
        .find(|(_, row)| {
            row.values.first().is_some_and(|v| {
                matches!(
                    &v.value,
                    Some(proto::query_result_value::Value::TripleValue(tv))
                        if matches!(&tv.value, Some(proto::triple_value::Value::String(s)) if s == "Charlie")
                )
            })
        })
        .map(|(i, _)| i);
    assert!(charlie_row.is_some());
    let charlie_idx = charlie_row.unwrap();
    assert!(!is_undefined_at(&query_response, charlie_idx, 1));
    assert_eq!(get_number_at(&query_response, charlie_idx, 1), Some(25.0));
}

/// Test that optional with no base where results returns empty.
#[test]
fn test_query_optional_no_base_results() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let name_attr = new_attribute_id(10);
    let age_attr = new_attribute_id(11);
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
                        entity_id: Some(entity1.to_vec()),
                        attribute_id: Some(age_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(30.0)),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                ],
            },
        )),
    });
    assert!(is_ok(&insert_response));

    // Query with a where clause that matches nothing, plus optional
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![
                proto::QueryPatternVariable {
                    label: Some("name".to_string()),
                },
                proto::QueryPatternVariable {
                    label: Some("age".to_string()),
                },
            ],
            r#where: vec![proto::QueryPattern {
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
                        label: Some("name".to_string()),
                    },
                )),
            }],
            optional: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    age_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("age".to_string()),
                    },
                )),
            }],
            where_not: vec![],
        })),
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 0);
}

/// Test multiple optional patterns.
#[test]
fn test_query_multiple_optional_patterns() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let entity2 = new_entity_id(2);
    let name_attr = new_attribute_id(10);
    let age_attr = new_attribute_id(11);
    let dept_attr = new_attribute_id(12);

    // Insert test data:
    // Entity 1: name="Alice", age=30, dept="Engineering"
    // Entity 2: name="Bob", age=25 (no dept)
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
                        attribute_id: Some(age_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(30.0)),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                    proto::Triple {
                        entity_id: Some(entity1.to_vec()),
                        attribute_id: Some(dept_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String(
                                "Engineering".to_string(),
                            )),
                        }),
                        hlc: Some(new_hlc(3)),
                    },
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Bob".to_string())),
                        }),
                        hlc: Some(new_hlc(4)),
                    },
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(age_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(25.0)),
                        }),
                        hlc: Some(new_hlc(5)),
                    },
                ],
            },
        )),
    });
    assert!(is_ok(&insert_response));

    // Query with multiple optional patterns
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![
                proto::QueryPatternVariable {
                    label: Some("name".to_string()),
                },
                proto::QueryPatternVariable {
                    label: Some("age".to_string()),
                },
                proto::QueryPatternVariable {
                    label: Some("dept".to_string()),
                },
            ],
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
            optional: vec![
                proto::QueryPattern {
                    entity: Some(proto::query_pattern::Entity::EntityVariable(
                        proto::QueryPatternVariable {
                            label: Some("id".to_string()),
                        },
                    )),
                    attribute: Some(proto::query_pattern::Attribute::AttributeId(
                        age_attr.to_vec(),
                    )),
                    value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                        proto::QueryPatternVariable {
                            label: Some("age".to_string()),
                        },
                    )),
                },
                proto::QueryPattern {
                    entity: Some(proto::query_pattern::Entity::EntityVariable(
                        proto::QueryPatternVariable {
                            label: Some("id".to_string()),
                        },
                    )),
                    attribute: Some(proto::query_pattern::Attribute::AttributeId(
                        dept_attr.to_vec(),
                    )),
                    value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                        proto::QueryPatternVariable {
                            label: Some("dept".to_string()),
                        },
                    )),
                },
            ],
            where_not: vec![],
        })),
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 2);
    assert_eq!(query_response.columns, vec!["name", "age", "dept"]);

    // Find Bob's row and verify dept is undefined but age is present
    let bob_row = query_response
        .rows
        .iter()
        .enumerate()
        .find(|(_, row)| {
            row.values.first().is_some_and(|v| {
                matches!(
                    &v.value,
                    Some(proto::query_result_value::Value::TripleValue(tv))
                        if matches!(&tv.value, Some(proto::triple_value::Value::String(s)) if s == "Bob")
                )
            })
        })
        .map(|(i, _)| i);

    assert!(bob_row.is_some());
    let bob_idx = bob_row.unwrap();
    assert!(!is_undefined_at(&query_response, bob_idx, 1)); // Bob has age
    assert_eq!(get_number_at(&query_response, bob_idx, 1), Some(25.0));
    assert!(is_undefined_at(&query_response, bob_idx, 2)); // Bob's dept is undefined

    // Find Alice's row and verify both are present
    let alice_row = query_response
        .rows
        .iter()
        .enumerate()
        .find(|(_, row)| {
            row.values.first().is_some_and(|v| {
                matches!(
                    &v.value,
                    Some(proto::query_result_value::Value::TripleValue(tv))
                        if matches!(&tv.value, Some(proto::triple_value::Value::String(s)) if s == "Alice")
                )
            })
        })
        .map(|(i, _)| i);

    assert!(alice_row.is_some());
    let alice_idx = alice_row.unwrap();
    assert!(!is_undefined_at(&query_response, alice_idx, 1)); // Alice has age
    assert!(!is_undefined_at(&query_response, alice_idx, 2)); // Alice has dept
    assert_eq!(get_number_at(&query_response, alice_idx, 1), Some(30.0));
    assert_eq!(
        get_string_at(&query_response, alice_idx, 2),
        Some("Engineering")
    );
}
