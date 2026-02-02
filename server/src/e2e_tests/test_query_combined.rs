//! Test combined query features (find, where, optional, `where_not` together).

use crate::e2e_tests::helpers::{
    TestClient, get_string_at, is_ok, is_undefined_at, new_attribute_id, new_entity_id, new_hlc,
};
use crate::proto;

/// Test combining where, optional, and `where_not` patterns.
///
/// Setup:
/// - User 1: name="Alice", dept="Engineering"
/// - User 2: name="Bob" (no dept)
/// - User 3: name="Charlie" (no dept)
/// - User 4: name="Dave", dept="HR", inactive=true
///
/// Query: Active users with optional dept, excluding inactive
/// Expected: 3 rows (Alice, Bob, Charlie - excluding Dave)
#[test]
#[allow(clippy::too_many_lines)]
fn test_query_combined_features() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let entity2 = new_entity_id(2);
    let entity3 = new_entity_id(3);
    let entity4 = new_entity_id(4);
    let name_attr = new_attribute_id(10);
    let dept_attr = new_attribute_id(11);
    let inactive_attr = new_attribute_id(12);

    // Insert test data
    let insert_response = client.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    // User 1: Alice with dept
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
                        attribute_id: Some(dept_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String(
                                "Engineering".to_string(),
                            )),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                    // User 2: Bob without dept
                    proto::Triple {
                        entity_id: Some(entity2.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Bob".to_string())),
                        }),
                        hlc: Some(new_hlc(3)),
                    },
                    // User 3: Charlie without dept
                    proto::Triple {
                        entity_id: Some(entity3.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Charlie".to_string())),
                        }),
                        hlc: Some(new_hlc(4)),
                    },
                    // User 4: Dave with dept and inactive
                    proto::Triple {
                        entity_id: Some(entity4.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Dave".to_string())),
                        }),
                        hlc: Some(new_hlc(5)),
                    },
                    proto::Triple {
                        entity_id: Some(entity4.to_vec()),
                        attribute_id: Some(dept_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("HR".to_string())),
                        }),
                        hlc: Some(new_hlc(6)),
                    },
                    proto::Triple {
                        entity_id: Some(entity4.to_vec()),
                        attribute_id: Some(inactive_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(true)),
                        }),
                        hlc: Some(new_hlc(7)),
                    },
                ],
            },
        )),
    });
    assert!(is_ok(&insert_response));

    // Query: users with optional dept, excluding inactive
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![
                proto::QueryPatternVariable {
                    label: Some("name".to_string()),
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
            optional: vec![proto::QueryPattern {
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
            }],
            where_not: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    inactive_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("_inactive".to_string()),
                    },
                )),
            }],
        })),
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 3);
    assert_eq!(query_response.columns, vec!["name", "dept"]);

    // Collect all names
    let names: Vec<&str> = (0..3)
        .filter_map(|i| get_string_at(&query_response, i, 0))
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
    assert!(!names.contains(&"Dave")); // Dave is excluded by where_not

    // Find each row and verify dept values
    for i in 0..3 {
        let name = get_string_at(&query_response, i, 0);
        match name {
            Some("Alice") => {
                assert!(!is_undefined_at(&query_response, i, 1));
                assert_eq!(get_string_at(&query_response, i, 1), Some("Engineering"));
            }
            Some("Bob" | "Charlie") => {
                assert!(is_undefined_at(&query_response, i, 1));
            }
            _ => panic!("Unexpected name: {name:?}"),
        }
    }
}

/// Test `where_not` with multiple patterns (all must not match).
#[test]
#[allow(clippy::too_many_lines)]
fn test_query_combined_multiple_where_not() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let entity2 = new_entity_id(2);
    let entity3 = new_entity_id(3);
    let name_attr = new_attribute_id(10);
    let deleted_attr = new_attribute_id(11);
    let archived_attr = new_attribute_id(12);

    // Insert test data:
    // Entity 1: name="Alice", deleted=true
    // Entity 2: name="Bob", archived=true
    // Entity 3: name="Charlie" (no deleted or archived)
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
                        attribute_id: Some(deleted_attr.to_vec()),
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
                        attribute_id: Some(archived_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(true)),
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

    // Query: exclude deleted AND archived
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
            where_not: vec![
                proto::QueryPattern {
                    entity: Some(proto::query_pattern::Entity::EntityVariable(
                        proto::QueryPatternVariable {
                            label: Some("id".to_string()),
                        },
                    )),
                    attribute: Some(proto::query_pattern::Attribute::AttributeId(
                        deleted_attr.to_vec(),
                    )),
                    value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                        proto::QueryPatternVariable {
                            label: Some("_deleted".to_string()),
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
                        archived_attr.to_vec(),
                    )),
                    value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                        proto::QueryPatternVariable {
                            label: Some("_archived".to_string()),
                        },
                    )),
                },
            ],
        })),
    });

    assert!(is_ok(&query_response));
    assert_eq!(query_response.rows.len(), 1);
    assert_eq!(get_string_at(&query_response, 0, 0), Some("Charlie"));
}

/// Test multiple where patterns with optional and `where_not`.
#[test]
#[allow(clippy::too_many_lines)]
fn test_query_combined_multiple_where() {
    let mut client = TestClient::new();

    let entity1 = new_entity_id(1);
    let entity2 = new_entity_id(2);
    let entity3 = new_entity_id(3);
    let name_attr = new_attribute_id(10);
    let role_attr = new_attribute_id(11);
    let email_attr = new_attribute_id(12);
    let inactive_attr = new_attribute_id(13);

    // Insert test data:
    // Entity 1: name="Alice", role="admin", email="alice@example.com"
    // Entity 2: name="Bob", role="user" (no email)
    // Entity 3: name="Charlie", role="admin", inactive=true
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
                        attribute_id: Some(role_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("admin".to_string())),
                        }),
                        hlc: Some(new_hlc(2)),
                    },
                    proto::Triple {
                        entity_id: Some(entity1.to_vec()),
                        attribute_id: Some(email_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String(
                                "alice@example.com".to_string(),
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
                        attribute_id: Some(role_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("user".to_string())),
                        }),
                        hlc: Some(new_hlc(5)),
                    },
                    proto::Triple {
                        entity_id: Some(entity3.to_vec()),
                        attribute_id: Some(name_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("Charlie".to_string())),
                        }),
                        hlc: Some(new_hlc(6)),
                    },
                    proto::Triple {
                        entity_id: Some(entity3.to_vec()),
                        attribute_id: Some(role_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("admin".to_string())),
                        }),
                        hlc: Some(new_hlc(7)),
                    },
                    proto::Triple {
                        entity_id: Some(entity3.to_vec()),
                        attribute_id: Some(inactive_attr.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Boolean(true)),
                        }),
                        hlc: Some(new_hlc(8)),
                    },
                ],
            },
        )),
    });
    assert!(is_ok(&insert_response));

    // Query: admins with optional email, excluding inactive
    let query_response = client.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![
                proto::QueryPatternVariable {
                    label: Some("name".to_string()),
                },
                proto::QueryPatternVariable {
                    label: Some("email".to_string()),
                },
            ],
            r#where: vec![
                proto::QueryPattern {
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
                },
                proto::QueryPattern {
                    entity: Some(proto::query_pattern::Entity::EntityVariable(
                        proto::QueryPatternVariable {
                            label: Some("id".to_string()),
                        },
                    )),
                    attribute: Some(proto::query_pattern::Attribute::AttributeId(
                        role_attr.to_vec(),
                    )),
                    value_group: Some(proto::query_pattern::ValueGroup::Value(
                        proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("admin".to_string())),
                        },
                    )),
                },
            ],
            optional: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    email_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("email".to_string()),
                    },
                )),
            }],
            where_not: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(
                    proto::QueryPatternVariable {
                        label: Some("id".to_string()),
                    },
                )),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    inactive_attr.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(
                    proto::QueryPatternVariable {
                        label: Some("_inactive".to_string()),
                    },
                )),
            }],
        })),
    });

    assert!(is_ok(&query_response));
    // Only Alice should match (admin + not inactive)
    // Bob is not admin, Charlie is admin but inactive
    assert_eq!(query_response.rows.len(), 1);
    assert_eq!(get_string_at(&query_response, 0, 0), Some("Alice"));
    assert!(!is_undefined_at(&query_response, 0, 1));
    assert_eq!(
        get_string_at(&query_response, 0, 1),
        Some("alice@example.com")
    );
}
