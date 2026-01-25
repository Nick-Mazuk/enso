//! End-to-end tests for `ConnectRequest` flow.
//!
//! These tests verify:
//! 1. `ConnectRequest` must be the first message
//! 2. `ConnectRequest` creates/opens the correct database
//! 3. Operations work after successful connect
//! 4. Errors are returned for invalid states

use std::sync::Arc;

use crate::client_connection::ClientConnection;
use crate::database_registry::DatabaseRegistry;
use crate::e2e_tests::helpers::{new_attribute_id, new_entity_id, new_hlc};
use crate::proto;

/// Create a test registry with a temporary directory.
fn create_test_registry() -> (tempfile::TempDir, Arc<DatabaseRegistry>) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let registry = Arc::new(DatabaseRegistry::new(dir.path().to_path_buf()));
    (dir, registry)
}

/// Test that `ConnectRequest` succeeds and transitions state.
#[test]
fn test_connect_request_succeeds() {
    let (_dir, registry) = create_test_registry();
    let mut conn = ClientConnection::new_awaiting_connect(registry);

    assert!(!conn.is_connected());

    let response = conn.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::Connect(
            proto::ConnectRequest {
                app_api_key: "test_app".to_string(),
            },
        )),
    });

    assert_eq!(response.len(), 1);
    match &response[0].payload {
        Some(proto::server_message::Payload::Response(r)) => {
            assert_eq!(
                r.status.as_ref().unwrap().code,
                proto::google::rpc::Code::Ok as i32
            );
        }
        _ => panic!("Expected Response"),
    }

    assert!(conn.is_connected());
}

/// Test that operations before `ConnectRequest` fail with `FailedPrecondition`.
#[test]
fn test_operation_before_connect_fails() {
    let (_dir, registry) = create_test_registry();
    let mut conn = ClientConnection::new_awaiting_connect(registry);

    // Try to send a query before connecting
    let response = conn.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::Query(proto::QueryRequest {
            find: vec![],
            r#where: vec![],
            optional: vec![],
            where_not: vec![],
        })),
    });

    assert_eq!(response.len(), 1);
    match &response[0].payload {
        Some(proto::server_message::Payload::Response(r)) => {
            assert_eq!(
                r.status.as_ref().unwrap().code,
                proto::google::rpc::Code::FailedPrecondition as i32
            );
            assert!(
                r.status
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("ConnectRequest")
            );
        }
        _ => panic!("Expected Response with error"),
    }

    // Connection should still be in AwaitingConnect state
    assert!(!conn.is_connected());
}

/// Test that update before `ConnectRequest` fails with `FailedPrecondition`.
#[test]
fn test_update_before_connect_fails() {
    let (_dir, registry) = create_test_registry();
    let mut conn = ClientConnection::new_awaiting_connect(registry);

    let entity_id = new_entity_id(1);
    let attribute_id = new_attribute_id(1);

    let response = conn.handle_message(proto::ClientMessage {
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

    assert_eq!(response.len(), 1);
    match &response[0].payload {
        Some(proto::server_message::Payload::Response(r)) => {
            assert_eq!(
                r.status.as_ref().unwrap().code,
                proto::google::rpc::Code::FailedPrecondition as i32
            );
        }
        _ => panic!("Expected Response with error"),
    }
}

/// Test that sending `ConnectRequest` twice fails.
#[test]
fn test_double_connect_fails() {
    let (_dir, registry) = create_test_registry();
    let mut conn = ClientConnection::new_awaiting_connect(registry);

    // First connect succeeds
    let _ = conn.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::Connect(
            proto::ConnectRequest {
                app_api_key: "test_app".to_string(),
            },
        )),
    });

    assert!(conn.is_connected());

    // Second connect fails
    let response = conn.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::Connect(
            proto::ConnectRequest {
                app_api_key: "other_app".to_string(),
            },
        )),
    });

    match &response[0].payload {
        Some(proto::server_message::Payload::Response(r)) => {
            assert_eq!(
                r.status.as_ref().unwrap().code,
                proto::google::rpc::Code::FailedPrecondition as i32
            );
            assert!(
                r.status
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("Already connected")
            );
        }
        _ => panic!("Expected Response with error"),
    }
}

/// Test that empty `app_api_key` fails with `InvalidArgument`.
#[test]
fn test_empty_api_key_fails() {
    let (_dir, registry) = create_test_registry();
    let mut conn = ClientConnection::new_awaiting_connect(registry);

    let response = conn.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::Connect(
            proto::ConnectRequest {
                app_api_key: String::new(),
            },
        )),
    });

    match &response[0].payload {
        Some(proto::server_message::Payload::Response(r)) => {
            assert_eq!(
                r.status.as_ref().unwrap().code,
                proto::google::rpc::Code::InvalidArgument as i32
            );
            assert!(r.status.as_ref().unwrap().message.contains("empty"));
        }
        _ => panic!("Expected Response with error"),
    }

    // Connection should still be in AwaitingConnect state
    assert!(!conn.is_connected());
}

/// Test that invalid characters in `api_key` fail.
#[test]
fn test_invalid_api_key_characters_fail() {
    let (_dir, registry) = create_test_registry();
    let mut conn = ClientConnection::new_awaiting_connect(registry);

    // Path traversal attempt
    let response = conn.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::Connect(
            proto::ConnectRequest {
                app_api_key: "../evil/path".to_string(),
            },
        )),
    });

    match &response[0].payload {
        Some(proto::server_message::Payload::Response(r)) => {
            assert_eq!(
                r.status.as_ref().unwrap().code,
                proto::google::rpc::Code::InvalidArgument as i32
            );
            assert!(
                r.status
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("invalid characters")
            );
        }
        _ => panic!("Expected Response with error"),
    }

    assert!(!conn.is_connected());
}

/// Test that operations work after successful connect.
#[test]
fn test_operations_after_connect_work() {
    let (_dir, registry) = create_test_registry();
    let mut conn = ClientConnection::new_awaiting_connect(registry);

    // Connect
    let _ = conn.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::Connect(
            proto::ConnectRequest {
                app_api_key: "test_app".to_string(),
            },
        )),
    });

    assert!(conn.is_connected());

    // Insert should work
    let entity_id = new_entity_id(1);
    let attribute_id = new_attribute_id(1);

    let response = conn.handle_message(proto::ClientMessage {
        request_id: Some(2),
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

    assert_eq!(response.len(), 1);
    match &response[0].payload {
        Some(proto::server_message::Payload::Response(r)) => {
            assert_eq!(
                r.status.as_ref().unwrap().code,
                proto::google::rpc::Code::Ok as i32
            );
        }
        _ => panic!("Expected successful Response"),
    }
}

/// Test that different `api_keys` create different databases.
#[test]
fn test_different_api_keys_create_different_databases() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let registry = Arc::new(DatabaseRegistry::new(dir.path().to_path_buf()));

    let entity_id = new_entity_id(1);
    let attribute_id = new_attribute_id(1);

    // First connection to app1
    {
        let mut conn1 = ClientConnection::new_awaiting_connect(Arc::clone(&registry));
        let _ = conn1.handle_message(proto::ClientMessage {
            request_id: Some(1),
            payload: Some(proto::client_message::Payload::Connect(
                proto::ConnectRequest {
                    app_api_key: "app1".to_string(),
                },
            )),
        });

        // Insert data
        let _ = conn1.handle_message(proto::ClientMessage {
            request_id: Some(2),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                proto::TripleUpdateRequest {
                    triples: vec![proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attribute_id.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String(
                                "app1_data".to_string(),
                            )),
                        }),
                        hlc: Some(new_hlc(1)),
                    }],
                },
            )),
        });
    }

    // Second connection to app2 should not see app1's data
    {
        let mut conn2 = ClientConnection::new_awaiting_connect(Arc::clone(&registry));
        let _ = conn2.handle_message(proto::ClientMessage {
            request_id: Some(1),
            payload: Some(proto::client_message::Payload::Connect(
                proto::ConnectRequest {
                    app_api_key: "app2".to_string(),
                },
            )),
        });

        // Query for the entity - should be empty in app2
        let response = conn2.handle_message(proto::ClientMessage {
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

        assert_eq!(response.len(), 1);
        match &response[0].payload {
            Some(proto::server_message::Payload::Response(r)) => {
                assert!(r.rows.is_empty(), "app2 should not see app1's data");
            }
            _ => panic!("Expected Response"),
        }
    }

    // Verify both database files exist
    assert!(dir.path().join("app1.db").exists());
    assert!(dir.path().join("app2.db").exists());
}

/// Test that same `api_key` shares the database across connections.
#[test]
fn test_same_api_key_shares_database() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let registry = Arc::new(DatabaseRegistry::new(dir.path().to_path_buf()));

    let entity_id = new_entity_id(1);
    let attribute_id = new_attribute_id(1);

    // First connection inserts data
    {
        let mut conn1 = ClientConnection::new_awaiting_connect(Arc::clone(&registry));
        let _ = conn1.handle_message(proto::ClientMessage {
            request_id: Some(1),
            payload: Some(proto::client_message::Payload::Connect(
                proto::ConnectRequest {
                    app_api_key: "shared_app".to_string(),
                },
            )),
        });

        let _ = conn1.handle_message(proto::ClientMessage {
            request_id: Some(2),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                proto::TripleUpdateRequest {
                    triples: vec![proto::Triple {
                        entity_id: Some(entity_id.to_vec()),
                        attribute_id: Some(attribute_id.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String(
                                "shared_data".to_string(),
                            )),
                        }),
                        hlc: Some(new_hlc(1)),
                    }],
                },
            )),
        });
    }

    // Second connection with same api_key should see the data
    {
        let mut conn2 = ClientConnection::new_awaiting_connect(Arc::clone(&registry));
        let _ = conn2.handle_message(proto::ClientMessage {
            request_id: Some(1),
            payload: Some(proto::client_message::Payload::Connect(
                proto::ConnectRequest {
                    app_api_key: "shared_app".to_string(),
                },
            )),
        });

        // Query for the entity - should see the data
        let response = conn2.handle_message(proto::ClientMessage {
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

        assert_eq!(response.len(), 1);
        match &response[0].payload {
            Some(proto::server_message::Payload::Response(r)) => {
                assert_eq!(r.rows.len(), 1, "conn2 should see conn1's data");

                // Verify the value
                let row = &r.rows[0];
                assert_eq!(row.values.len(), 1);
                match &row.values[0].value {
                    Some(proto::query_result_value::Value::TripleValue(tv)) => {
                        assert_eq!(
                            tv.value,
                            Some(proto::triple_value::Value::String(
                                "shared_data".to_string()
                            ))
                        );
                    }
                    _ => panic!("Expected TripleValue"),
                }
            }
            _ => panic!("Expected Response"),
        }
    }

    // Only one database file should exist
    assert!(dir.path().join("shared_app.db").exists());
}
