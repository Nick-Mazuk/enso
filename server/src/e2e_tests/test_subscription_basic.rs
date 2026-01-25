//! End-to-end tests for subscription/change notification operations.
//!
//! These tests verify that change notifications are properly broadcast
//! when triples are inserted, updated, or deleted through the ClientConnection.

use crate::e2e_tests::helpers::{TestClient, is_ok, new_attribute_id, new_entity_id, new_hlc};
use crate::proto;
use crate::storage::HlcTimestamp;

/// Test that inserting a triple broadcasts a change notification.
#[test]
fn test_insert_broadcasts_change_notification() {
    let test = TestClient::new();
    let mut change_rx = test.subscribe_to_changes();

    let entity_id = new_entity_id(1);
    let attribute_id = new_attribute_id(1);

    // Insert a triple
    let insert_resp = test.handle_message(proto::ClientMessage {
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

    assert!(is_ok(&insert_resp));

    // Verify a change notification was broadcast
    let notification = change_rx.try_recv().expect("should receive notification");
    assert_eq!(notification.changes.len(), 1);

    let change = &notification.changes[0];
    assert_eq!(change.change_type, proto::ChangeType::Insert as i32);

    let triple = change.triple.as_ref().expect("should have triple");
    assert_eq!(triple.entity_id.as_ref(), Some(&entity_id.to_vec()));
    assert_eq!(triple.attribute_id.as_ref(), Some(&attribute_id.to_vec()));

    // Assert the value is correct
    let value = triple.value.as_ref().expect("should have value");
    assert_eq!(
        value.value,
        Some(proto::triple_value::Value::String("test".to_string()))
    );
}

/// Test that updating a triple broadcasts a change notification with Update type.
#[test]
fn test_update_broadcasts_change_notification() {
    let test = TestClient::new();

    let entity_id = new_entity_id(2);
    let attribute_id = new_attribute_id(2);

    // First insert
    let insert_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("initial".to_string())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });
    assert!(is_ok(&insert_resp));

    // Subscribe after the insert so we only see the update
    let mut change_rx = test.subscribe_to_changes();

    // Update the triple with a newer HLC
    let update_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("updated".to_string())),
                    }),
                    hlc: Some(new_hlc(2)),
                }],
            },
        )),
    });

    assert!(is_ok(&update_resp));

    // Verify an Update notification was broadcast
    let notification = change_rx.try_recv().expect("should receive notification");
    assert_eq!(notification.changes.len(), 1);

    let change = &notification.changes[0];
    assert_eq!(change.change_type, proto::ChangeType::Update as i32);

    let triple = change.triple.as_ref().expect("should have triple");
    assert_eq!(triple.entity_id.as_ref(), Some(&entity_id.to_vec()));
    assert_eq!(triple.attribute_id.as_ref(), Some(&attribute_id.to_vec()));

    // Assert the updated value is correct
    let value = triple.value.as_ref().expect("should have value");
    assert_eq!(
        value.value,
        Some(proto::triple_value::Value::String("updated".to_string()))
    );
}

/// Test that multiple triples in one request broadcast multiple changes.
#[test]
fn test_batch_insert_broadcasts_multiple_changes() {
    let test = TestClient::new();
    let mut change_rx = test.subscribe_to_changes();

    let entity_id_1 = new_entity_id(4);
    let entity_id_2 = new_entity_id(5);
    let attribute_id = new_attribute_id(4);

    // Insert two triples in one request
    let insert_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![
                    proto::Triple {
                        entity_id: Some(entity_id_1.to_vec()),
                        attribute_id: Some(attribute_id.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::String("first".to_string())),
                        }),
                        hlc: Some(new_hlc(1)),
                    },
                    proto::Triple {
                        entity_id: Some(entity_id_2.to_vec()),
                        attribute_id: Some(attribute_id.to_vec()),
                        value: Some(proto::TripleValue {
                            value: Some(proto::triple_value::Value::Number(42.0)),
                        }),
                        hlc: Some(new_hlc(1)),
                    },
                ],
            },
        )),
    });

    assert!(is_ok(&insert_resp));

    // Verify both changes were broadcast
    let notification = change_rx.try_recv().expect("should receive notification");
    assert_eq!(notification.changes.len(), 2);

    // First change
    assert_eq!(
        notification.changes[0].change_type,
        proto::ChangeType::Insert as i32
    );
    let triple1 = notification.changes[0]
        .triple
        .as_ref()
        .expect("should have triple");
    let value1 = triple1.value.as_ref().expect("should have value");
    assert_eq!(
        value1.value,
        Some(proto::triple_value::Value::String("first".to_string()))
    );

    // Second change
    assert_eq!(
        notification.changes[1].change_type,
        proto::ChangeType::Insert as i32
    );
    let triple2 = notification.changes[1]
        .triple
        .as_ref()
        .expect("should have triple");
    let value2 = triple2.value.as_ref().expect("should have value");
    assert_eq!(value2.value, Some(proto::triple_value::Value::Number(42.0)));
}

/// Test that `get_changes_since` returns changes after a given HLC.
#[test]
fn test_get_changes_since_returns_changes() {
    let test = TestClient::new();

    let entity_id = new_entity_id(6);
    let attribute_id = new_attribute_id(6);

    // Record the HLC before insert
    let before_hlc = HlcTimestamp {
        physical_time: 0,
        logical_counter: 0,
        node_id: 0,
    };

    // Insert a triple
    let insert_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(
                            "backfill test".to_string(),
                        )),
                    }),
                    hlc: Some(new_hlc(5)),
                }],
            },
        )),
    });

    assert!(is_ok(&insert_resp));

    // Get changes since before the insert
    let changes = test
        .get_changes_since(before_hlc)
        .expect("get_changes_since should succeed");

    // Should have at least the insert record (plus BEGIN/COMMIT markers)
    assert!(!changes.is_empty(), "should have changes since HLC 0");
}

/// Test that failed operations do not broadcast notifications.
#[test]
fn test_failed_operation_does_not_broadcast() {
    let test = TestClient::new();
    let mut change_rx = test.subscribe_to_changes();

    // Send an invalid request (missing entity_id)
    let invalid_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: None, // Invalid: missing entity_id
                    attribute_id: Some(new_attribute_id(7).to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("invalid".to_string())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });

    // Request should fail
    assert!(!is_ok(&invalid_resp));

    // No notification should be broadcast
    assert!(
        change_rx.try_recv().is_err(),
        "failed operations should not broadcast"
    );
}

/// Test that an older HLC update does not broadcast (conflict resolution).
#[test]
fn test_older_hlc_update_does_not_broadcast() {
    let test = TestClient::new();

    let entity_id = new_entity_id(8);
    let attribute_id = new_attribute_id(8);

    // Insert with HLC 10
    let insert_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("newer".to_string())),
                    }),
                    hlc: Some(new_hlc(10)),
                }],
            },
        )),
    });
    assert!(is_ok(&insert_resp));

    // Subscribe after the insert
    let mut change_rx = test.subscribe_to_changes();

    // Try to update with older HLC (should be rejected by conflict resolution)
    let old_update_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("older".to_string())),
                    }),
                    hlc: Some(new_hlc(5)), // Older than 10
                }],
            },
        )),
    });

    // The request succeeds but the triple is not updated (conflict resolution)
    assert!(is_ok(&old_update_resp));

    // No notification should be broadcast since the triple wasn't actually changed
    assert!(
        change_rx.try_recv().is_err(),
        "older HLC updates should not broadcast"
    );
}

/// Test inserting a boolean value broadcasts correctly.
#[test]
fn test_insert_boolean_broadcasts_correctly() {
    let test = TestClient::new();
    let mut change_rx = test.subscribe_to_changes();

    let entity_id = new_entity_id(9);
    let attribute_id = new_attribute_id(9);

    // Insert a boolean triple
    let insert_resp = test.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Boolean(true)),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });

    assert!(is_ok(&insert_resp));

    // Verify the boolean value is correct in the notification
    let notification = change_rx.try_recv().expect("should receive notification");
    let triple = notification.changes[0]
        .triple
        .as_ref()
        .expect("should have triple");
    let value = triple.value.as_ref().expect("should have value");
    assert_eq!(value.value, Some(proto::triple_value::Value::Boolean(true)));
}
