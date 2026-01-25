//! End-to-end tests for multi-connection subscription scenarios.
//!
//! These tests verify that change notifications are properly broadcast
//! to ALL subscribers when multiple `ClientConnection` instances share
//! the same database.
//!
//! This tests the database-level broadcasting behavior where the broadcast
//! channel is owned by the Database, ensuring all connections receive updates.

use crate::e2e_tests::helpers::{TestClient, is_ok, new_attribute_id, new_entity_id, new_hlc};
use crate::proto;
use crate::storage::{ChangeType, TripleValue};

/// Test that a subscriber on one connection receives updates from another connection.
///
/// This is the core test for multi-connection broadcasting: two separate
/// `ClientConnection` instances share the same database, and when one
/// writes, the other's subscriber receives the notification.
#[test]
fn test_sibling_connection_receives_notification() {
    let client1 = TestClient::new();
    let client2 = client1.create_sibling();

    // Subscribe on client2 (the sibling)
    let mut rx2 = client2.subscribe_to_changes();

    let entity_id = new_entity_id(200);
    let attribute_id = new_attribute_id(200);

    // Insert via client1
    let insert_resp = client1.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(
                            "from client1".to_string(),
                        )),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });

    assert!(is_ok(&insert_resp));

    // client2's subscriber should receive the notification
    let notification = rx2
        .try_recv()
        .expect("client2 should receive notification from client1");
    assert_eq!(notification.changes.len(), 1);
    assert_eq!(notification.changes[0].change_type, ChangeType::Insert);
    assert_eq!(
        notification.changes[0].value,
        Some(TripleValue::String("from client1".to_string()))
    );
}

/// Test bidirectional notifications between two connections.
///
/// Both connections can write and receive notifications from each other.
#[test]
fn test_bidirectional_notifications() {
    let client1 = TestClient::new();
    let client2 = client1.create_sibling();

    // Both subscribe
    let mut rx1 = client1.subscribe_to_changes();
    let mut rx2 = client2.subscribe_to_changes();

    let attribute_id = new_attribute_id(201);

    // Client1 writes
    let entity1 = new_entity_id(201);
    let resp1 = client1.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity1.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(
                            "from client1".to_string(),
                        )),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // Both should receive client1's write
    let notif1_from_c1 = rx1.try_recv().expect("rx1 should receive client1's write");
    let notif2_from_c1 = rx2.try_recv().expect("rx2 should receive client1's write");
    assert_eq!(
        notif1_from_c1.changes[0].value,
        Some(TripleValue::String("from client1".to_string()))
    );
    assert_eq!(
        notif2_from_c1.changes[0].value,
        Some(TripleValue::String("from client1".to_string()))
    );

    // Client2 writes
    let entity2 = new_entity_id(202);
    let resp2 = client2.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity2.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(
                            "from client2".to_string(),
                        )),
                    }),
                    hlc: Some(new_hlc(2)),
                }],
            },
        )),
    });
    assert!(is_ok(&resp2));

    // Both should receive client2's write
    let notif1_from_c2 = rx1.try_recv().expect("rx1 should receive client2's write");
    let notif2_from_c2 = rx2.try_recv().expect("rx2 should receive client2's write");
    assert_eq!(
        notif1_from_c2.changes[0].value,
        Some(TripleValue::String("from client2".to_string()))
    );
    assert_eq!(
        notif2_from_c2.changes[0].value,
        Some(TripleValue::String("from client2".to_string()))
    );
}

/// Test multiple sibling connections all receive notifications.
#[test]
fn test_multiple_siblings_all_receive_notifications() {
    let client1 = TestClient::new();
    let client2 = client1.create_sibling();
    let client3 = client1.create_sibling();

    // All three subscribe
    let mut rx1 = client1.subscribe_to_changes();
    let mut rx2 = client2.subscribe_to_changes();
    let mut rx3 = client3.subscribe_to_changes();

    let entity_id = new_entity_id(210);
    let attribute_id = new_attribute_id(210);

    // Client2 writes
    let insert_resp = client2.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(
                            "from client2".to_string(),
                        )),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });

    assert!(is_ok(&insert_resp));

    // All three should receive the notification
    let notif1 = rx1.try_recv().expect("rx1 should receive notification");
    let notif2 = rx2.try_recv().expect("rx2 should receive notification");
    let notif3 = rx3.try_recv().expect("rx3 should receive notification");

    assert_eq!(notif1.changes.len(), 1);
    assert_eq!(notif2.changes.len(), 1);
    assert_eq!(notif3.changes.len(), 1);

    assert_eq!(
        notif1.changes[0].value,
        Some(TripleValue::String("from client2".to_string()))
    );
    assert_eq!(
        notif2.changes[0].value,
        Some(TripleValue::String("from client2".to_string()))
    );
    assert_eq!(
        notif3.changes[0].value,
        Some(TripleValue::String("from client2".to_string()))
    );
}

/// Test that late-subscribing sibling doesn't receive past notifications.
#[test]
fn test_late_sibling_subscriber() {
    let client1 = TestClient::new();
    let client2 = client1.create_sibling();

    // Only client1 subscribes initially
    let mut rx1 = client1.subscribe_to_changes();

    let entity_id = new_entity_id(220);
    let attribute_id = new_attribute_id(220);

    // Client1 writes
    let resp1 = client1.handle_message(proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("before".to_string())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    });
    assert!(is_ok(&resp1));

    // rx1 should have received it
    assert!(rx1.try_recv().is_ok(), "rx1 should receive the first write");

    // Now client2 subscribes (late)
    let mut rx2 = client2.subscribe_to_changes();

    // rx2 should NOT have the first notification
    assert!(
        rx2.try_recv().is_err(),
        "rx2 should not receive notifications from before subscribing"
    );

    // Another write
    let entity_id2 = new_entity_id(221);
    let resp2 = client2.handle_message(proto::ClientMessage {
        request_id: Some(2),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(entity_id2.to_vec()),
                    attribute_id: Some(attribute_id.to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("after".to_string())),
                    }),
                    hlc: Some(new_hlc(2)),
                }],
            },
        )),
    });
    assert!(is_ok(&resp2));

    // Both should receive this one
    let notif1 = rx1.try_recv().expect("rx1 should receive second write");
    let notif2 = rx2.try_recv().expect("rx2 should receive second write");

    assert_eq!(
        notif1.changes[0].value,
        Some(TripleValue::String("after".to_string()))
    );
    assert_eq!(
        notif2.changes[0].value,
        Some(TripleValue::String("after".to_string()))
    );
}
