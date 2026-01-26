//! Subscription management for real-time triple updates.
//!
//! This module provides infrastructure for clients to subscribe to triple changes
//! and receive streaming updates as they occur.
//!
//! # Subscription Lifecycle
//!
//! 1. Client sends `SubscribeRequest` with a `subscription_id` and optional `since_hlc`
//! 2. Server validates the subscription ID is unique for this connection
//! 3. If `since_hlc` provided, server sends historical changes as initial `SubscriptionUpdate`
//! 4. Server sends ongoing `SubscriptionUpdate` messages as changes occur
//! 5. Client sends `UnsubscribeRequest` to cancel, or subscription ends on disconnect

use std::collections::HashMap;

use crate::proto;
use crate::storage::{LogRecord, LogRecordPayload};
use crate::types::{HlcTimestamp, ProtoSerializable, TripleRecord};

/// Per-connection subscription tracking.
///
/// Each WebSocket connection maintains its own set of active subscriptions.
/// Subscriptions are identified by client-provided IDs that must be unique
/// within the connection.
pub struct ClientSubscriptions {
    /// Map of `subscription_id` -> Subscription metadata.
    subscriptions: HashMap<u32, Subscription>,
}

/// Metadata for a single subscription.
pub struct Subscription {
    /// Client-provided subscription ID.
    pub id: u32,
    /// Optional HLC timestamp for filtering changes.
    /// Only changes with HLC > `since_hlc` are sent.
    pub since_hlc: Option<HlcTimestamp>,
}

impl ClientSubscriptions {
    /// Create a new empty subscription tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    /// Add a new subscription.
    ///
    /// # Errors
    ///
    /// Returns `SubscriptionError::AlreadyExists` if a subscription with the
    /// given ID already exists.
    pub fn add(
        &mut self,
        id: u32,
        since_hlc: Option<HlcTimestamp>,
    ) -> Result<(), SubscriptionError> {
        if self.subscriptions.contains_key(&id) {
            return Err(SubscriptionError::AlreadyExists(id));
        }
        self.subscriptions
            .insert(id, Subscription { id, since_hlc });
        Ok(())
    }

    /// Remove a subscription by ID.
    ///
    /// # Errors
    ///
    /// Returns `SubscriptionError::NotFound` if no subscription with the
    /// given ID exists.
    pub fn remove(&mut self, id: u32) -> Result<(), SubscriptionError> {
        if self.subscriptions.remove(&id).is_none() {
            return Err(SubscriptionError::NotFound(id));
        }
        Ok(())
    }

    /// Get a subscription by ID.
    #[must_use]
    pub fn get(&self, id: u32) -> Option<&Subscription> {
        self.subscriptions.get(&id)
    }

    /// Iterate over all active subscriptions.
    pub fn iter(&self) -> impl Iterator<Item = &Subscription> {
        self.subscriptions.values()
    }

    /// Check if there are any active subscriptions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    /// Get the number of active subscriptions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }
}

impl Default for ClientSubscriptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during subscription operations.
#[derive(Debug, PartialEq, Eq)]
pub enum SubscriptionError {
    /// A subscription with this ID already exists.
    AlreadyExists(u32),
    /// No subscription with this ID exists.
    NotFound(u32),
}

impl std::fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists(id) => write!(f, "subscription {id} already exists"),
            Self::NotFound(id) => write!(f, "subscription {id} not found"),
        }
    }
}

impl std::error::Error for SubscriptionError {}

/// Create a success response message.
#[must_use]
pub fn create_ok_response(request_id: Option<u32>) -> proto::ServerMessage {
    proto::ServerMessage {
        payload: Some(proto::server_message::Payload::Response(
            proto::ServerResponse {
                request_id,
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Ok.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    }
}

/// Create an error response message with `InvalidArgument` status.
#[must_use]
pub fn create_error_response(request_id: Option<u32>, message: &str) -> proto::ServerMessage {
    proto::ServerMessage {
        payload: Some(proto::server_message::Payload::Response(
            proto::ServerResponse {
                request_id,
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::InvalidArgument.into(),
                    message: message.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    }
}

/// Create a `FailedPrecondition` error response message.
///
/// Use this when a request cannot be fulfilled because the system is not
/// in the required state (e.g., connection not established).
#[must_use]
pub fn create_failed_precondition_response(
    request_id: Option<u32>,
    message: &str,
) -> proto::ServerMessage {
    proto::ServerMessage {
        payload: Some(proto::server_message::Payload::Response(
            proto::ServerResponse {
                request_id,
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::FailedPrecondition.into(),
                    message: message.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    }
}

/// Create an `Internal` error response message.
///
/// Use this for internal server errors that the client cannot resolve.
#[must_use]
pub fn create_internal_error_response(
    request_id: Option<u32>,
    message: &str,
) -> proto::ServerMessage {
    proto::ServerMessage {
        payload: Some(proto::server_message::Payload::Response(
            proto::ServerResponse {
                request_id,
                status: Some(proto::google::rpc::Status {
                    code: proto::google::rpc::Code::Internal.into(),
                    message: message.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    }
}

/// Convert a slice of log records to proto change records.
///
/// Filters out non-change records (BEGIN, COMMIT, CHECKPOINT) and logs warnings
/// for any conversion errors.
pub fn convert_log_records_to_changes(log_records: &[LogRecord]) -> Vec<proto::ChangeRecord> {
    let mut changes = Vec::new();
    for record in log_records {
        match log_record_to_change_record(record) {
            Ok(Some(change)) => changes.push(change),
            Ok(None) => {} // Skip non-change records
            Err(e) => {
                tracing::warn!("failed to convert log record: {e}");
            }
        }
    }
    changes
}

/// Convert a WAL `LogRecord` to a proto `ChangeRecord`.
///
/// Returns `None` for non-change records (BEGIN, COMMIT, CHECKPOINT).
///
/// # Errors
///
/// Returns an error if the triple record cannot be deserialized.
pub fn log_record_to_change_record(
    record: &LogRecord,
) -> Result<Option<proto::ChangeRecord>, String> {
    match &record.payload {
        LogRecordPayload::Insert(bytes) => {
            let triple = TripleRecord::from_bytes(bytes)
                .map_err(|e| format!("failed to deserialize insert triple: {e:?}"))?;
            Ok(Some(proto::ChangeRecord {
                change_type: proto::ChangeType::Insert.into(),
                triple: Some(proto::Triple {
                    entity_id: Some(triple.entity_id.to_vec()),
                    attribute_id: Some(triple.attribute_id.to_vec()),
                    value: (&triple.value).to_proto(),
                    hlc: Some(record.hlc.to_proto()),
                }),
            }))
        }
        LogRecordPayload::Update(bytes) => {
            let triple = TripleRecord::from_bytes(bytes)
                .map_err(|e| format!("failed to deserialize update triple: {e:?}"))?;
            Ok(Some(proto::ChangeRecord {
                change_type: proto::ChangeType::Update.into(),
                triple: Some(proto::Triple {
                    entity_id: Some(triple.entity_id.to_vec()),
                    attribute_id: Some(triple.attribute_id.to_vec()),
                    value: (&triple.value).to_proto(),
                    hlc: Some(record.hlc.to_proto()),
                }),
            }))
        }
        LogRecordPayload::Delete {
            entity_id,
            attribute_id,
        } => Ok(Some(proto::ChangeRecord {
            change_type: proto::ChangeType::Delete.into(),
            triple: Some(proto::Triple {
                entity_id: Some(entity_id.to_vec()),
                attribute_id: Some(attribute_id.to_vec()),
                value: None,
                hlc: Some(record.hlc.to_proto()),
            }),
        })),
        LogRecordPayload::Begin
        | LogRecordPayload::Commit
        | LogRecordPayload::Checkpoint { .. } => Ok(None),
    }
}

/// Create a proto `SubscriptionUpdate` from proto change records.
#[must_use]
#[allow(clippy::disallowed_methods)] // Clone needed for proto types
pub fn create_subscription_update(
    subscription_id: u32,
    changes: &[proto::ChangeRecord],
) -> proto::SubscriptionUpdate {
    proto::SubscriptionUpdate {
        subscription_id,
        changes: changes.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientConnection;

    #[test]
    fn test_add_subscription() {
        let mut subs = ClientSubscriptions::new();
        assert!(subs.add(1, None).is_ok());
        assert!(subs.get(1).is_some());
        assert_eq!(subs.len(), 1);
    }

    #[test]
    fn test_add_duplicate_subscription() {
        let mut subs = ClientSubscriptions::new();
        assert!(subs.add(1, None).is_ok());
        assert_eq!(subs.add(1, None), Err(SubscriptionError::AlreadyExists(1)));
    }

    #[test]
    fn test_remove_subscription() {
        let mut subs = ClientSubscriptions::new();
        subs.add(1, None).expect("add should succeed");
        assert!(subs.remove(1).is_ok());
        assert!(subs.get(1).is_none());
        assert!(subs.is_empty());
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut subs = ClientSubscriptions::new();
        assert_eq!(subs.remove(1), Err(SubscriptionError::NotFound(1)));
    }

    #[test]
    fn test_add_with_since_hlc() {
        let mut subs = ClientSubscriptions::new();
        let hlc = HlcTimestamp {
            physical_time: 1000,
            logical_counter: 1,
            node_id: 1,
        };
        subs.add(1, Some(hlc)).expect("add should succeed");
        let sub = subs.get(1).expect("subscription should exist");
        assert_eq!(sub.since_hlc, Some(hlc));
    }

    #[test]
    fn test_client_subscribe_returns_ok() {
        let db = crate::testing::new_test_database().expect("create test db");
        let mut client = ClientConnection::new(db);

        let msg = proto::ClientMessage {
            request_id: Some(42),
            payload: Some(proto::client_message::Payload::Subscribe(
                proto::SubscribeRequest {
                    subscription_id: 1,
                    since_hlc: None,
                },
            )),
        };

        let messages = client.handle_message(msg);
        assert_eq!(messages.len(), 1);

        // Verify it's an OK response
        match &messages[0].payload {
            Some(proto::server_message::Payload::Response(resp)) => {
                assert_eq!(resp.request_id, Some(42));
                assert_eq!(
                    resp.status.as_ref().unwrap().code,
                    proto::google::rpc::Code::Ok as i32
                );
            }
            _ => panic!("expected Response payload"),
        }

        // Verify subscription was added
        assert_eq!(client.subscriptions().count(), 1);
    }

    #[test]
    fn test_client_subscribe_duplicate_returns_error() {
        let db = crate::testing::new_test_database().expect("create test db");
        let mut client = ClientConnection::new(db);

        // First subscribe succeeds
        let msg1 = proto::ClientMessage {
            request_id: Some(1),
            payload: Some(proto::client_message::Payload::Subscribe(
                proto::SubscribeRequest {
                    subscription_id: 1,
                    since_hlc: None,
                },
            )),
        };
        let _ = client.handle_message(msg1);

        // Second subscribe with same ID fails
        let msg2 = proto::ClientMessage {
            request_id: Some(42),
            payload: Some(proto::client_message::Payload::Subscribe(
                proto::SubscribeRequest {
                    subscription_id: 1,
                    since_hlc: None,
                },
            )),
        };
        let messages = client.handle_message(msg2);
        assert_eq!(messages.len(), 1);

        // Verify it's an error response
        match &messages[0].payload {
            Some(proto::server_message::Payload::Response(resp)) => {
                assert_eq!(resp.request_id, Some(42));
                assert_eq!(
                    resp.status.as_ref().unwrap().code,
                    proto::google::rpc::Code::InvalidArgument as i32
                );
                assert!(
                    resp.status
                        .as_ref()
                        .unwrap()
                        .message
                        .contains("already exists")
                );
            }
            _ => panic!("expected Response payload"),
        }
    }

    #[test]
    fn test_client_unsubscribe_returns_ok() {
        let db = crate::testing::new_test_database().expect("create test db");
        let mut client = ClientConnection::new(db);

        // First subscribe
        let sub_msg = proto::ClientMessage {
            request_id: Some(1),
            payload: Some(proto::client_message::Payload::Subscribe(
                proto::SubscribeRequest {
                    subscription_id: 1,
                    since_hlc: None,
                },
            )),
        };
        let _ = client.handle_message(sub_msg);

        // Then unsubscribe
        let unsub_msg = proto::ClientMessage {
            request_id: Some(42),
            payload: Some(proto::client_message::Payload::Unsubscribe(
                proto::UnsubscribeRequest { subscription_id: 1 },
            )),
        };
        let messages = client.handle_message(unsub_msg);
        assert_eq!(messages.len(), 1);

        // Verify it's an OK response
        match &messages[0].payload {
            Some(proto::server_message::Payload::Response(resp)) => {
                assert_eq!(resp.request_id, Some(42));
                assert_eq!(
                    resp.status.as_ref().unwrap().code,
                    proto::google::rpc::Code::Ok as i32
                );
            }
            _ => panic!("expected Response payload"),
        }

        // Verify subscription was removed
        assert_eq!(client.subscriptions().count(), 0);
    }

    #[test]
    fn test_client_unsubscribe_nonexistent_returns_error() {
        let db = crate::testing::new_test_database().expect("create test db");
        let mut client = ClientConnection::new(db);

        let msg = proto::ClientMessage {
            request_id: Some(42),
            payload: Some(proto::client_message::Payload::Unsubscribe(
                proto::UnsubscribeRequest { subscription_id: 1 },
            )),
        };
        let messages = client.handle_message(msg);
        assert_eq!(messages.len(), 1);

        // Verify it's an error response
        match &messages[0].payload {
            Some(proto::server_message::Payload::Response(resp)) => {
                assert_eq!(resp.request_id, Some(42));
                assert_eq!(
                    resp.status.as_ref().unwrap().code,
                    proto::google::rpc::Code::InvalidArgument as i32
                );
                assert!(resp.status.as_ref().unwrap().message.contains("not found"));
            }
            _ => panic!("expected Response payload"),
        }
    }
}
