//! Test that missing required fields are rejected.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_missing_entity_id() {
    let test = TestClient::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: None,
                    attribute_id: Some(vec![0u8; 16]),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                }],
            },
        )),
    };

    let resp = test.handle_message(req);
    assert_eq!(
        status_code(&resp),
        proto::google::rpc::Code::InvalidArgument as i32
    );
}

#[test]
fn test_missing_attribute_id() {
    let test = TestClient::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(vec![0u8; 16]),
                    attribute_id: None,
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                }],
            },
        )),
    };

    let resp = test.handle_message(req);
    assert_eq!(
        status_code(&resp),
        proto::google::rpc::Code::InvalidArgument as i32
    );
}

#[test]
fn test_missing_value() {
    let test = TestClient::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(vec![0u8; 16]),
                    attribute_id: Some(vec![0u8; 16]),
                    value: None,
                }],
            },
        )),
    };

    let resp = test.handle_message(req);
    assert_eq!(
        status_code(&resp),
        proto::google::rpc::Code::InvalidArgument as i32
    );
}

#[test]
fn test_no_payload() {
    let test = TestClient::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: None,
    };

    let resp = test.handle_message(req);
    assert_eq!(
        status_code(&resp),
        proto::google::rpc::Code::InvalidArgument as i32
    );
}
