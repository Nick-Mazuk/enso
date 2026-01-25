//! Test that invalid attribute IDs are rejected.

use crate::e2e_tests::helpers::{TestClient, new_hlc, status_code};
use crate::proto;

#[test]
fn test_invalid_attribute_id_length() {
    let mut client = TestClient::new();

    // Attribute ID with wrong length
    let request = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest {
                triples: vec![proto::Triple {
                    entity_id: Some(vec![0u8; 16]),
                    attribute_id: Some(vec![1, 2, 3, 4, 5]), // Only 5 bytes
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String("test".to_string())),
                    }),
                    hlc: Some(new_hlc(1)),
                }],
            },
        )),
    };

    let response = client.handle_message(request);
    assert_eq!(
        status_code(&response),
        proto::google::rpc::Code::InvalidArgument as i32
    );
}
