//! Test that an empty triples request is accepted.

use crate::e2e_tests::helpers::{TestClient, is_ok};
use crate::proto;

#[test]
fn test_empty_triples_request() {
    let mut client = TestClient::new();

    let request = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest { triples: vec![] },
        )),
    };

    let response = client.handle_message(request);
    assert!(is_ok(&response));
    assert_eq!(response.request_id, Some(1));
}
