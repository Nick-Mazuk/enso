//! Test that an empty triples request is accepted.

use crate::e2e_tests::helpers::{TestClient, is_ok};
use crate::proto;

#[test]
fn test_empty_triples_request() {
    let mut test = TestClient::new();

    let req = proto::ClientMessage {
        request_id: Some(1),
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest { triples: vec![] },
        )),
    };

    let resp = test.handle_message(req);
    assert!(is_ok(&resp));
    assert_eq!(resp.request_id, Some(1));
}
