//! Test that request IDs are correctly echoed in responses.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_request_id_preserved() {
    let test = TestClient::new();

    for request_id in [1, 100, 999, u32::MAX] {
        let req = proto::ClientMessage {
            request_id: Some(request_id),
            payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                proto::TripleUpdateRequest { triples: vec![] },
            )),
        };

        let resp = test.send(req);
        assert_eq!(resp.request_id, Some(request_id));
    }
}

#[test]
fn test_request_id_none() {
    let test = TestClient::new();

    let req = proto::ClientMessage {
        request_id: None,
        payload: Some(proto::client_message::Payload::TripleUpdateRequest(
            proto::TripleUpdateRequest { triples: vec![] },
        )),
    };

    let resp = test.send(req);
    assert_eq!(resp.request_id, None);
}
