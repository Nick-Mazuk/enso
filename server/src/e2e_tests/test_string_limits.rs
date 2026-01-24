//! Test string length limits.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_max_length_string_value() {
    let test = TestClient::new();

    let eid = entity_id(70);
    let aid = attribute_id(70);

    // Create a string at max length (1024 chars)
    let max_string: String = "x".repeat(1024);

    let resp = test.send(update_request(
        1,
        eid,
        aid,
        proto::triple_value::Value::String(max_string.clone()),
    ));
    assert!(is_ok(&resp));

    // Query and verify
    let query_resp = test.send(point_query(2, eid, aid));
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_string_value(&query_resp, 0), Some(max_string.as_str()));
}

#[test]
fn test_string_too_long_rejected() {
    let test = TestClient::new();

    let eid = entity_id(71);
    let aid = attribute_id(71);

    // Create a string exceeding max length (1025 chars)
    let too_long_string: String = "y".repeat(1025);

    let resp = test.send(update_request(
        1,
        eid,
        aid,
        proto::triple_value::Value::String(too_long_string),
    ));

    assert_eq!(
        status_code(&resp),
        proto::google::rpc::Code::InvalidArgument as i32
    );
}
