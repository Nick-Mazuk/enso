//! Test inserting and querying a string value.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_insert_string_then_query() {
    let test = TestClient::new();

    let eid = entity_id(1);
    let aid = attribute_id(1);

    // Insert a string value
    let insert_resp = test.send(update_request(
        1,
        eid,
        aid,
        proto::triple_value::Value::String("hello world".to_string()),
    ));

    assert!(is_ok(&insert_resp));
    assert_eq!(insert_resp.request_id, Some(1));

    // Query it back
    let query_resp = test.send(point_query(2, eid, aid));

    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.request_id, Some(2));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_string_value(&query_resp, 0), Some("hello world"));
}
