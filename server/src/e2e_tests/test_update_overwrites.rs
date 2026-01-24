//! Test that updating a triple overwrites the previous value.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_update_overwrites_value() {
    let test = TestClient::new();

    let eid = entity_id(30);
    let aid = attribute_id(30);

    // Insert initial value
    let resp1 = test.send(update_request(
        1,
        eid,
        aid,
        proto::triple_value::Value::String("original".to_string()),
    ));
    assert!(is_ok(&resp1));

    // Update with new value
    let resp2 = test.send(update_request(
        2,
        eid,
        aid,
        proto::triple_value::Value::String("updated".to_string()),
    ));
    assert!(is_ok(&resp2));

    // Query should return updated value
    let query_resp = test.send(point_query(3, eid, aid));
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_string_value(&query_resp, 0), Some("updated"));
}
