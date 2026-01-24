//! Test that updating a triple can change the value type.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_update_changes_value_type() {
    let test = TestClient::new();

    let eid = entity_id(32);
    let aid = attribute_id(32);

    // Insert as string
    let resp1 = test.send(update_request(
        1,
        eid,
        aid,
        proto::triple_value::Value::String("text".to_string()),
    ));
    assert!(is_ok(&resp1));

    // Update to number
    let resp2 = test.send(update_request(
        2,
        eid,
        aid,
        proto::triple_value::Value::Number(123.0),
    ));
    assert!(is_ok(&resp2));

    // Query should return number
    let query_resp = test.send(point_query(3, eid, aid));
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_number_value(&query_resp, 0), Some(123.0));
}
