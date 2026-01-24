//! Test inserting and querying a boolean value.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_insert_boolean_then_query() {
    let test = TestClient::new();

    let eid = entity_id(3);
    let aid = attribute_id(3);

    // Insert a boolean value
    let insert_resp = test.send(update_request(
        1,
        eid,
        aid,
        proto::triple_value::Value::Boolean(true),
    ));

    assert!(is_ok(&insert_resp));

    // Query it back
    let query_resp = test.send(point_query(2, eid, aid));

    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 1);
    assert_eq!(get_bool_value(&query_resp, 0), Some(true));
}
