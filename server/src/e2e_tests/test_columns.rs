//! Test that query responses include correct column names.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_query_returns_correct_columns() {
    let test = TestClient::new();

    let eid = entity_id(90);
    let aid = attribute_id(90);

    // Insert a value
    let resp = test.send(update_request(
        1,
        eid,
        aid,
        proto::triple_value::Value::String("test".to_string()),
    ));
    assert!(is_ok(&resp));

    // Point query returns column "v"
    let point_resp = test.send(point_query(2, eid, aid));
    assert!(is_ok(&point_resp));
    assert_eq!(point_resp.columns, vec!["v"]);

    // Entity scan returns columns "a" and "v"
    let scan_resp = test.send(entity_scan_query(3, eid));
    assert!(is_ok(&scan_resp));
    assert_eq!(scan_resp.columns, vec!["a", "v"]);
}
