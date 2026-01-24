//! Test querying a nonexistent entity returns no results.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_query_nonexistent_entity() {
    let test = TestClient::new();

    let eid = entity_id(40);
    let other_eid = entity_id(99);
    let aid = attribute_id(40);

    // Insert some data
    let resp = test.send(update_request(
        1,
        eid,
        aid,
        proto::triple_value::Value::String("exists".to_string()),
    ));
    assert!(is_ok(&resp));

    // Query for a different entity
    let query_resp = test.send(point_query(2, other_eid, aid));

    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 0);
}
