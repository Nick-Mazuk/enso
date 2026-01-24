//! Test querying an empty database returns no results.

use crate::e2e_tests::helpers::*;

#[test]
fn test_query_empty_database() {
    let test = TestClient::new();

    let eid = entity_id(99);
    let aid = attribute_id(99);

    let resp = test.send(point_query(1, eid, aid));

    assert!(is_ok(&resp));
    assert_eq!(resp.rows.len(), 0);
}
