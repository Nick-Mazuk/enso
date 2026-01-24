//! Test a sequence of operations: insert, query, update, query.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_sequence_insert_query_update_query() {
    let test = TestClient::new();

    let eid = entity_id(50);
    let aid = attribute_id(50);

    // Step 1: Insert
    let resp1 = test.send(update_request(
        1,
        eid,
        aid,
        proto::triple_value::Value::Number(1.0),
    ));
    assert!(is_ok(&resp1));

    // Step 2: Query (should see 1.0)
    let resp2 = test.send(point_query(2, eid, aid));
    assert!(is_ok(&resp2));
    assert_eq!(resp2.rows.len(), 1);
    assert_eq!(get_number_value(&resp2, 0), Some(1.0));

    // Step 3: Update
    let resp3 = test.send(update_request(
        3,
        eid,
        aid,
        proto::triple_value::Value::Number(2.0),
    ));
    assert!(is_ok(&resp3));

    // Step 4: Query (should see 2.0)
    let resp4 = test.send(point_query(4, eid, aid));
    assert!(is_ok(&resp4));
    assert_eq!(resp4.rows.len(), 1);
    assert_eq!(get_number_value(&resp4, 0), Some(2.0));
}
