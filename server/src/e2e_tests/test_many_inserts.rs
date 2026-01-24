//! Test inserting many triples sequentially.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_many_sequential_inserts() {
    let test = TestClient::new();

    let eid = entity_id(60);

    // Insert 100 different attributes
    for i in 0..100u8 {
        let aid = attribute_id(i);

        let resp = test.send(update_request(
            u32::from(i) + 1,
            eid,
            aid,
            proto::triple_value::Value::Number(f64::from(i)),
        ));
        assert!(is_ok(&resp));
    }

    // Query all attributes
    let query_resp = test.send(entity_scan_query(101, eid));
    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 100);
}
