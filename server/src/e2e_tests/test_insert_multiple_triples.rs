//! Test inserting multiple triples in a single request.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_insert_multiple_triples_single_request() {
    let test = TestClient::new();

    let eid = entity_id(10);
    let aid1 = attribute_id(11);
    let aid2 = attribute_id(12);
    let aid3 = attribute_id(13);

    // Insert multiple triples in one request
    let insert_resp = test.send(update_request_multi(
        1,
        vec![
            (
                eid,
                aid1,
                proto::triple_value::Value::String("name".to_string()),
            ),
            (eid, aid2, proto::triple_value::Value::Number(25.0)),
            (eid, aid3, proto::triple_value::Value::Boolean(false)),
        ],
    ));

    assert!(is_ok(&insert_resp));

    // Query all attributes for the entity
    let query_resp = test.send(entity_scan_query(2, eid));

    assert!(is_ok(&query_resp));
    assert_eq!(query_resp.rows.len(), 3);
}
