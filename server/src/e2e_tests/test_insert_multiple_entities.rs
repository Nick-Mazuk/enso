//! Test inserting triples for multiple entities.

use crate::e2e_tests::helpers::*;
use crate::proto;

#[test]
fn test_insert_multiple_entities() {
    let test = TestClient::new();

    let eid1 = entity_id(20);
    let eid2 = entity_id(21);
    let aid = attribute_id(22);

    // Insert for entity 1
    let resp1 = test.send(update_request(
        1,
        eid1,
        aid,
        proto::triple_value::Value::String("entity one".to_string()),
    ));
    assert!(is_ok(&resp1));

    // Insert for entity 2
    let resp2 = test.send(update_request(
        2,
        eid2,
        aid,
        proto::triple_value::Value::String("entity two".to_string()),
    ));
    assert!(is_ok(&resp2));

    // Query entity 1
    let query1 = test.send(point_query(3, eid1, aid));
    assert!(is_ok(&query1));
    assert_eq!(query1.rows.len(), 1);
    assert_eq!(get_string_value(&query1, 0), Some("entity one"));

    // Query entity 2
    let query2 = test.send(point_query(4, eid2, aid));
    assert!(is_ok(&query2));
    assert_eq!(query2.rows.len(), 1);
    assert_eq!(get_string_value(&query2, 0), Some("entity two"));
}
