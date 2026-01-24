//! Test that the same sequence of operations produces identical results.

use crate::e2e_tests::helpers::*;
use crate::proto;

fn run_sequence() -> Vec<proto::ServerResponse> {
    let test = TestClient::new();
    let mut responses = Vec::new();

    let eid = entity_id(80);
    let aid1 = attribute_id(81);
    let aid2 = attribute_id(82);

    // Insert
    responses.push(test.send(update_request(
        1,
        eid,
        aid1,
        proto::triple_value::Value::String("first".to_string()),
    )));

    responses.push(test.send(update_request(
        2,
        eid,
        aid2,
        proto::triple_value::Value::Number(42.0),
    )));

    // Query
    responses.push(test.send(entity_scan_query(3, eid)));

    // Update
    responses.push(test.send(update_request(
        4,
        eid,
        aid1,
        proto::triple_value::Value::String("updated".to_string()),
    )));

    // Query again
    responses.push(test.send(entity_scan_query(5, eid)));

    responses
}

#[test]
fn test_deterministic_sequence() {
    let run1 = run_sequence();
    let run2 = run_sequence();

    // Compare all responses
    assert_eq!(run1.len(), run2.len());

    for (i, (r1, r2)) in run1.iter().zip(run2.iter()).enumerate() {
        assert_eq!(r1.request_id, r2.request_id, "request_id mismatch at {i}");
        assert_eq!(
            status_code(r1),
            status_code(r2),
            "status code mismatch at {i}"
        );
        assert_eq!(r1.rows.len(), r2.rows.len(), "row count mismatch at {i}");

        // Compare row contents
        for (j, (row1, row2)) in r1.rows.iter().zip(r2.rows.iter()).enumerate() {
            assert_eq!(
                row1.values.len(),
                row2.values.len(),
                "value count mismatch at {i}:{j}"
            );
            for (k, (v1, v2)) in row1.values.iter().zip(row2.values.iter()).enumerate() {
                assert_eq!(v1.value, v2.value, "value mismatch at {i}:{j}:{k}");
            }
        }
    }
}
