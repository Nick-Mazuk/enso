//! Storage `TripleValue` proto conversion.
//!
//! Provides conversion between storage `TripleValue` and proto `TripleValue`.
//!
//! Note: This is separate from `types/triple.rs` which handles a different
//! `TripleValue` type used for client messages.

use crate::proto;
use crate::storage::TripleValue;
use crate::types::ProtoSerializable;

impl ProtoSerializable<Option<proto::TripleValue>> for TripleValue {
    fn to_proto(self) -> Option<proto::TripleValue> {
        match self {
            Self::Null => None,
            Self::Boolean(b) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Boolean(b)),
            }),
            Self::Number(n) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Number(n)),
            }),
            Self::String(s) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String(s)),
            }),
        }
    }
}

impl ProtoSerializable<Option<proto::TripleValue>> for &TripleValue {
    #[allow(clippy::disallowed_methods)] // Clone needed for String conversion
    fn to_proto(self) -> Option<proto::TripleValue> {
        match self {
            TripleValue::Null => None,
            TripleValue::Boolean(b) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Boolean(*b)),
            }),
            TripleValue::Number(n) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::Number(*n)),
            }),
            TripleValue::String(s) => Some(proto::TripleValue {
                value: Some(proto::triple_value::Value::String(s.as_str().to_owned())),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_to_proto() {
        let value = TripleValue::Null;
        let proto_value: Option<proto::TripleValue> = value.to_proto();
        assert!(proto_value.is_none());
    }

    #[test]
    fn test_boolean_to_proto() {
        let value = TripleValue::Boolean(true);
        let proto_value: Option<proto::TripleValue> = value.to_proto();
        assert!(proto_value.is_some());
        match proto_value.expect("should be some").value {
            Some(proto::triple_value::Value::Boolean(b)) => assert!(b),
            _ => panic!("expected Boolean"),
        }
    }

    #[test]
    fn test_number_to_proto() {
        let value = TripleValue::Number(42.5);
        let proto_value: Option<proto::TripleValue> = value.to_proto();
        assert!(proto_value.is_some());
        match proto_value.expect("should be some").value {
            Some(proto::triple_value::Value::Number(n)) => {
                assert!((n - 42.5).abs() < f64::EPSILON);
            }
            _ => panic!("expected Number"),
        }
    }

    #[test]
    fn test_string_to_proto() {
        let value = TripleValue::String("hello".to_string());
        let proto_value: Option<proto::TripleValue> = value.to_proto();
        assert!(proto_value.is_some());
        match proto_value.expect("should be some").value {
            Some(proto::triple_value::Value::String(s)) => assert_eq!(s, "hello"),
            _ => panic!("expected String"),
        }
    }

    #[test]
    fn test_ref_to_proto() {
        let value = TripleValue::String("world".to_string());
        let proto_value: Option<proto::TripleValue> = (&value).to_proto();
        assert!(proto_value.is_some());
        match proto_value.expect("should be some").value {
            Some(proto::triple_value::Value::String(s)) => assert_eq!(s, "world"),
            _ => panic!("expected String"),
        }
        // Original still accessible
        assert!(matches!(value, TripleValue::String(_)));
    }
}
