//! Proto conversion for query types.
//!
//! This module implements `ProtoDeserializable` and `ProtoSerializable` for query types.

use crate::{
    proto,
    query::{Datom, EntityId, FieldId, Pattern, PatternElement, Query, QueryResult, Value, Variable},
    types::{ProtoDeserializable, ProtoSerializable},
};

/// Query response containing columns and rows for proto serialization.
pub struct QueryResponse {
    /// The column names.
    pub columns: Vec<String>,
    /// The result rows.
    pub rows: Vec<proto::QueryResultRow>,
}

impl ProtoDeserializable<&proto::QueryRequest> for Query {
    fn from_proto(request: &proto::QueryRequest) -> Result<Self, String> {
        let mut query = Self::new();

        // Convert find variables
        for var in &request.find {
            let name = var.label.as_deref().unwrap_or("");
            query = query.find(name);
        }

        // Convert where patterns
        for pattern in &request.r#where {
            query = query.where_pattern(proto_pattern_to_query(pattern)?);
        }

        // Convert optional patterns
        for pattern in &request.optional {
            query = query.optional(proto_pattern_to_query(pattern)?);
        }

        // Convert where_not patterns
        for pattern in &request.where_not {
            query = query.where_not(proto_pattern_to_query(pattern)?);
        }

        Ok(query)
    }
}

impl ProtoSerializable<QueryResponse> for QueryResult {
    fn to_proto(self) -> QueryResponse {
        let columns = self.columns.iter().map(ToOwned::to_owned).collect();

        let rows = self
            .rows
            .iter()
            .map(|row| proto::QueryResultRow {
                values: row
                    .iter()
                    .map(|d| datom_to_proto_result_value(d.as_ref()))
                    .collect(),
            })
            .collect();

        QueryResponse { columns, rows }
    }
}

/// Convert a proto `QueryPatternVariable` to an internal `Variable`.
fn proto_variable_to_query(var: &proto::QueryPatternVariable) -> Variable {
    Variable::new(var.label.as_deref().unwrap_or(""))
}

/// Convert bytes to a 16-byte array (zero-padded if needed).
fn bytes_to_id(bytes: &[u8]) -> [u8; 16] {
    let mut arr = [0u8; 16];
    let len = bytes.len().min(16);
    arr[..len].copy_from_slice(&bytes[..len]);
    arr
}

/// Convert a proto `QueryPattern` to an internal `Pattern`.
fn proto_pattern_to_query(pattern: &proto::QueryPattern) -> Result<Pattern, String> {
    // Convert entity
    let entity = match &pattern.entity {
        Some(proto::query_pattern::Entity::EntityId(bytes)) => {
            PatternElement::Entity(EntityId(bytes_to_id(bytes)))
        }
        Some(proto::query_pattern::Entity::EntityVariable(var)) => {
            PatternElement::Variable(proto_variable_to_query(var))
        }
        None => return Err("Pattern missing entity".to_owned()),
    };

    // Convert attribute/field
    let field = match &pattern.attribute {
        Some(proto::query_pattern::Attribute::AttributeId(bytes)) => {
            PatternElement::Field(FieldId(bytes_to_id(bytes)))
        }
        Some(proto::query_pattern::Attribute::AttributeVariable(var)) => {
            PatternElement::Variable(proto_variable_to_query(var))
        }
        None => return Err("Pattern missing attribute".to_owned()),
    };

    // Convert value
    let value = match &pattern.value_group {
        Some(proto::query_pattern::ValueGroup::Value(v)) => {
            PatternElement::Value(proto_triple_value_to_query(v))
        }
        Some(proto::query_pattern::ValueGroup::ValueVariable(var)) => {
            PatternElement::Variable(proto_variable_to_query(var))
        }
        None => return Err("Pattern missing value".to_owned()),
    };

    Ok(Pattern::new(entity, field, value))
}

/// Convert a proto `TripleValue` to an internal `Value`.
fn proto_triple_value_to_query(v: &proto::TripleValue) -> Value {
    match &v.value {
        Some(proto::triple_value::Value::String(s)) => Value::String(s.to_owned()),
        Some(proto::triple_value::Value::Number(n)) => Value::Number(*n),
        Some(proto::triple_value::Value::Boolean(b)) => Value::Boolean(*b),
        None => Value::Null,
    }
}

/// Convert a 16-byte ID to a string (UTF-8, trimming null bytes).
fn id_to_string(id: &[u8; 16]) -> String {
    std::str::from_utf8(id).map_or_else(
        |_| {
            // Fallback: hex encoding (manual)
            use std::fmt::Write;
            id.iter().fold(String::with_capacity(32), |mut acc, b| {
                let _ = write!(acc, "{b:02x}");
                acc
            })
        },
        |s| s.trim_end_matches('\0').to_owned(),
    )
}

/// Convert an internal `Datom` to a proto `QueryResultValue`.
fn datom_to_proto_result_value(datom: Option<&Datom>) -> proto::QueryResultValue {
    match datom {
        None => proto::QueryResultValue {
            value: None,
            is_undefined: true,
        },
        Some(Datom::Entity(id)) => proto::QueryResultValue {
            value: Some(proto::query_result_value::Value::Id(id_to_string(&id.0))),
            is_undefined: false,
        },
        Some(Datom::Field(id)) => proto::QueryResultValue {
            value: Some(proto::query_result_value::Value::Id(id_to_string(&id.0))),
            is_undefined: false,
        },
        Some(Datom::Value(v)) => proto::QueryResultValue {
            value: Some(proto::query_result_value::Value::TripleValue(
                value_to_proto_triple_value(v),
            )),
            is_undefined: false,
        },
    }
}

/// Convert an internal `Value` to a proto `TripleValue`.
fn value_to_proto_triple_value(v: &Value) -> proto::TripleValue {
    match v {
        Value::Null => proto::TripleValue { value: None },
        Value::Boolean(b) => proto::TripleValue {
            value: Some(proto::triple_value::Value::Boolean(*b)),
        },
        Value::Number(n) => proto::TripleValue {
            value: Some(proto::triple_value::Value::Number(*n)),
        },
        Value::String(s) => proto::TripleValue {
            value: Some(proto::triple_value::Value::String(s.to_owned())),
        },
        Value::Ref(id) => proto::TripleValue {
            // Store ref as a string ID
            value: Some(proto::triple_value::Value::String(id_to_string(&id.0))),
        },
    }
}
