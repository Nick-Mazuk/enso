use crate::{
    constants::MAX_TRIPLE_STRING_VALUE_LENGTH,
    proto,
    types::{ProtoDeserializable, ProtoSerializable},
};

const ID_LENGTH: usize = 16;
type ID = [u8; ID_LENGTH];

#[derive(Debug)]
pub enum TripleValue {
    String(String),
    Boolean(bool),
    Number(f64),
}

#[derive(Debug)]
/// A triple, readonly.
///
/// INVARIANT: the Triple struct must always represent a well-formed triple.
pub struct Triple {
    pub entity_id: ID,
    pub attribute_id: ID,
    pub value: TripleValue,
}

fn validate_proto_string<S: Into<Option<prost::alloc::string::String>>>(
    string: S,
    max_length: usize,
    proto_name: &'static str,
    field_name: &'static str,
) -> Result<String, String> {
    let Some(validated_string) = string.into() else {
        return Err(format!("{proto_name} proto did not contain a {field_name}"));
    };
    if validated_string.len() > max_length {
        return Err(format!(
            "{proto_name} proto {field_name} was too long. Expect a max length of {max_length} characters."
        ));
    }
    if validated_string.is_empty() {
        return Err(format!(
            "{proto_name} proto {field_name} was empty. Expected to have some content."
        ));
    }
    Ok(validated_string)
}

fn validate_proto_id<B: Into<Option<Vec<u8>>>>(
    maybe_bytes: B,
    proto_name: &'static str,
    field_name: &'static str,
) -> Result<ID, String> {
    let Some(bytes) = maybe_bytes.into() else {
        return Err(format!("{proto_name} proto did not contain a {field_name}"));
    };
    let bytes_length = bytes.len();
    if bytes.len() != ID_LENGTH {
        return Err(format!(
            "{proto_name} field {field_name} did not contain the correct number of bytes. Expected {ID_LENGTH}, got {bytes_length}"
        ));
    }
    let Ok(id) = bytes.try_into() else {
        return Err(format!(
            "{proto_name} field {field_name} did not contain the correct number of bytes. Expected {ID_LENGTH}, got {bytes_length}"
        ));
    };
    Ok(id)
}

impl ProtoDeserializable<proto::Triple> for Triple {
    fn from_proto(proto_triple: proto::Triple) -> Result<Self, String> {
        let entity_id = validate_proto_id(proto_triple.entity_id, "Triple", "subject")?;
        let attribute_id = validate_proto_id(proto_triple.attribute_id, "Triple", "predicate")?;
        let value = match proto_triple.value {
            Some(proto::triple::Value::String(string)) => {
                TripleValue::String(validate_proto_string(
                    string,
                    MAX_TRIPLE_STRING_VALUE_LENGTH,
                    "Triple object",
                    "string",
                )?)
            }
            Some(proto::triple::Value::Boolean(boolean)) => TripleValue::Boolean(boolean),
            Some(proto::triple::Value::Number(number)) => TripleValue::Number(number),
            None => return Err("Triple proto did not contain an object.".into()),
        };
        Ok(Self {
            entity_id,
            attribute_id,
            value,
        })
    }
}

impl ProtoSerializable<proto::Triple> for Triple {
    fn to_proto(self) -> proto::Triple {
        proto::Triple {
            entity_id: Some(self.entity_id.to_vec()),
            attribute_id: Some(self.attribute_id.to_vec()),
            value: match self.value {
                TripleValue::String(string) => Some(proto::triple::Value::String(string)),
                TripleValue::Boolean(boolean) => Some(proto::triple::Value::Boolean(boolean)),
                TripleValue::Number(number) => Some(proto::triple::Value::Number(number)),
            },
        }
    }
}
