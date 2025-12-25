use crate::{
    constants::{
        MAX_TRIPLE_OBJECT_STRING_LENGTH, MAX_TRIPLE_PREDICATE_LENGTH, MAX_TRIPLE_SUBJECT_LENGTH,
    },
    proto,
    types::{ProtoDeserializable, ProtoSerializable},
};

#[derive(Debug, Clone)]
pub enum TripleValue {
    String(String),
    Boolean(bool),
    Number(f64),
}

#[derive(Debug, Clone)]
pub struct Triple {
    pub subject: String,
    pub predicate: String,
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
    };
    return Ok(validated_string);
}

impl ProtoDeserializable<proto::Triple> for Triple {
    fn from_proto(proto_triple: proto::Triple) -> Result<Triple, String> {
        let subject = validate_proto_string(
            proto_triple.subject,
            MAX_TRIPLE_SUBJECT_LENGTH,
            "Triple",
            "subject",
        )?;
        let predicate = validate_proto_string(
            proto_triple.predicate,
            MAX_TRIPLE_PREDICATE_LENGTH,
            "Triple",
            "predicate",
        )?;
        let value = match proto_triple.object {
            Some(proto::triple::Object::String(string)) => {
                TripleValue::String(validate_proto_string(
                    string,
                    MAX_TRIPLE_OBJECT_STRING_LENGTH,
                    "Triple object",
                    "string",
                )?)
            }
            Some(proto::triple::Object::Boolean(boolean)) => TripleValue::Boolean(boolean),
            Some(proto::triple::Object::Number(number)) => TripleValue::Number(number),
            None => return Err("Triple proto did not contain an object.".into()),
        };
        return Ok(Triple {
            subject,
            predicate,
            value,
        });
    }
}

impl ProtoSerializable<proto::Triple> for Triple {
    fn to_proto(self) -> proto::Triple {
        proto::Triple {
            subject: Some(self.subject),
            predicate: Some(self.predicate),
            object: match self.value {
                TripleValue::String(string) => Some(proto::triple::Object::String(string)),
                TripleValue::Boolean(boolean) => Some(proto::triple::Object::Boolean(boolean)),
                TripleValue::Number(number) => Some(proto::triple::Object::Number(number)),
            },
        }
    }
}
