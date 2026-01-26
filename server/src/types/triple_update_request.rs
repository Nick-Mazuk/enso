use crate::proto;
use crate::types::{PendingTripleData, ProtoDeserializable};

#[derive(Debug)]
pub struct TripleUpdateRequest {
    pub triples: Vec<PendingTripleData>,
}

impl ProtoDeserializable<proto::TripleUpdateRequest> for TripleUpdateRequest {
    fn from_proto(request: proto::TripleUpdateRequest) -> Result<Self, String> {
        let mut triples = Vec::with_capacity(request.triples.len());

        for (index, triple) in request.triples.into_iter().enumerate() {
            match PendingTripleData::from_proto(triple) {
                Ok(data) => triples.push(data),
                Err(err) => return Err(format!("Failed to parse triple #{index}: {err}")),
            }
        }

        Ok(Self { triples })
    }
}
