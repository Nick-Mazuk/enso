use crate::{
    proto,
    types::{ProtoDeserializable, triple::Triple},
};

#[derive(Debug, Clone)]
pub struct TripleUpdateRequest {
    triples: Vec<Triple>,
}

impl TripleUpdateRequest {
    pub fn triples(&self) -> &[Triple] {
        &self.triples
    }
}

impl ProtoDeserializable<proto::TripleUpdateRequest> for TripleUpdateRequest {
    fn from_proto(request: proto::TripleUpdateRequest) -> Result<Self, String> {
        let mut triples = Vec::with_capacity(request.triples.len());

        for (index, triple) in request.triples.into_iter().enumerate() {
            let result = Triple::from_proto(triple);
            match result {
                Ok(triple) => triples.push(triple),
                Err(err) => return Err(format!("Failed to parse triple #{index}: {err}")),
            };
        }

        Ok(Self { triples })
    }
}
