//! Message generator for deterministic simulation testing.
//!
//! This module generates random but reproducible `ClientMessage` sequences
//! for testing, including both well-formed and malformed messages.

// Simulation code legitimately needs cloning for test data
#![allow(clippy::disallowed_methods)]

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::proto;

/// Configuration for message generation.
#[derive(Debug, Clone)]
pub struct MessageGenConfig {
    /// Probability of generating a malformed message (0.0 - 1.0).
    pub malformed_rate: f64,
    /// Probability of generating a query vs update (0.0 = always update, 1.0 = always query).
    pub query_rate: f64,
    /// Maximum number of triples per update.
    pub max_triples_per_update: usize,
    /// Maximum string value length.
    pub max_string_length: usize,
    /// Size of the entity/attribute ID pool for reuse.
    pub id_pool_size: usize,
}

impl Default for MessageGenConfig {
    fn default() -> Self {
        Self {
            malformed_rate: 0.0,
            query_rate: 0.3,
            max_triples_per_update: 10,
            max_string_length: 100,
            id_pool_size: 50,
        }
    }
}

/// Types of malformations that can be generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MalformationType {
    /// Entity ID with wrong length (not 16 bytes).
    WrongLengthEntityId,
    /// Attribute ID with wrong length (not 16 bytes).
    WrongLengthAttributeId,
    /// Missing entity ID.
    MissingEntityId,
    /// Missing attribute ID.
    MissingAttributeId,
    /// Missing value.
    MissingValue,
    /// Empty triples list in update.
    EmptyTriples,
    /// Very long string value.
    OverflowStringValue,
    /// NaN number value.
    NanNumberValue,
    /// Infinity number value.
    InfinityNumberValue,
    /// Empty string value.
    EmptyStringValue,
}

impl MalformationType {
    /// All malformation types.
    pub const ALL: [Self; 10] = [
        Self::WrongLengthEntityId,
        Self::WrongLengthAttributeId,
        Self::MissingEntityId,
        Self::MissingAttributeId,
        Self::MissingValue,
        Self::EmptyTriples,
        Self::OverflowStringValue,
        Self::NanNumberValue,
        Self::InfinityNumberValue,
        Self::EmptyStringValue,
    ];
}

/// Generator for random `ClientMessage` instances.
///
/// This generator produces deterministic sequences of messages
/// given the same seed, enabling reproducible testing.
pub struct MessageGenerator {
    rng: StdRng,
    config: MessageGenConfig,
    /// Pool of entity IDs for reuse (promotes conflicts/updates).
    entity_pool: Vec<[u8; 16]>,
    /// Pool of attribute IDs for reuse.
    attribute_pool: Vec<[u8; 16]>,
    /// Next request ID.
    next_request_id: u32,
}

impl MessageGenerator {
    /// Create a new message generator with the given seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self::with_config(seed, MessageGenConfig::default())
    }

    /// Create a new message generator with custom configuration.
    #[must_use]
    pub fn with_config(seed: u64, config: MessageGenConfig) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);

        // Pre-populate ID pools
        let entity_pool: Vec<[u8; 16]> = (0..config.id_pool_size)
            .map(|_| {
                let mut id = [0u8; 16];
                rng.fill(&mut id);
                id
            })
            .collect();

        let attribute_pool: Vec<[u8; 16]> = (0..config.id_pool_size)
            .map(|_| {
                let mut id = [0u8; 16];
                rng.fill(&mut id);
                id
            })
            .collect();

        Self {
            rng,
            config,
            entity_pool,
            attribute_pool,
            next_request_id: 1,
        }
    }

    /// Get the configuration.
    #[must_use]
    pub const fn config(&self) -> &MessageGenConfig {
        &self.config
    }

    /// Update the configuration.
    pub const fn set_config(&mut self, config: MessageGenConfig) {
        self.config = config;
    }

    /// Set the malformed rate.
    pub const fn set_malformed_rate(&mut self, rate: f64) {
        self.config.malformed_rate = rate;
    }

    /// Generate the next message.
    ///
    /// This may generate a well-formed or malformed message depending
    /// on the configuration.
    pub fn next_message(&mut self) -> proto::ClientMessage {
        let should_malform = self.rng.random::<f64>() < self.config.malformed_rate;

        if should_malform {
            self.generate_malformed_message()
        } else {
            self.generate_wellformed_message()
        }
    }

    /// Generate a well-formed message.
    pub fn generate_wellformed_message(&mut self) -> proto::ClientMessage {
        let request_id = self.next_request_id;
        self.next_request_id += 1;

        let is_query = self.rng.random::<f64>() < self.config.query_rate;

        let payload = if is_query {
            proto::client_message::Payload::Query(self.generate_query())
        } else {
            proto::client_message::Payload::TripleUpdateRequest(self.generate_update())
        };

        proto::ClientMessage {
            request_id: Some(request_id),
            payload: Some(payload),
        }
    }

    /// Generate a malformed message.
    pub fn generate_malformed_message(&mut self) -> proto::ClientMessage {
        let request_id = self.next_request_id;
        self.next_request_id += 1;

        // Pick a random malformation type
        let malformation_idx = self.rng.random_range(0..MalformationType::ALL.len());
        let malformation = MalformationType::ALL[malformation_idx];

        self.generate_message_with_malformation(request_id, malformation)
    }

    /// Generate a message with a specific malformation.
    #[allow(clippy::too_many_lines)] // Complex match on all malformation types
    pub fn generate_message_with_malformation(
        &mut self,
        request_id: u32,
        malformation: MalformationType,
    ) -> proto::ClientMessage {
        match malformation {
            MalformationType::WrongLengthEntityId => {
                let hlc = Some(self.random_hlc());
                let triple = proto::Triple {
                    entity_id: Some(vec![1, 2, 3]), // Wrong length (3 instead of 16)
                    attribute_id: Some(self.random_attribute_id().to_vec()),
                    value: Some(self.random_value()),
                    hlc,
                };
                proto::ClientMessage {
                    request_id: Some(request_id),
                    payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                        proto::TripleUpdateRequest {
                            triples: vec![triple],
                        },
                    )),
                }
            }
            MalformationType::WrongLengthAttributeId => {
                let hlc = Some(self.random_hlc());
                let triple = proto::Triple {
                    entity_id: Some(self.random_entity_id().to_vec()),
                    attribute_id: Some(vec![1, 2, 3, 4, 5]), // Wrong length (5 instead of 16)
                    value: Some(self.random_value()),
                    hlc,
                };
                proto::ClientMessage {
                    request_id: Some(request_id),
                    payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                        proto::TripleUpdateRequest {
                            triples: vec![triple],
                        },
                    )),
                }
            }
            MalformationType::MissingEntityId => {
                let hlc = Some(self.random_hlc());
                let triple = proto::Triple {
                    entity_id: None, // Missing
                    attribute_id: Some(self.random_attribute_id().to_vec()),
                    value: Some(self.random_value()),
                    hlc,
                };
                proto::ClientMessage {
                    request_id: Some(request_id),
                    payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                        proto::TripleUpdateRequest {
                            triples: vec![triple],
                        },
                    )),
                }
            }
            MalformationType::MissingAttributeId => {
                let hlc = Some(self.random_hlc());
                let triple = proto::Triple {
                    entity_id: Some(self.random_entity_id().to_vec()),
                    attribute_id: None, // Missing
                    value: Some(self.random_value()),
                    hlc,
                };
                proto::ClientMessage {
                    request_id: Some(request_id),
                    payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                        proto::TripleUpdateRequest {
                            triples: vec![triple],
                        },
                    )),
                }
            }
            MalformationType::MissingValue => {
                let hlc = Some(self.random_hlc());
                let triple = proto::Triple {
                    entity_id: Some(self.random_entity_id().to_vec()),
                    attribute_id: Some(self.random_attribute_id().to_vec()),
                    value: None, // Missing
                    hlc,
                };
                proto::ClientMessage {
                    request_id: Some(request_id),
                    payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                        proto::TripleUpdateRequest {
                            triples: vec![triple],
                        },
                    )),
                }
            }
            MalformationType::EmptyTriples => proto::ClientMessage {
                request_id: Some(request_id),
                payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                    proto::TripleUpdateRequest { triples: vec![] },
                )),
            },
            MalformationType::OverflowStringValue => {
                // Generate a very long string (10KB)
                let long_string: String = (0..10_000).map(|_| 'x').collect();
                let hlc = Some(self.random_hlc());
                let triple = proto::Triple {
                    entity_id: Some(self.random_entity_id().to_vec()),
                    attribute_id: Some(self.random_attribute_id().to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(long_string)),
                    }),
                    hlc,
                };
                proto::ClientMessage {
                    request_id: Some(request_id),
                    payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                        proto::TripleUpdateRequest {
                            triples: vec![triple],
                        },
                    )),
                }
            }
            MalformationType::NanNumberValue => {
                let hlc = Some(self.random_hlc());
                let triple = proto::Triple {
                    entity_id: Some(self.random_entity_id().to_vec()),
                    attribute_id: Some(self.random_attribute_id().to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Number(f64::NAN)),
                    }),
                    hlc,
                };
                proto::ClientMessage {
                    request_id: Some(request_id),
                    payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                        proto::TripleUpdateRequest {
                            triples: vec![triple],
                        },
                    )),
                }
            }
            MalformationType::InfinityNumberValue => {
                let hlc = Some(self.random_hlc());
                let triple = proto::Triple {
                    entity_id: Some(self.random_entity_id().to_vec()),
                    attribute_id: Some(self.random_attribute_id().to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::Number(f64::INFINITY)),
                    }),
                    hlc,
                };
                proto::ClientMessage {
                    request_id: Some(request_id),
                    payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                        proto::TripleUpdateRequest {
                            triples: vec![triple],
                        },
                    )),
                }
            }
            MalformationType::EmptyStringValue => {
                let hlc = Some(self.random_hlc());
                let triple = proto::Triple {
                    entity_id: Some(self.random_entity_id().to_vec()),
                    attribute_id: Some(self.random_attribute_id().to_vec()),
                    value: Some(proto::TripleValue {
                        value: Some(proto::triple_value::Value::String(String::new())),
                    }),
                    hlc,
                };
                proto::ClientMessage {
                    request_id: Some(request_id),
                    payload: Some(proto::client_message::Payload::TripleUpdateRequest(
                        proto::TripleUpdateRequest {
                            triples: vec![triple],
                        },
                    )),
                }
            }
        }
    }

    /// Generate a random query.
    fn generate_query(&mut self) -> proto::QueryRequest {
        // Generate a simple query that finds entities with a specific attribute
        let entity_var = proto::QueryPatternVariable {
            label: Some("e".to_string()),
        };
        let value_var = proto::QueryPatternVariable {
            label: Some("v".to_string()),
        };

        let attribute_id = self.random_attribute_id();

        proto::QueryRequest {
            find: vec![entity_var.clone(), value_var.clone()],
            r#where: vec![proto::QueryPattern {
                entity: Some(proto::query_pattern::Entity::EntityVariable(entity_var)),
                attribute: Some(proto::query_pattern::Attribute::AttributeId(
                    attribute_id.to_vec(),
                )),
                value_group: Some(proto::query_pattern::ValueGroup::ValueVariable(value_var)),
            }],
            optional: vec![],
            where_not: vec![],
        }
    }

    /// Generate a random update request.
    fn generate_update(&mut self) -> proto::TripleUpdateRequest {
        let num_triples = self
            .rng
            .random_range(1..=self.config.max_triples_per_update);

        let triples: Vec<proto::Triple> =
            (0..num_triples).map(|_| self.generate_triple()).collect();

        proto::TripleUpdateRequest { triples }
    }

    /// Generate a random well-formed triple.
    fn generate_triple(&mut self) -> proto::Triple {
        proto::Triple {
            entity_id: Some(self.random_entity_id().to_vec()),
            attribute_id: Some(self.random_attribute_id().to_vec()),
            value: Some(self.random_value()),
            hlc: Some(self.random_hlc()),
        }
    }

    /// Get a random entity ID from the pool.
    fn random_entity_id(&mut self) -> [u8; 16] {
        let idx = self.rng.random_range(0..self.entity_pool.len());
        self.entity_pool[idx]
    }

    /// Get a random attribute ID from the pool.
    fn random_attribute_id(&mut self) -> [u8; 16] {
        let idx = self.rng.random_range(0..self.attribute_pool.len());
        self.attribute_pool[idx]
    }

    /// Generate a random HLC timestamp.
    fn random_hlc(&mut self) -> proto::HlcTimestamp {
        proto::HlcTimestamp {
            physical_time_ms: self.rng.random(),
            logical_counter: self.rng.random(),
            node_id: self.rng.random(),
        }
    }

    /// Generate a random value.
    fn random_value(&mut self) -> proto::TripleValue {
        let value_type = self.rng.random_range(0..3);

        let value = match value_type {
            0 => {
                // String
                let len = self.rng.random_range(1..=self.config.max_string_length);
                let s: String = (0..len)
                    .map(|_| {
                        let c = self.rng.random_range(b'a'..=b'z');
                        char::from(c)
                    })
                    .collect();
                proto::triple_value::Value::String(s)
            }
            1 => {
                // Number
                let n: f64 = self.rng.random::<f64>().mul_add(1000.0, -500.0);
                proto::triple_value::Value::Number(n)
            }
            _ => {
                // Boolean
                proto::triple_value::Value::Boolean(self.rng.random())
            }
        };

        proto::TripleValue { value: Some(value) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_generator_deterministic() {
        // Same seed should produce same messages
        let mut generator1 = MessageGenerator::new(12345);
        let mut generator2 = MessageGenerator::new(12345);

        for _ in 0..100 {
            let msg1 = generator1.next_message();
            let msg2 = generator2.next_message();
            assert_eq!(msg1.request_id, msg2.request_id);
            // We can't easily compare payloads, but request IDs should match
        }
    }

    #[test]
    fn test_message_generator_wellformed() {
        let mut generator = MessageGenerator::new(12345);

        for _ in 0..100 {
            let msg = generator.generate_wellformed_message();
            assert!(msg.request_id.is_some());
            assert!(msg.payload.is_some());
        }
    }

    #[test]
    fn test_message_generator_malformed() {
        let mut generator = MessageGenerator::new(12345);

        // Test each malformation type
        for malformation in MalformationType::ALL {
            let msg = generator.generate_message_with_malformation(1, malformation);
            assert!(msg.payload.is_some());
        }
    }

    #[test]
    fn test_message_generator_with_malformed_rate() {
        let config = MessageGenConfig {
            malformed_rate: 1.0, // Always malformed
            ..Default::default()
        };
        let mut generator = MessageGenerator::with_config(12345, config);

        // All messages should be malformed
        for _ in 0..10 {
            let _msg = generator.next_message();
            // We can't easily check if it's malformed, but it should not panic
        }
    }

    #[test]
    fn test_message_generator_request_ids_increment() {
        let mut generator = MessageGenerator::new(12345);

        let msg1 = generator.next_message();
        let msg2 = generator.next_message();
        let msg3 = generator.next_message();

        assert_eq!(msg1.request_id, Some(1));
        assert_eq!(msg2.request_id, Some(2));
        assert_eq!(msg3.request_id, Some(3));
    }
}
