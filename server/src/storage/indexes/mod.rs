//! Index implementations for the storage engine.
//!
//! Indexes provide efficient access patterns for triple data:
//! - Primary index: (`entity_id`, `attribute_id`) -> full triple record
//! - Attribute index: `attribute_id` -> [(`entity_id`, pointer)] (Phase 4)
//! - Value indexes: (`attribute_id`, value) -> [`entity_id`] (Phase 4)

pub mod primary;
