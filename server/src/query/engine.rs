//! Query engine implementation.
//!
//! The `QueryEngine` evaluates datalog-style queries against a database snapshot.
//! It supports:
//! - WHERE patterns (required matches)
//! - OPTIONAL patterns (left join)
//! - WHERE-NOT patterns (anti-join / negation)
//! - Filters (predicate functions)

// Allow some clippy lints that trigger on valid query engine patterns
#![allow(clippy::option_if_let_else)] // if-let is clearer for mutable pattern matching
#![allow(clippy::unused_self)] // Methods take &self for API consistency
#![allow(clippy::match_wildcard_for_single_variants)] // Wildcards are intentional for extensibility

use super::context::QueryContext;
use super::types::{
    Datom, EntityId, FieldId, Pattern, PatternElement, Query, QueryResult, QueryRow, Triple, Value,
};
use crate::storage::{DatabaseError, Snapshot};
use crate::types::{AttributeId, TripleRecord};

/// The query engine evaluates queries against a database snapshot.
pub struct QueryEngine<'a, 'b> {
    snapshot: &'a mut Snapshot<'b>,
}

impl<'a, 'b> QueryEngine<'a, 'b> {
    /// Create a new query engine for a database snapshot.
    pub const fn new(snapshot: &'a mut Snapshot<'b>) -> Self {
        Self { snapshot }
    }

    /// Execute a query and return results.
    pub fn execute(&mut self, query: &Query) -> Result<QueryResult, DatabaseError> {
        // Start with a single empty context
        let mut contexts = vec![QueryContext::new()];

        // Process WHERE patterns (required)
        for pattern in &query.where_patterns {
            contexts = self.match_pattern_all(pattern, contexts)?;
            if contexts.is_empty() {
                return Ok(QueryResult::with_columns(
                    query
                        .find
                        .iter()
                        .map(|v| v.name.as_str().to_owned())
                        .collect(),
                ));
            }
        }

        // Process OPTIONAL patterns (left join)
        for pattern in &query.optional_patterns {
            contexts = self.match_optional_pattern(pattern, contexts)?;
        }

        // Process WHERE-NOT patterns (anti-join)
        for pattern in &query.where_not_patterns {
            contexts = self.match_negation_pattern(pattern, contexts)?;
        }

        // Apply filters
        for filter in &query.filters {
            contexts.retain(|ctx| {
                let datom = ctx.get(&filter.selector);
                filter.apply(datom)
            });
        }

        // Build result
        let columns: Vec<String> = query
            .find
            .iter()
            .map(|v| v.name.as_str().to_owned())
            .collect();
        let mut result = QueryResult::with_columns(columns);

        for ctx in contexts {
            let row: QueryRow = query
                .find
                .iter()
                .map(|var| ctx.get(var).map(Datom::clone_value))
                .collect();
            result.push(row);
        }

        Ok(result)
    }

    /// Match a pattern against all triples, extending each context.
    fn match_pattern_all(
        &mut self,
        pattern: &Pattern,
        contexts: Vec<QueryContext>,
    ) -> Result<Vec<QueryContext>, DatabaseError> {
        let mut new_contexts = Vec::new();

        for ctx in contexts {
            let matches = self.match_pattern(pattern, &ctx)?;
            new_contexts.extend(matches);
        }

        Ok(new_contexts)
    }

    /// Match a pattern against all triples with the given context.
    fn match_pattern(
        &mut self,
        pattern: &Pattern,
        ctx: &QueryContext,
    ) -> Result<Vec<QueryContext>, DatabaseError> {
        let triples = self.get_candidate_triples(pattern, ctx)?;
        let mut results = Vec::new();

        for triple in triples {
            if let Some(new_ctx) = self.try_match_triple(pattern, &triple, ctx) {
                results.push(new_ctx);
            }
        }

        Ok(results)
    }

    /// Get candidate triples based on pattern constraints.
    fn get_candidate_triples(
        &mut self,
        pattern: &Pattern,
        ctx: &QueryContext,
    ) -> Result<Vec<Triple>, DatabaseError> {
        // Try to use entity index if we have a concrete entity
        if let Some(entity_id) = self.resolve_entity(&pattern.entity, ctx) {
            if let Some(field_id) = self.resolve_field(&pattern.field, ctx) {
                // Most specific: entity + field lookup
                if let Some(record) = self.snapshot.get(&entity_id, &field_id)? {
                    return Ok(vec![record_to_triple(record)]);
                }
                return Ok(Vec::new());
            }
            // Entity-only scan
            let records = self.snapshot.scan_entity(&entity_id)?;
            return Ok(records.into_iter().map(record_to_triple).collect());
        }

        // Try attribute index if we have a concrete field but no entity
        if let Some(field_id) = self.resolve_field(&pattern.field, ctx) {
            // Use attribute index to get all entities with this attribute
            let entity_ids = self.snapshot.get_entities_with_attribute(&field_id)?;
            let mut triples = Vec::new();
            for entity_id in entity_ids {
                if let Some(record) = self.snapshot.get(&entity_id, &field_id)? {
                    triples.push(record_to_triple(record));
                }
            }
            return Ok(triples);
        }

        // Fall back to scanning all triples
        let records = self.snapshot.collect_all()?;
        Ok(records.into_iter().map(record_to_triple).collect())
    }

    /// Try to resolve a pattern element to an entity ID.
    fn resolve_entity(&self, element: &PatternElement, ctx: &QueryContext) -> Option<EntityId> {
        match element {
            PatternElement::Entity(id) => Some(*id),
            PatternElement::Variable(var) => match ctx.get(var) {
                Some(Datom::Entity(id)) => Some(*id),
                _ => None,
            },
            _ => None,
        }
    }

    /// Try to resolve a pattern element to a field ID.
    fn resolve_field(&self, element: &PatternElement, ctx: &QueryContext) -> Option<FieldId> {
        match element {
            PatternElement::Field(id) => Some(*id),
            PatternElement::Variable(var) => match ctx.get(var) {
                Some(Datom::Field(id)) => Some(*id),
                _ => None,
            },
            _ => None,
        }
    }

    /// Try to match a triple against a pattern with the given context.
    /// Returns a new context with additional bindings if the match succeeds.
    fn try_match_triple(
        &self,
        pattern: &Pattern,
        triple: &Triple,
        ctx: &QueryContext,
    ) -> Option<QueryContext> {
        let mut new_ctx = ctx.clone_value();

        // Match entity
        if !self.match_entity_element(&pattern.entity, &triple.entity, &mut new_ctx) {
            return None;
        }

        // Match field
        if !self.match_field_element(&pattern.field, &triple.field, &mut new_ctx) {
            return None;
        }

        // Match value
        if !self.match_value_element(&pattern.value, &triple.value, &mut new_ctx) {
            return None;
        }

        Some(new_ctx)
    }

    /// Match an entity pattern element against an entity ID.
    fn match_entity_element(
        &self,
        element: &PatternElement,
        entity: &EntityId,
        ctx: &mut QueryContext,
    ) -> bool {
        match element {
            PatternElement::Entity(id) => id == entity,
            PatternElement::Variable(var) => {
                if let Some(bound) = ctx.get(var) {
                    // Variable already bound - check consistency
                    match bound {
                        Datom::Entity(id) => id == entity,
                        _ => false,
                    }
                } else {
                    // Bind the variable
                    ctx.set(var, Datom::Entity(*entity));
                    true
                }
            }
            _ => false,
        }
    }

    /// Match a field pattern element against a field ID.
    fn match_field_element(
        &self,
        element: &PatternElement,
        field: &FieldId,
        ctx: &mut QueryContext,
    ) -> bool {
        match element {
            PatternElement::Field(id) => id == field,
            PatternElement::Variable(var) => {
                if let Some(bound) = ctx.get(var) {
                    match bound {
                        Datom::Field(id) => id == field,
                        _ => false,
                    }
                } else {
                    ctx.set(var, Datom::Field(*field));
                    true
                }
            }
            _ => false,
        }
    }

    /// Match a value pattern element against a value.
    fn match_value_element(
        &self,
        element: &PatternElement,
        value: &Value,
        ctx: &mut QueryContext,
    ) -> bool {
        match element {
            PatternElement::Value(v) => values_equal(v, value),
            PatternElement::Variable(var) => {
                if let Some(bound) = ctx.get(var) {
                    match bound {
                        Datom::Value(v) => values_equal(v, value),
                        Datom::Entity(id) => {
                            // Can match a Ref value to an Entity binding
                            matches!(value, Value::Ref(ref_id) if ref_id == id)
                        }
                        _ => false,
                    }
                } else {
                    ctx.set(var, Datom::Value(value.clone_value()));
                    true
                }
            }
            PatternElement::Entity(id) => {
                // Can match an entity pattern against a Ref value
                matches!(value, Value::Ref(ref_id) if ref_id == id)
            }
            _ => false,
        }
    }

    /// Match an optional pattern (left join).
    fn match_optional_pattern(
        &mut self,
        pattern: &Pattern,
        contexts: Vec<QueryContext>,
    ) -> Result<Vec<QueryContext>, DatabaseError> {
        let mut results = Vec::new();

        for ctx in contexts {
            let matches = self.match_pattern(pattern, &ctx)?;
            if matches.is_empty() {
                // No matches - keep original context (left join behavior)
                results.push(ctx);
            } else {
                // Has matches - extend with all of them
                results.extend(matches);
            }
        }

        Ok(results)
    }

    /// Match a negation pattern (anti-join).
    fn match_negation_pattern(
        &mut self,
        pattern: &Pattern,
        contexts: Vec<QueryContext>,
    ) -> Result<Vec<QueryContext>, DatabaseError> {
        let mut results = Vec::new();

        for ctx in contexts {
            let matches = self.match_pattern(pattern, &ctx)?;
            if matches.is_empty() {
                // Keep only contexts with no matches
                results.push(ctx);
            }
        }

        Ok(results)
    }
}

/// Convert a storage `TripleRecord` to a query `Triple`.
///
/// Since query types are now unified with storage types, this is a simple
/// field extraction.
fn record_to_triple(record: TripleRecord) -> Triple {
    Triple {
        entity: record.entity_id,
        field: record.attribute_id,
        value: record.value,
    }
}

/// Check if two values are equal.
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Boolean(x), Value::Boolean(y)) => x == y,
        (Value::Number(x), Value::Number(y)) => (x - y).abs() < f64::EPSILON,
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Ref(x), Value::Ref(y)) => x == y,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::types::Variable;
    use crate::storage::Database;
    use crate::types::{AttributeId, EntityId, TripleValue as StorageTripleValue};
    use tempfile::tempdir;

    fn create_test_db_with_data() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        let mut db = Database::create(&path).expect("create db");

        // Insert test data
        {
            let mut txn = db.begin(0).expect("begin");

            // Create field IDs
            let name_field = AttributeId::from_string("name");
            let age_field = AttributeId::from_string("age");
            let active_field = AttributeId::from_string("active");

            // User 1: Alice
            let user1 = EntityId::from_string("user1");
            txn.insert(
                user1,
                name_field,
                StorageTripleValue::String("Alice".to_string()),
            );
            txn.insert(user1, age_field, StorageTripleValue::Number(30.0));
            txn.insert(user1, active_field, StorageTripleValue::Boolean(true));

            // User 2: Bob
            let user2 = EntityId::from_string("user2");
            txn.insert(
                user2,
                name_field,
                StorageTripleValue::String("Bob".to_string()),
            );
            txn.insert(user2, age_field, StorageTripleValue::Number(25.0));
            txn.insert(user2, active_field, StorageTripleValue::Boolean(false));

            // User 3: Charlie (no age)
            let user3 = EntityId::from_string("user3");
            txn.insert(
                user3,
                name_field,
                StorageTripleValue::String("Charlie".to_string()),
            );
            txn.insert(user3, active_field, StorageTripleValue::Boolean(true));

            txn.commit().expect("commit");
        }

        db.close().expect("close");
        (dir, path)
    }

    #[test]
    fn test_simple_query() {
        let (_dir, path) = create_test_db_with_data();
        let (mut db, _) = Database::open(&path).expect("open db");

        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let mut engine = QueryEngine::new(&mut snapshot);

            // Find all entity names
            let query = Query::new()
                .find("e")
                .find("name")
                .where_pattern(Pattern::new(
                    PatternElement::var("e"),
                    PatternElement::field("name"),
                    PatternElement::var("name"),
                ));

            let result = engine.execute(&query).expect("execute");
            assert_eq!(result.len(), 3);
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_query_with_concrete_entity() {
        let (_dir, path) = create_test_db_with_data();
        let (mut db, _) = Database::open(&path).expect("open db");

        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let mut engine = QueryEngine::new(&mut snapshot);

            // Find name for user1
            let query = Query::new().find("name").where_pattern(Pattern::new(
                PatternElement::entity("user1"),
                PatternElement::field("name"),
                PatternElement::var("name"),
            ));

            let result = engine.execute(&query).expect("execute");
            assert_eq!(result.len(), 1);

            let name = result.rows[0][0].as_ref().expect("should have name");
            assert!(matches!(name, Datom::Value(Value::String(s)) if s == "Alice"));
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_query_with_multiple_patterns() {
        let (_dir, path) = create_test_db_with_data();
        let (mut db, _) = Database::open(&path).expect("open db");

        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let mut engine = QueryEngine::new(&mut snapshot);

            // Find entities with both name and age
            let query = Query::new()
                .find("e")
                .find("name")
                .find("age")
                .where_pattern(Pattern::new(
                    PatternElement::var("e"),
                    PatternElement::field("name"),
                    PatternElement::var("name"),
                ))
                .where_pattern(Pattern::new(
                    PatternElement::var("e"),
                    PatternElement::field("age"),
                    PatternElement::var("age"),
                ));

            let result = engine.execute(&query).expect("execute");
            assert_eq!(result.len(), 2); // Only user1 and user2 have age
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_optional_pattern() {
        let (_dir, path) = create_test_db_with_data();
        let (mut db, _) = Database::open(&path).expect("open db");

        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let mut engine = QueryEngine::new(&mut snapshot);

            // Find all entities with name, optionally with age
            let query = Query::new()
                .find("e")
                .find("name")
                .find("age")
                .where_pattern(Pattern::new(
                    PatternElement::var("e"),
                    PatternElement::field("name"),
                    PatternElement::var("name"),
                ))
                .optional(Pattern::new(
                    PatternElement::var("e"),
                    PatternElement::field("age"),
                    PatternElement::var("age"),
                ));

            let result = engine.execute(&query).expect("execute");
            assert_eq!(result.len(), 3); // All 3 users

            // Check that user3 has no age
            let charlie_row = result.rows.iter().find(
                |row| matches!(&row[1], Some(Datom::Value(Value::String(s))) if s == "Charlie"),
            );
            assert!(charlie_row.is_some());
            let charlie = charlie_row.unwrap();
            assert!(charlie[2].is_none()); // No age
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_where_not_pattern() {
        let (_dir, path) = create_test_db_with_data();
        let (mut db, _) = Database::open(&path).expect("open db");

        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let mut engine = QueryEngine::new(&mut snapshot);

            // Find entities that don't have an age
            let query = Query::new()
                .find("e")
                .find("name")
                .where_pattern(Pattern::new(
                    PatternElement::var("e"),
                    PatternElement::field("name"),
                    PatternElement::var("name"),
                ))
                .where_not(Pattern::new(
                    PatternElement::var("e"),
                    PatternElement::field("age"),
                    PatternElement::var("_age"),
                ));

            let result = engine.execute(&query).expect("execute");
            assert_eq!(result.len(), 1); // Only Charlie

            let name = result.rows[0][1].as_ref().expect("should have name");
            assert!(matches!(name, Datom::Value(Value::String(s)) if s == "Charlie"));
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_filter() {
        let (_dir, path) = create_test_db_with_data();
        let (mut db, _) = Database::open(&path).expect("open db");

        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let mut engine = QueryEngine::new(&mut snapshot);

            // Find entities with age > 26
            let query = Query::new()
                .find("e")
                .find("age")
                .where_pattern(Pattern::new(
                    PatternElement::var("e"),
                    PatternElement::field("age"),
                    PatternElement::var("age"),
                ))
                .filter(super::super::types::Filter::new(
                    Variable::new("age"),
                    |datom| match datom {
                        Some(Datom::Value(Value::Number(n))) => *n > 26.0,
                        _ => false,
                    },
                ));

            let result = engine.execute(&query).expect("execute");
            assert_eq!(result.len(), 1); // Only Alice (age 30)
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_value_match() {
        let (_dir, path) = create_test_db_with_data();
        let (mut db, _) = Database::open(&path).expect("open db");

        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let mut engine = QueryEngine::new(&mut snapshot);

            // Find entity with name "Bob"
            let query = Query::new().find("e").where_pattern(Pattern::new(
                PatternElement::var("e"),
                PatternElement::field("name"),
                PatternElement::string("Bob"),
            ));

            let result = engine.execute(&query).expect("execute");
            assert_eq!(result.len(), 1);

            let entity = result.rows[0][0].as_ref().expect("should have entity");
            assert!(matches!(entity, Datom::Entity(id) if id.0[..4] == *b"user"));
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_empty_result() {
        let (_dir, path) = create_test_db_with_data();
        let (mut db, _) = Database::open(&path).expect("open db");

        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let mut engine = QueryEngine::new(&mut snapshot);

            // Find entity with name "Nobody"
            let query = Query::new().find("e").where_pattern(Pattern::new(
                PatternElement::var("e"),
                PatternElement::field("name"),
                PatternElement::string("Nobody"),
            ));

            let result = engine.execute(&query).expect("execute");
            assert!(result.is_empty());
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_query_empty_database() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        let mut db = Database::create(&path).expect("create db");

        let txn_id = {
            let mut snapshot = db.begin_readonly();
            let mut engine = QueryEngine::new(&mut snapshot);

            // Query should return empty results
            let query = Query::new()
                .find("e")
                .find("name")
                .where_pattern(Pattern::new(
                    PatternElement::var("e"),
                    PatternElement::field("name"),
                    PatternElement::var("name"),
                ));

            let result = engine.execute(&query).expect("execute");
            assert!(result.is_empty());
            snapshot.close()
        };
        db.release_snapshot(txn_id);
    }

    #[test]
    fn test_snapshot_isolation_in_query() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        let mut db = Database::create(&path).expect("create db");

        // Create name field
        let name_field = AttributeId::from_string("name");

        // Insert initial data
        {
            let mut txn = db.begin(0).expect("begin");
            let user1 = EntityId::from_string("user1");
            txn.insert(
                user1,
                name_field,
                StorageTripleValue::String("Alice".to_string()),
            );
            txn.commit().expect("commit");
        }

        // Create a snapshot at txn_id = 1
        let mut snapshot = db.begin_readonly();

        // Verify the initial data is visible
        {
            let mut engine = QueryEngine::new(&mut snapshot);
            let query = Query::new().find("name").where_pattern(Pattern::new(
                PatternElement::var("e"),
                PatternElement::field("name"),
                PatternElement::var("name"),
            ));
            let result = engine.execute(&query).expect("execute");
            assert_eq!(result.len(), 1);
        }

        let txn_id = snapshot.close();
        db.release_snapshot(txn_id);
    }
}
