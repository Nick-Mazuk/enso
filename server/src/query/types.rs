//! Query types for the datalog-style query engine.
//!
//! This module defines the core types used in the query system:
//! - `Datom` - A single piece of data (entity ID, field, or value)
//! - `Triple` - A complete fact: (entity, attribute, value)
//! - `Variable` - A placeholder in query patterns
//! - `Pattern` - A query pattern with variables or concrete values
//! - `Query` - A complete query with where, optional, filters, and whereNot

#![allow(clippy::type_complexity)] // Complex boxed trait objects are necessary for filters

use std::fmt;

// Re-export storage types for use in queries.
// This unifies the type system so queries use the same types as storage.
pub use crate::types::{AttributeId, EntityId, TripleValue};

/// Type alias for backwards compatibility.
/// In queries, we refer to attributes as "fields".
pub type FieldId = AttributeId;

/// Type alias for query values.
/// This is the same as `TripleValue` from the storage layer.
pub type Value = TripleValue;

/// A datom is any piece of data that can appear in a triple.
#[derive(Debug, PartialEq)]
pub enum Datom {
    /// An entity ID.
    Entity(EntityId),
    /// A field/attribute ID.
    Field(FieldId),
    /// A value.
    Value(Value),
}

impl Datom {
    /// Create an entity datom from a string.
    #[must_use]
    pub fn entity(s: &str) -> Self {
        Self::Entity(EntityId::from_string(s))
    }

    /// Create a field datom from a string.
    #[must_use]
    pub fn field(s: &str) -> Self {
        Self::Field(FieldId::from_string(s))
    }

    /// Create a string value datom.
    #[must_use]
    pub fn string(s: impl Into<String>) -> Self {
        Self::Value(Value::string(s))
    }

    /// Create a number value datom.
    #[must_use]
    pub fn number(n: impl Into<f64>) -> Self {
        Self::Value(Value::number(n))
    }

    /// Create a boolean value datom.
    #[must_use]
    pub const fn boolean(b: bool) -> Self {
        Self::Value(Value::boolean(b))
    }

    /// Create a copy of this datom.
    ///
    /// This is used instead of Clone to comply with project policy.
    #[must_use]
    pub fn clone_value(&self) -> Self {
        match self {
            Self::Entity(id) => Self::Entity(*id),
            Self::Field(id) => Self::Field(*id),
            Self::Value(v) => Self::Value(v.clone_value()),
        }
    }
}

impl fmt::Display for Datom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entity(id) => write!(f, "#{id}"),
            Self::Field(id) => write!(f, ":{id}"),
            Self::Value(v) => write!(f, "{v}"),
        }
    }
}

/// A complete triple (entity, attribute, value).
#[derive(Debug, PartialEq)]
pub struct Triple {
    /// The entity this triple describes.
    pub entity: EntityId,
    /// The attribute/field.
    pub field: FieldId,
    /// The value.
    pub value: Value,
}

impl Triple {
    /// Create a new triple.
    #[must_use]
    pub const fn new(entity: EntityId, field: FieldId, value: Value) -> Self {
        Self {
            entity,
            field,
            value,
        }
    }

    /// Create a copy of this triple.
    ///
    /// This is used instead of Clone to comply with project policy.
    #[must_use]
    pub fn clone_value(&self) -> Self {
        Self {
            entity: self.entity,
            field: self.field,
            value: self.value.clone_value(),
        }
    }
}

/// A query variable.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Variable {
    /// The variable name.
    pub name: String,
}

impl Variable {
    /// Create a new variable.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Create a copy of this variable.
    ///
    /// This is used instead of Clone to comply with project policy.
    #[must_use]
    pub fn clone_value(&self) -> Self {
        Self {
            name: self.name.as_str().to_owned(),
        }
    }
}

impl fmt::Display for Variable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "?{}", self.name)
    }
}

/// A pattern element - either a concrete value or a variable.
#[derive(Debug, PartialEq)]
pub enum PatternElement {
    /// A concrete entity ID.
    Entity(EntityId),
    /// A concrete field ID.
    Field(FieldId),
    /// A concrete value.
    Value(Value),
    /// A variable to be bound.
    Variable(Variable),
}

impl PatternElement {
    /// Create a variable pattern element.
    #[must_use]
    pub fn var(name: impl Into<String>) -> Self {
        Self::Variable(Variable::new(name))
    }

    /// Create an entity pattern element.
    #[must_use]
    pub fn entity(s: &str) -> Self {
        Self::Entity(EntityId::from_string(s))
    }

    /// Create a field pattern element.
    #[must_use]
    pub fn field(s: &str) -> Self {
        Self::Field(FieldId::from_string(s))
    }

    /// Create a string value pattern element.
    #[must_use]
    pub fn string(s: impl Into<String>) -> Self {
        Self::Value(Value::string(s))
    }

    /// Create a number value pattern element.
    #[must_use]
    pub fn number(n: impl Into<f64>) -> Self {
        Self::Value(Value::number(n))
    }

    /// Check if this is a variable.
    #[must_use]
    pub const fn is_variable(&self) -> bool {
        matches!(self, Self::Variable(_))
    }

    /// Get the variable if this is one.
    #[must_use]
    pub const fn as_variable(&self) -> Option<&Variable> {
        match self {
            Self::Variable(v) => Some(v),
            _ => None,
        }
    }
}

/// A query pattern - a triple where any element can be a variable.
#[derive(Debug, PartialEq)]
pub struct Pattern {
    /// The entity pattern (ID or variable).
    pub entity: PatternElement,
    /// The field pattern (field or variable).
    pub field: PatternElement,
    /// The value pattern (value or variable).
    pub value: PatternElement,
}

impl Pattern {
    /// Create a new pattern.
    #[must_use]
    pub const fn new(entity: PatternElement, field: PatternElement, value: PatternElement) -> Self {
        Self {
            entity,
            field,
            value,
        }
    }

    /// Create a pattern from three elements.
    #[must_use]
    pub fn from_parts(
        entity: impl Into<PatternElement>,
        field: impl Into<PatternElement>,
        value: impl Into<PatternElement>,
    ) -> Self {
        Self {
            entity: entity.into(),
            field: field.into(),
            value: value.into(),
        }
    }
}

/// A filter that can be applied to query results.
pub struct Filter {
    /// The variable to filter on.
    pub selector: Variable,
    /// The filter predicate.
    pub predicate: Box<dyn Fn(Option<&Datom>) -> bool + Send + Sync>,
}

impl Filter {
    /// Create a new filter.
    pub fn new<F>(selector: Variable, predicate: F) -> Self
    where
        F: Fn(Option<&Datom>) -> bool + Send + Sync + 'static,
    {
        Self {
            selector,
            predicate: Box::new(predicate),
        }
    }

    /// Apply the filter to a datom.
    #[must_use]
    pub fn apply(&self, datom: Option<&Datom>) -> bool {
        (self.predicate)(datom)
    }
}

impl fmt::Debug for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Filter")
            .field("selector", &self.selector)
            .field("predicate", &"<fn>")
            .finish()
    }
}

/// A complete query.
#[derive(Debug, Default)]
pub struct Query {
    /// Variables to return in results.
    pub find: Vec<Variable>,
    /// Required patterns (conjunction).
    pub where_patterns: Vec<Pattern>,
    /// Optional patterns (left join).
    pub optional_patterns: Vec<Pattern>,
    /// Negation patterns (anti-join).
    pub where_not_patterns: Vec<Pattern>,
    /// Filters to apply.
    pub filters: Vec<Filter>,
}

impl Query {
    /// Create a new empty query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a variable to the find clause.
    pub fn find(mut self, var: impl Into<String>) -> Self {
        self.find.push(Variable::new(var));
        self
    }

    /// Add a where pattern.
    pub fn where_pattern(mut self, pattern: Pattern) -> Self {
        self.where_patterns.push(pattern);
        self
    }

    /// Add an optional pattern.
    pub fn optional(mut self, pattern: Pattern) -> Self {
        self.optional_patterns.push(pattern);
        self
    }

    /// Add a where-not pattern.
    pub fn where_not(mut self, pattern: Pattern) -> Self {
        self.where_not_patterns.push(pattern);
        self
    }

    /// Add a filter.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }
}

/// A row of query results.
pub type QueryRow = Vec<Option<Datom>>;

/// Query results.
#[derive(Debug, Default)]
pub struct QueryResult {
    /// The variable names in order.
    pub columns: Vec<String>,
    /// The result rows.
    pub rows: Vec<QueryRow>,
}

impl QueryResult {
    /// Create empty results.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create results with columns.
    #[must_use]
    pub const fn with_columns(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    /// Add a row.
    pub fn push(&mut self, row: QueryRow) {
        self.rows.push(row);
    }

    /// Get the number of rows.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_id_from_string() {
        let id = EntityId::from_string("test");
        assert_eq!(&id.0[..4], b"test");
        assert_eq!(&id.0[4..], &[0u8; 12]);
    }

    #[test]
    fn test_value_clone() {
        let v1 = Value::String("hello".to_owned());
        let v2 = v1.clone_value();
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_datom_clone() {
        let d1 = Datom::string("world");
        let d2 = d1.clone_value();
        assert_eq!(d1, d2);
    }
}
