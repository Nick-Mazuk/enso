//! Query context for managing variable bindings during query evaluation.
//!
//! The `QueryContext` holds the current bindings of query variables to datoms.
//! It's used during pattern matching to track what values variables are bound to.

use std::collections::HashMap;

use super::types::{Datom, Variable};

/// A context that holds variable bindings during query evaluation.
///
/// Variables are bound to `Datom` values as patterns are matched.
/// The context can be copied to explore different binding branches.
#[derive(Debug, Default)]
pub struct QueryContext {
    /// Map from variable names to their bound values.
    bindings: HashMap<String, Datom>,
}

impl QueryContext {
    /// Create a new empty context.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a variable to a value.
    ///
    /// If the variable is already bound, the old value is replaced.
    pub fn set(&mut self, variable: &Variable, value: Datom) {
        self.bindings
            .insert(variable.name.as_str().to_owned(), value);
    }

    /// Get the value bound to a variable.
    #[must_use]
    pub fn get(&self, variable: &Variable) -> Option<&Datom> {
        self.bindings.get(&variable.name)
    }

    /// Get the value bound to a variable by name.
    #[must_use]
    pub fn get_by_name(&self, name: &str) -> Option<&Datom> {
        self.bindings.get(name)
    }

    /// Check if a variable is bound.
    #[must_use]
    pub fn has(&self, variable: &Variable) -> bool {
        self.bindings.contains_key(&variable.name)
    }

    /// Check if a variable is bound by name.
    #[must_use]
    pub fn has_name(&self, name: &str) -> bool {
        self.bindings.contains_key(name)
    }

    /// Get the number of bound variables.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    /// Check if the context has no bindings.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Remove a binding from the context.
    pub fn remove(&mut self, variable: &Variable) -> Option<Datom> {
        self.bindings.remove(&variable.name)
    }

    /// Get an iterator over all bindings.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Datom)> {
        self.bindings.iter()
    }

    /// Create a copy of this context.
    ///
    /// This is used instead of Clone to comply with project policy.
    #[must_use]
    pub fn clone_value(&self) -> Self {
        let mut new_bindings = HashMap::with_capacity(self.bindings.len());
        for (name, value) in &self.bindings {
            new_bindings.insert(name.as_str().to_owned(), value.clone_value());
        }
        Self {
            bindings: new_bindings,
        }
    }

    /// Merge another context into this one.
    ///
    /// Bindings from the other context override existing bindings.
    pub fn merge(&mut self, other: &Self) {
        for (name, value) in &other.bindings {
            self.bindings
                .insert(name.as_str().to_owned(), value.clone_value());
        }
    }

    /// Check if this context is consistent with another.
    ///
    /// Two contexts are consistent if all shared variables have the same value.
    #[must_use]
    pub fn is_consistent_with(&self, other: &Self) -> bool {
        for (name, value) in &self.bindings {
            if let Some(other_value) = other.bindings.get(name) {
                if value != other_value {
                    return false;
                }
            }
        }
        true
    }

    /// Create a new context that combines this one with another.
    ///
    /// Returns `None` if the contexts are inconsistent.
    #[must_use]
    pub fn combine(&self, other: &Self) -> Option<Self> {
        if !self.is_consistent_with(other) {
            return None;
        }

        let mut combined = self.clone_value();
        combined.merge(other);
        Some(combined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::types::{EntityId, Value};

    #[test]
    fn test_context_basic() {
        let mut ctx = QueryContext::new();
        let var_x = Variable::new("x");
        let var_y = Variable::new("y");

        assert!(ctx.is_empty());
        assert!(!ctx.has(&var_x));

        ctx.set(&var_x, Datom::string("hello"));
        assert!(ctx.has(&var_x));
        assert!(!ctx.has(&var_y));
        assert_eq!(ctx.len(), 1);

        let value = ctx.get(&var_x).expect("should have value");
        assert!(matches!(value, Datom::Value(Value::String(s)) if s == "hello"));
    }

    #[test]
    fn test_context_clone() {
        let mut ctx1 = QueryContext::new();
        let var_x = Variable::new("x");

        ctx1.set(&var_x, Datom::number(42.0));

        let ctx2 = ctx1.clone_value();
        assert!(ctx2.has(&var_x));

        let value = ctx2.get(&var_x).expect("should have value");
        assert!(
            matches!(value, Datom::Value(Value::Number(n)) if (*n - 42.0).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn test_context_merge() {
        let mut ctx1 = QueryContext::new();
        let mut ctx2 = QueryContext::new();
        let var_x = Variable::new("x");
        let var_y = Variable::new("y");

        ctx1.set(&var_x, Datom::string("a"));
        ctx2.set(&var_y, Datom::string("b"));

        ctx1.merge(&ctx2);

        assert!(ctx1.has(&var_x));
        assert!(ctx1.has(&var_y));
        assert_eq!(ctx1.len(), 2);
    }

    #[test]
    fn test_context_consistency() {
        let mut ctx1 = QueryContext::new();
        let mut ctx2 = QueryContext::new();
        let var_x = Variable::new("x");

        ctx1.set(&var_x, Datom::string("same"));
        ctx2.set(&var_x, Datom::string("same"));

        assert!(ctx1.is_consistent_with(&ctx2));

        ctx2.set(&var_x, Datom::string("different"));
        assert!(!ctx1.is_consistent_with(&ctx2));
    }

    #[test]
    fn test_context_combine() {
        let mut ctx1 = QueryContext::new();
        let mut ctx2 = QueryContext::new();
        let var_x = Variable::new("x");
        let var_y = Variable::new("y");

        ctx1.set(&var_x, Datom::string("a"));
        ctx2.set(&var_y, Datom::string("b"));

        let combined = ctx1.combine(&ctx2).expect("should combine");
        assert!(combined.has(&var_x));
        assert!(combined.has(&var_y));

        // Inconsistent contexts don't combine
        ctx2.set(&var_x, Datom::string("different"));
        assert!(ctx1.combine(&ctx2).is_none());
    }

    #[test]
    fn test_context_with_entity() {
        let mut ctx = QueryContext::new();
        let var_e = Variable::new("e");

        ctx.set(&var_e, Datom::Entity(EntityId::from_string("user1")));

        let value = ctx.get(&var_e).expect("should have value");
        assert!(matches!(value, Datom::Entity(_)));
    }
}
