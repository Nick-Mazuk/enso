//! Query execution engine.
//!
//! Executes queries against the storage engine and converts results
//! to protocol types.

#![allow(dead_code)] // Query executor will be used when QueryRequest is implemented

use crate::storage::{AttributeId, EntityId, Transaction, TransactionError, TripleRecord};
use crate::types::triple::{Triple, TripleValue};

/// A query executor that operates within a transaction.
pub struct QueryExecutor<'a, 'b> {
    txn: &'a mut Transaction<'b>,
}

impl<'a, 'b> QueryExecutor<'a, 'b> {
    /// Create a new query executor for the given transaction.
    pub const fn new(txn: &'a mut Transaction<'b>) -> Self {
        Self { txn }
    }

    /// Look up a single triple by entity and attribute ID.
    pub fn get(
        &mut self,
        entity_id: &EntityId,
        attribute_id: &AttributeId,
    ) -> Result<Option<Triple>, QueryError> {
        let record = self.txn.get(entity_id, attribute_id)?;
        Ok(record.map(triple_from_record))
    }

    /// Scan all triples for an entity.
    pub fn scan_entity(&mut self, entity_id: &EntityId) -> Result<Vec<Triple>, QueryError> {
        let mut results = Vec::new();
        let mut iter = self.txn.scan_entity(entity_id)?;

        while let Some(record) = iter.next_record()? {
            // Filter out deleted records
            if !record.is_deleted() {
                results.push(triple_from_record(record));
            }
        }

        Ok(results)
    }

    /// Scan all triples in the database.
    ///
    /// Use with caution - this can be expensive for large databases.
    pub fn scan_all(&mut self) -> Result<Vec<Triple>, QueryError> {
        let mut results = Vec::new();
        let mut cursor = self.txn.cursor()?;

        while let Some(record) = cursor.next_record()? {
            if !record.is_deleted() {
                results.push(triple_from_record(record));
            }
        }

        Ok(results)
    }
}

/// Convert a storage `TripleRecord` to a protocol `Triple`.
fn triple_from_record(record: TripleRecord) -> Triple {
    Triple {
        entity_id: record.entity_id,
        attribute_id: record.attribute_id,
        value: value_from_storage(record.value),
        hlc: record.created_hlc,
    }
}

/// Convert a storage `TripleValue` to a protocol `TripleValue`.
pub fn value_from_storage(value: crate::storage::TripleValue) -> TripleValue {
    match value {
        crate::storage::TripleValue::Null => {
            // Protocol doesn't have null, use empty string as fallback
            TripleValue::String(String::new())
        }
        crate::storage::TripleValue::Boolean(b) => TripleValue::Boolean(b),
        crate::storage::TripleValue::Number(n) => TripleValue::Number(n),
        crate::storage::TripleValue::String(s) => TripleValue::String(s),
    }
}

/// Convert a protocol `TripleValue` to a storage `TripleValue`.
pub fn value_to_storage(value: TripleValue) -> crate::storage::TripleValue {
    match value {
        TripleValue::Boolean(b) => crate::storage::TripleValue::Boolean(b),
        TripleValue::Number(n) => crate::storage::TripleValue::Number(n),
        TripleValue::String(s) => crate::storage::TripleValue::String(s),
    }
}

/// Errors that can occur during query execution.
#[derive(Debug)]
pub enum QueryError {
    /// Transaction error.
    Transaction(TransactionError),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transaction(e) => write!(f, "transaction error: {e}"),
        }
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transaction(e) => Some(e),
        }
    }
}

impl From<TransactionError> for QueryError {
    fn from(e: TransactionError) -> Self {
        Self::Transaction(e)
    }
}

impl From<crate::storage::indexes::primary::PrimaryIndexError> for QueryError {
    fn from(e: crate::storage::indexes::primary::PrimaryIndexError) -> Self {
        Self::Transaction(TransactionError::Index(e))
    }
}
