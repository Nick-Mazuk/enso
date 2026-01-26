//! Query execution engine.
//!
//! Executes queries against the storage engine and returns storage types directly.

#![allow(dead_code)] // Query executor will be used when QueryRequest is implemented

use crate::storage::{Transaction, TransactionError};
use crate::types::{AttributeId, EntityId, TripleRecord};

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
    ) -> Result<Option<TripleRecord>, QueryError> {
        Ok(self.txn.get(entity_id, attribute_id)?)
    }

    /// Scan all triples for an entity.
    pub fn scan_entity(&mut self, entity_id: &EntityId) -> Result<Vec<TripleRecord>, QueryError> {
        let mut results = Vec::new();
        let mut iter = self.txn.scan_entity(entity_id)?;

        while let Some(record) = iter.next_record()? {
            // Filter out deleted records
            if !record.is_deleted() {
                results.push(record);
            }
        }

        Ok(results)
    }

    /// Scan all triples in the database.
    ///
    /// Use with caution - this can be expensive for large databases.
    pub fn scan_all(&mut self) -> Result<Vec<TripleRecord>, QueryError> {
        let mut results = Vec::new();
        let mut cursor = self.txn.cursor()?;

        while let Some(record) = cursor.next_record()? {
            if !record.is_deleted() {
                results.push(record);
            }
        }

        Ok(results)
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
