//! High-level database interface.
//!
//! Provides a clean API for opening, creating, and managing databases.

use std::path::Path;

use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::transaction::{Transaction, TransactionError};

/// A database instance.
///
/// This is the main entry point for working with the storage engine.
/// It owns the underlying database file and provides methods for
/// creating transactions.
pub struct Database {
    file: DatabaseFile,
}

impl Database {
    /// Create a new database at the given path.
    ///
    /// The path must not already exist.
    pub fn create(path: &Path) -> Result<Self, DatabaseError> {
        let file = DatabaseFile::create(path)?;
        Ok(Self { file })
    }

    /// Open an existing database at the given path.
    pub fn open(path: &Path) -> Result<Self, DatabaseError> {
        let file = DatabaseFile::open(path)?;
        Ok(Self { file })
    }

    /// Open an existing database or create a new one if it doesn't exist.
    pub fn open_or_create(path: &Path) -> Result<Self, DatabaseError> {
        if path.exists() {
            Self::open(path)
        } else {
            Self::create(path)
        }
    }

    /// Begin a new transaction.
    ///
    /// In Phase 1, only one transaction can be active at a time.
    pub fn begin(&mut self) -> Result<Transaction<'_>, DatabaseError> {
        Ok(Transaction::begin(&mut self.file)?)
    }
}

/// Errors that can occur during database operations.
#[derive(Debug)]
pub enum DatabaseError {
    /// File I/O error.
    File(FileError),
    /// Transaction error.
    Transaction(TransactionError),
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(e) => write!(f, "file error: {e}"),
            Self::Transaction(e) => write!(f, "transaction error: {e}"),
        }
    }
}

impl std::error::Error for DatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(e) => Some(e),
            Self::Transaction(e) => Some(e),
        }
    }
}

impl From<FileError> for DatabaseError {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

impl From<TransactionError> for DatabaseError {
    fn from(e: TransactionError) -> Self {
        Self::Transaction(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TripleValue;
    use tempfile::tempdir;

    #[test]
    fn test_database_create_and_open() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        // Create database
        {
            let mut db = Database::create(&path).expect("create db");
            let mut txn = db.begin().expect("begin txn");

            let entity_id = [1u8; 16];
            let attribute_id = [2u8; 16];
            txn.insert(
                entity_id,
                attribute_id,
                TripleValue::String("hello".to_string()),
            )
            .expect("insert");
            txn.commit().expect("commit");
        }

        // Reopen and verify
        {
            let mut db = Database::open(&path).expect("open db");
            let mut txn = db.begin().expect("begin txn");

            let entity_id = [1u8; 16];
            let attribute_id = [2u8; 16];
            let record = txn.get(&entity_id, &attribute_id).expect("get");
            assert!(record.is_some());
            assert_eq!(
                record.unwrap().value,
                TripleValue::String("hello".to_string())
            );
            txn.abort();
        }
    }

    #[test]
    fn test_database_open_or_create() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");

        // First call creates
        {
            let db = Database::open_or_create(&path).expect("open_or_create");
            drop(db);
        }

        // Second call opens
        {
            let _db = Database::open_or_create(&path).expect("open_or_create again");
        }
    }
}
