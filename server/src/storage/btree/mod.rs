//! B-tree implementation for the storage engine.
//!
//! This module provides a disk-based B-tree that is used for the primary index.
//!
//! # Structure
//!
//! The B-tree consists of:
//! - Internal nodes: store keys and child page pointers
//! - Leaf nodes: store key-value pairs, doubly-linked for efficient range scans
//!
//! # Key Format
//!
//! Keys are 32 bytes: `(entity_id: [u8; 16], attribute_id: [u8; 16])`
//!
//! # Usage
//!
//! ```
//! use server::storage::btree::{make_key, KEY_SIZE};
//! use server::types::{EntityId, AttributeId};
//!
//! // Keys are 32 bytes: entity_id (16) + attribute_id (16)
//! let entity_id = EntityId([1u8; 16]);
//! let attribute_id = AttributeId([2u8; 16]);
//! let key = make_key(&entity_id, &attribute_id);
//!
//! assert_eq!(key.len(), KEY_SIZE);
//! assert_eq!(&key[..16], &entity_id.0);
//! assert_eq!(&key[16..], &attribute_id.0);
//! ```

mod node;
mod tree;

pub use node::{
    InternalNode, KEY_SIZE, Key, LeafEntry, LeafNode, MAX_INLINE_VALUE_SIZE, NodeError, NodeHeader,
    NodeType, compare_keys, make_key, split_key,
};
pub use tree::{BTree, BTreeError, BTreeIterator};
#[cfg(unix)]
pub use tree::{BTreeReader, BTreeReaderIterator};
