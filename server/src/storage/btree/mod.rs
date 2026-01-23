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
//! ```ignore
//! use storage::btree::{BTree, make_key};
//!
//! let mut tree = BTree::new(&mut file, 0)?; // 0 = create new tree
//!
//! // Insert
//! let key = make_key(&entity_id, &attribute_id);
//! tree.insert(key, value)?;
//!
//! // Lookup
//! if let Some(value) = tree.get(&key)? {
//!     // ...
//! }
//!
//! // Iterate
//! let mut iter = tree.iter()?;
//! while let Some((key, value)) = iter.next_entry()? {
//!     // ...
//! }
//! ```

mod node;
mod tree;

pub use node::{
    InternalNode, KEY_SIZE, Key, LeafEntry, LeafNode, MAX_INLINE_VALUE_SIZE, NodeError, NodeHeader,
    NodeType, compare_keys, make_key, split_key,
};
pub use tree::{BTree, BTreeError, BTreeIterator};
