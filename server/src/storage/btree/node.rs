//! B-tree node types and serialization.
//!
//! The B-tree uses 8KB pages with the following node types:
//! - Internal nodes: store keys and child page pointers
//! - Leaf nodes: store key-value pairs, doubly-linked for range scans

#![allow(clippy::cast_possible_truncation)]

use crate::storage::page::{PAGE_SIZE, Page, PageHeader, PageId, PageType};
use crate::types::{AttributeId, EntityId};

/// Size of a key in bytes (`entity_id` + `attribute_id` = 16 + 16).
pub const KEY_SIZE: usize = 32;

/// A 32-byte key for the primary index (`entity_id`, `attribute_id`).
pub type Key = [u8; KEY_SIZE];

/// Node header layout (after page header):
/// - `node_type`: 1 byte (0 = internal, 1 = leaf)
/// - `key_count`: 2 bytes
/// - `parent_page`: 8 bytes
/// - `prev_leaf`: 8 bytes (only for leaf nodes, 0 if none)
/// - `next_leaf`: 8 bytes (only for leaf nodes, 0 if none)
///
/// Total: 27 bytes
const NODE_HEADER_SIZE: usize = 27;

/// Offset where node data starts (after page header + node header).
const DATA_OFFSET: usize = PageHeader::SIZE + NODE_HEADER_SIZE;

/// Available space for node data.
const DATA_SPACE: usize = PAGE_SIZE - DATA_OFFSET;

/// Internal node entry size: key (32 bytes) + child pointer (8 bytes).
const INTERNAL_ENTRY_SIZE: usize = KEY_SIZE + 8;

/// Maximum number of keys in an internal node.
/// We need space for N keys and N+1 child pointers.
/// `DATA_SPACE` = N * `KEY_SIZE` + (N+1) * 8
/// `DATA_SPACE` = N * 32 + N * 8 + 8
/// `DATA_SPACE` - 8 = N * 40
/// N = (`DATA_SPACE` - 8) / 40
pub const MAX_INTERNAL_KEYS: usize = (DATA_SPACE - 8) / INTERNAL_ENTRY_SIZE;

/// Leaf entry overhead: key (32 bytes) + `value_len` (2 bytes).
const LEAF_ENTRY_OVERHEAD: usize = KEY_SIZE + 2;

/// Maximum value size that can be stored inline in a leaf.
/// For larger values, we'd use overflow pages (not implemented in Phase 1).
pub const MAX_INLINE_VALUE_SIZE: usize = 1024;

/// Minimum number of entries in a leaf node (except root).
/// This is approximate since leaf entries are variable-sized.
pub const MIN_LEAF_ENTRIES: usize = 2;

/// Node type discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

impl TryFrom<u8> for NodeType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Internal),
            1 => Ok(Self::Leaf),
            _ => Err(value),
        }
    }
}

/// Header information for a B-tree node.
#[derive(Debug, Clone, Copy)]
pub struct NodeHeader {
    pub node_type: NodeType,
    pub key_count: u16,
    pub parent_page: PageId,
    pub prev_leaf: PageId,
    pub next_leaf: PageId,
}

impl NodeHeader {
    /// Read a node header from a page.
    #[must_use]
    pub fn from_page(page: &Page) -> Option<Self> {
        let offset = PageHeader::SIZE;
        let node_type = NodeType::try_from(page.read_u8(offset)).ok()?;
        let key_count = u16::from_le_bytes([page.read_u8(offset + 1), page.read_u8(offset + 2)]);
        let parent_page = page.read_u64(offset + 3);
        let prev_leaf = page.read_u64(offset + 11);
        let next_leaf = page.read_u64(offset + 19);

        Some(Self {
            node_type,
            key_count,
            parent_page,
            prev_leaf,
            next_leaf,
        })
    }

    /// Write a node header to a page.
    pub fn write_to_page(&self, page: &mut Page) {
        let offset = PageHeader::SIZE;
        page.write_u8(offset, self.node_type as u8);
        page.write_bytes(offset + 1, &self.key_count.to_le_bytes());
        page.write_u64(offset + 3, self.parent_page);
        page.write_u64(offset + 11, self.prev_leaf);
        page.write_u64(offset + 19, self.next_leaf);
    }
}

/// An internal (non-leaf) B-tree node.
///
/// Stores N keys and N+1 child pointers.
/// `Child[i]` contains keys < `Key[i]`
/// `Child[i+1]` contains keys >= `Key[i]`
#[derive(Debug)]
pub struct InternalNode {
    pub header: NodeHeader,
    /// Keys in sorted order.
    pub keys: Vec<Key>,
    /// Child page pointers. `children.len()` == `keys.len()` + 1
    pub children: Vec<PageId>,
}

impl InternalNode {
    /// Create a new empty internal node.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Vec::new() is not const-stable
    pub fn new(parent_page: PageId) -> Self {
        Self {
            header: NodeHeader {
                node_type: NodeType::Internal,
                key_count: 0,
                parent_page,
                prev_leaf: 0,
                next_leaf: 0,
            },
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Create an internal node with initial children.
    #[must_use]
    pub fn with_children(
        parent_page: PageId,
        left_child: PageId,
        key: Key,
        right_child: PageId,
    ) -> Self {
        Self {
            header: NodeHeader {
                node_type: NodeType::Internal,
                key_count: 1,
                parent_page,
                prev_leaf: 0,
                next_leaf: 0,
            },
            keys: vec![key],
            children: vec![left_child, right_child],
        }
    }

    /// Read an internal node from a page.
    pub fn from_page(page: &Page) -> Result<Self, NodeError> {
        let header = NodeHeader::from_page(page).ok_or(NodeError::InvalidHeader)?;
        if header.node_type != NodeType::Internal {
            return Err(NodeError::WrongNodeType);
        }

        let key_count = header.key_count as usize;
        let mut keys = Vec::with_capacity(key_count);
        let mut children = Vec::with_capacity(key_count + 1);

        let mut offset = DATA_OFFSET;

        // Read first child pointer
        children.push(page.read_u64(offset));
        offset += 8;

        // Read key-child pairs
        for _ in 0..key_count {
            let mut key = [0u8; KEY_SIZE];
            key.copy_from_slice(page.read_bytes(offset, KEY_SIZE));
            keys.push(key);
            offset += KEY_SIZE;

            children.push(page.read_u64(offset));
            offset += 8;
        }

        Ok(Self {
            header,
            keys,
            children,
        })
    }

    /// Write an internal node to a page.
    pub fn write_to_page(&self, page: &mut Page) {
        // Write page header
        let page_header = PageHeader {
            page_type: PageType::BTreeInternal,
            flags: 0,
            checksum: 0,
        };
        page.write_bytes(0, &page_header.to_bytes());

        // Update and write node header
        let mut header = self.header;
        header.key_count = self.keys.len() as u16;
        header.write_to_page(page);

        let mut offset = DATA_OFFSET;

        // Write first child pointer
        if !self.children.is_empty() {
            page.write_u64(offset, self.children[0]);
        }
        offset += 8;

        // Write key-child pairs
        for (i, key) in self.keys.iter().enumerate() {
            page.write_bytes(offset, key);
            offset += KEY_SIZE;

            if i + 1 < self.children.len() {
                page.write_u64(offset, self.children[i + 1]);
            }
            offset += 8;
        }
    }

    /// Find the child index for a given key.
    #[must_use]
    pub fn find_child_index(&self, key: &Key) -> usize {
        // Binary search for the first key >= target
        match self.keys.binary_search(key) {
            Ok(i) => i + 1, // Exact match, go right
            Err(i) => i,    // Insert position
        }
    }

    /// Check if the node is full.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Vec::len() is not const-stable
    pub fn is_full(&self) -> bool {
        self.keys.len() >= MAX_INTERNAL_KEYS
    }

    /// Insert a key and right child at the appropriate position.
    pub fn insert(&mut self, key: Key, right_child: PageId) {
        let idx = self.find_child_index(&key);
        self.keys.insert(idx, key);
        self.children.insert(idx + 1, right_child);
    }

    /// Split the node, returning the median key and the new right node.
    #[must_use]
    pub fn split(&mut self) -> (Key, Self) {
        let mid = self.keys.len() / 2;
        let median_key = self.keys[mid];

        // Right node gets keys and children after median
        let right_keys: Vec<Key> = self.keys.drain(mid + 1..).collect();
        let right_children: Vec<PageId> = self.children.drain(mid + 1..).collect();

        // Remove median key from left node
        self.keys.pop();

        let right_node = Self {
            header: NodeHeader {
                node_type: NodeType::Internal,
                key_count: right_keys.len() as u16,
                parent_page: self.header.parent_page,
                prev_leaf: 0,
                next_leaf: 0,
            },
            keys: right_keys,
            children: right_children,
        };

        (median_key, right_node)
    }
}

/// A leaf B-tree node.
///
/// Stores key-value pairs and links to sibling leaves.
#[derive(Debug)]
pub struct LeafNode {
    pub header: NodeHeader,
    /// Entries in sorted order by key.
    pub entries: Vec<LeafEntry>,
}

/// A key-value entry in a leaf node.
#[derive(Debug)]
pub struct LeafEntry {
    pub key: Key,
    pub value: Vec<u8>,
}

impl LeafNode {
    /// Create a new empty leaf node.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Vec::new() is not const-stable
    pub fn new(parent_page: PageId) -> Self {
        Self {
            header: NodeHeader {
                node_type: NodeType::Leaf,
                key_count: 0,
                parent_page,
                prev_leaf: 0,
                next_leaf: 0,
            },
            entries: Vec::new(),
        }
    }

    /// Calculate the serialized size of all entries.
    fn entries_size(&self) -> usize {
        self.entries
            .iter()
            .map(|e| LEAF_ENTRY_OVERHEAD + e.value.len())
            .sum()
    }

    /// Check if a new entry would fit in this node.
    #[must_use]
    pub fn can_fit(&self, value_len: usize) -> bool {
        self.entries_size() + LEAF_ENTRY_OVERHEAD + value_len <= DATA_SPACE
    }

    /// Check if an updated value would fit in this node.
    ///
    /// When updating an existing entry, the new value replaces the old one,
    /// so we need to account for the size difference rather than the full size.
    #[must_use]
    pub fn can_fit_update(&self, old_value_len: usize, new_value_len: usize) -> bool {
        // New size = current size - old value + new value
        let current = self.entries_size();
        if new_value_len <= old_value_len {
            // Shrinking or same size always fits
            true
        } else {
            // Growing: check if the additional space fits
            let additional = new_value_len - old_value_len;
            current + additional <= DATA_SPACE
        }
    }

    /// Check if the node is at minimum capacity.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Vec::len() is not const-stable
    pub fn is_underfull(&self) -> bool {
        self.entries.len() < MIN_LEAF_ENTRIES
    }

    /// Read a leaf node from a page.
    pub fn from_page(page: &Page) -> Result<Self, NodeError> {
        let header = NodeHeader::from_page(page).ok_or(NodeError::InvalidHeader)?;
        if header.node_type != NodeType::Leaf {
            return Err(NodeError::WrongNodeType);
        }

        let entry_count = header.key_count as usize;
        let mut entries = Vec::with_capacity(entry_count);

        let mut offset = DATA_OFFSET;

        for _ in 0..entry_count {
            // Read key
            let mut key = [0u8; KEY_SIZE];
            key.copy_from_slice(page.read_bytes(offset, KEY_SIZE));
            offset += KEY_SIZE;

            // Read value length
            let value_len =
                u16::from_le_bytes([page.read_u8(offset), page.read_u8(offset + 1)]) as usize;
            offset += 2;

            // Read value
            let value = page.read_bytes(offset, value_len).to_vec();
            offset += value_len;

            entries.push(LeafEntry { key, value });
        }

        Ok(Self { header, entries })
    }

    /// Write a leaf node to a page.
    pub fn write_to_page(&self, page: &mut Page) {
        // Write page header
        let page_header = PageHeader {
            page_type: PageType::BTreeLeaf,
            flags: 0,
            checksum: 0,
        };
        page.write_bytes(0, &page_header.to_bytes());

        // Update and write node header
        let mut header = self.header;
        header.key_count = self.entries.len() as u16;
        header.write_to_page(page);

        let mut offset = DATA_OFFSET;

        for entry in &self.entries {
            // Write key
            page.write_bytes(offset, &entry.key);
            offset += KEY_SIZE;

            // Write value length
            let value_len = entry.value.len() as u16;
            page.write_bytes(offset, &value_len.to_le_bytes());
            offset += 2;

            // Write value
            page.write_bytes(offset, &entry.value);
            offset += entry.value.len();
        }
    }

    /// Find the index where a key should be inserted (or exists).
    pub fn find_index(&self, key: &Key) -> Result<usize, usize> {
        self.entries.binary_search_by(|e| e.key.cmp(key))
    }

    /// Get a value by key.
    #[must_use]
    pub fn get(&self, key: &Key) -> Option<&[u8]> {
        self.find_index(key)
            .ok()
            .map(|i| self.entries[i].value.as_slice())
    }

    /// Insert or update an entry.
    ///
    /// Returns the old value if updating, None if inserting.
    pub fn insert(&mut self, key: Key, value: Vec<u8>) -> Option<Vec<u8>> {
        match self.find_index(&key) {
            Ok(i) => {
                // Key exists, update value
                let old = std::mem::replace(&mut self.entries[i].value, value);
                Some(old)
            }
            Err(i) => {
                // Key doesn't exist, insert
                self.entries.insert(i, LeafEntry { key, value });
                None
            }
        }
    }

    /// Remove an entry by key.
    ///
    /// Returns the removed value if found.
    pub fn remove(&mut self, key: &Key) -> Option<Vec<u8>> {
        self.find_index(key)
            .ok()
            .map(|i| self.entries.remove(i).value)
    }

    /// Split the node, returning the split key and the new right node.
    #[must_use]
    pub fn split(&mut self) -> (Key, Self) {
        let mid = self.entries.len() / 2;

        // Right node gets entries from mid onwards
        let right_entries: Vec<LeafEntry> = self.entries.drain(mid..).collect();
        let split_key = right_entries[0].key;

        let right_node = Self {
            header: NodeHeader {
                node_type: NodeType::Leaf,
                key_count: right_entries.len() as u16,
                parent_page: self.header.parent_page,
                prev_leaf: 0, // Will be set by caller
                next_leaf: self.header.next_leaf,
            },
            entries: right_entries,
        };

        (split_key, right_node)
    }
}

/// Errors that can occur when working with B-tree nodes.
#[derive(Debug)]
pub enum NodeError {
    /// Invalid node header.
    InvalidHeader,
    /// Wrong node type for operation.
    WrongNodeType,
    /// Value too large to store inline.
    ValueTooLarge(usize),
    /// Node is full.
    NodeFull,
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidHeader => write!(f, "invalid node header"),
            Self::WrongNodeType => write!(f, "wrong node type for operation"),
            Self::ValueTooLarge(size) => {
                write!(
                    f,
                    "value too large: {size} bytes (max {MAX_INLINE_VALUE_SIZE})"
                )
            }
            Self::NodeFull => write!(f, "node is full"),
        }
    }
}

impl std::error::Error for NodeError {}

/// Compare two keys.
#[must_use]
pub fn compare_keys(a: &Key, b: &Key) -> std::cmp::Ordering {
    a.cmp(b)
}

/// Create a key from `entity_id` and `attribute_id`.
#[must_use]
pub fn make_key(entity_id: &EntityId, attribute_id: &AttributeId) -> Key {
    let mut key = [0u8; KEY_SIZE];
    key[..16].copy_from_slice(&entity_id.0);
    key[16..].copy_from_slice(&attribute_id.0);
    key
}

/// Extract `entity_id` and `attribute_id` from a key.
#[must_use]
pub fn split_key(key: &Key) -> (EntityId, AttributeId) {
    let mut entity_bytes = [0u8; 16];
    let mut attribute_bytes = [0u8; 16];
    entity_bytes.copy_from_slice(&key[..16]);
    attribute_bytes.copy_from_slice(&key[16..]);
    (EntityId(entity_bytes), AttributeId(attribute_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_pool::BufferPool;

    #[test]
    fn test_key_operations() {
        let entity_id = EntityId([1u8; 16]);
        let attribute_id = AttributeId([2u8; 16]);

        let key = make_key(&entity_id, &attribute_id);
        let (e, a) = split_key(&key);

        assert_eq!(e, entity_id);
        assert_eq!(a, attribute_id);
    }

    #[test]
    fn test_internal_node_roundtrip() {
        let pool = BufferPool::new(10);
        let mut node = InternalNode::new(0);
        node.keys = vec![[1u8; KEY_SIZE], [2u8; KEY_SIZE], [3u8; KEY_SIZE]];
        node.children = vec![10, 20, 30, 40];

        let mut page = pool.lease_page_zeroed().expect("should lease");
        node.write_to_page(&mut page);

        let restored = InternalNode::from_page(&page).expect("should parse");
        assert_eq!(restored.keys.len(), 3);
        assert_eq!(restored.children.len(), 4);
        assert_eq!(restored.keys[0], [1u8; KEY_SIZE]);
        assert_eq!(restored.children[0], 10);
        assert_eq!(restored.children[3], 40);
    }

    #[test]
    fn test_leaf_node_roundtrip() {
        let pool = BufferPool::new(10);
        let mut node = LeafNode::new(0);
        node.insert([1u8; KEY_SIZE], b"value1".to_vec());
        node.insert([2u8; KEY_SIZE], b"value2".to_vec());
        node.insert([3u8; KEY_SIZE], b"value3".to_vec());

        let mut page = pool.lease_page_zeroed().expect("should lease");
        node.write_to_page(&mut page);

        let restored = LeafNode::from_page(&page).expect("should parse");
        assert_eq!(restored.entries.len(), 3);
        assert_eq!(restored.get(&[1u8; KEY_SIZE]), Some(b"value1".as_slice()));
        assert_eq!(restored.get(&[2u8; KEY_SIZE]), Some(b"value2".as_slice()));
        assert_eq!(restored.get(&[3u8; KEY_SIZE]), Some(b"value3".as_slice()));
    }

    #[test]
    fn test_leaf_node_insert_update() {
        let mut node = LeafNode::new(0);

        // Insert new key
        let old = node.insert([1u8; KEY_SIZE], b"value1".to_vec());
        assert!(old.is_none());
        assert_eq!(node.entries.len(), 1);

        // Update existing key
        let old = node.insert([1u8; KEY_SIZE], b"updated".to_vec());
        assert_eq!(old, Some(b"value1".to_vec()));
        assert_eq!(node.entries.len(), 1);
        assert_eq!(node.get(&[1u8; KEY_SIZE]), Some(b"updated".as_slice()));
    }

    #[test]
    fn test_leaf_node_remove() {
        let mut node = LeafNode::new(0);
        node.insert([1u8; KEY_SIZE], b"value1".to_vec());
        node.insert([2u8; KEY_SIZE], b"value2".to_vec());

        let removed = node.remove(&[1u8; KEY_SIZE]);
        assert_eq!(removed, Some(b"value1".to_vec()));
        assert_eq!(node.entries.len(), 1);
        assert!(node.get(&[1u8; KEY_SIZE]).is_none());
    }

    #[test]
    fn test_internal_node_find_child() {
        let mut node = InternalNode::new(0);
        node.keys = vec![[10u8; KEY_SIZE], [20u8; KEY_SIZE], [30u8; KEY_SIZE]];
        node.children = vec![100, 200, 300, 400];

        // Key less than first key -> first child
        assert_eq!(node.find_child_index(&[5u8; KEY_SIZE]), 0);

        // Key equal to first key -> second child
        assert_eq!(node.find_child_index(&[10u8; KEY_SIZE]), 1);

        // Key between first and second -> second child
        assert_eq!(node.find_child_index(&[15u8; KEY_SIZE]), 1);

        // Key greater than all -> last child
        assert_eq!(node.find_child_index(&[35u8; KEY_SIZE]), 3);
    }

    #[test]
    fn test_leaf_node_split() {
        let mut node = LeafNode::new(0);
        for i in 0..10u8 {
            let mut key = [0u8; KEY_SIZE];
            key[0] = i;
            node.insert(key, vec![i; 10]);
        }

        let (split_key, right) = node.split();

        // Left node should have first half
        assert_eq!(node.entries.len(), 5);
        // Right node should have second half
        assert_eq!(right.entries.len(), 5);
        // Split key should be first key of right node
        assert_eq!(split_key, right.entries[0].key);
    }

    #[test]
    fn test_internal_node_split() {
        let mut node = InternalNode::new(0);
        for i in 0..10u8 {
            let mut key = [0u8; KEY_SIZE];
            key[0] = i;
            node.keys.push(key);
            node.children.push(PageId::from(i));
        }
        node.children.push(10); // N+1 children

        let original_len = node.keys.len();
        let (median_key, right) = node.split();

        // Left should have keys before median
        // Right should have keys after median
        // Median key is promoted
        assert!(node.keys.len() < original_len);
        assert!(!right.keys.is_empty());
        assert!(median_key[0] > node.keys.last().map_or(0, |k| k[0]));
        assert!(median_key[0] < right.keys.first().map_or(255, |k| k[0]));
    }
}
