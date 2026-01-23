//! B-tree implementation for the primary index.
//!
//! This is a disk-based B-tree that stores key-value pairs where:
//! - Key: (`entity_id`, `attribute_id`) = 32 bytes
//! - Value: serialized triple value (variable length)

#![allow(clippy::cast_possible_truncation)]

use crate::storage::btree::node::{
    InternalNode, Key, LeafEntry, LeafNode, NodeError, NodeHeader, NodeType,
    MAX_INLINE_VALUE_SIZE,
};
use crate::storage::file::{DatabaseFile, FileError};
use crate::storage::page::{Page, PageId};

/// A B-tree backed by a database file.
pub struct BTree<'a> {
    file: &'a mut DatabaseFile,
    root_page: PageId,
}

impl<'a> BTree<'a> {
    /// Create a new B-tree with the given root page.
    ///
    /// If `root_page` is 0, creates a new empty tree.
    pub fn new(file: &'a mut DatabaseFile, root_page: PageId) -> Result<Self, BTreeError> {
        if root_page == 0 {
            // Create a new empty tree with a leaf root
            let leaf = LeafNode::new(0);
            let page_id = file.allocate_pages(1)?;

            let mut page = Page::new();
            leaf.write_to_page(&mut page);
            file.write_page(page_id, &page)?;

            Ok(Self {
                file,
                root_page: page_id,
            })
        } else {
            Ok(Self { file, root_page })
        }
    }

    /// Get the root page ID.
    #[must_use]
    pub const fn root_page(&self) -> PageId {
        self.root_page
    }

    /// Look up a value by key.
    pub fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, BTreeError> {
        let leaf_page_id = self.find_leaf(key)?;
        let page = self.file.read_page(leaf_page_id)?;
        let leaf = LeafNode::from_page(&page)?;

        Ok(leaf.get(key).map(<[_]>::to_vec))
    }

    /// Insert or update a key-value pair.
    ///
    /// Returns the old value if updating, None if inserting.
    pub fn insert(&mut self, key: Key, value: Vec<u8>) -> Result<Option<Vec<u8>>, BTreeError> {
        if value.len() > MAX_INLINE_VALUE_SIZE {
            return Err(BTreeError::Node(NodeError::ValueTooLarge(value.len())));
        }

        // Find the leaf node
        let leaf_page_id = self.find_leaf(&key)?;
        let page = self.file.read_page(leaf_page_id)?;
        let mut leaf = LeafNode::from_page(&page)?;

        // Check if we can fit the new entry
        if !leaf.can_fit(value.len()) && leaf.get(&key).is_none() {
            // Need to split before inserting
            return self.insert_with_split(leaf_page_id, leaf, key, value);
        }

        // Insert into the leaf
        let old_value = leaf.insert(key, value);

        // Write back
        let mut page = Page::new();
        leaf.write_to_page(&mut page);
        self.file.write_page(leaf_page_id, &page)?;

        Ok(old_value)
    }

    /// Remove a key-value pair.
    ///
    /// Returns the removed value if found.
    pub fn remove(&mut self, key: &Key) -> Result<Option<Vec<u8>>, BTreeError> {
        let leaf_page_id = self.find_leaf(key)?;
        let page = self.file.read_page(leaf_page_id)?;
        let mut leaf = LeafNode::from_page(&page)?;

        let old_value = leaf.remove(key);

        if old_value.is_some() {
            // Write back the modified leaf
            let mut page = Page::new();
            leaf.write_to_page(&mut page);
            self.file.write_page(leaf_page_id, &page)?;

            // Note: We don't handle underflow/merging in Phase 1
            // This is acceptable for append-heavy workloads
        }

        Ok(old_value)
    }

    /// Find the leaf page that should contain the given key.
    fn find_leaf(&mut self, key: &Key) -> Result<PageId, BTreeError> {
        let mut current_page_id = self.root_page;

        loop {
            let page = self.file.read_page(current_page_id)?;
            let header = NodeHeader::from_page(&page).ok_or(BTreeError::Node(NodeError::InvalidHeader))?;

            match header.node_type {
                NodeType::Leaf => return Ok(current_page_id),
                NodeType::Internal => {
                    let node = InternalNode::from_page(&page)?;
                    let child_idx = node.find_child_index(key);
                    current_page_id = node.children[child_idx];
                }
            }
        }
    }

    /// Insert with node splitting.
    fn insert_with_split(
        &mut self,
        leaf_page_id: PageId,
        mut leaf: LeafNode,
        key: Key,
        value: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, BTreeError> {
        // Insert into the leaf first (it may overflow temporarily)
        let old_value = leaf.insert(key, value);

        // Split the leaf
        let (split_key, mut right_leaf) = leaf.split();

        // Allocate page for right leaf
        let right_page_id = self.file.allocate_pages(1)?;

        // Update sibling pointers
        right_leaf.header.prev_leaf = leaf_page_id;
        right_leaf.header.next_leaf = leaf.header.next_leaf;
        leaf.header.next_leaf = right_page_id;

        // Write both leaves
        let mut left_page = Page::new();
        leaf.write_to_page(&mut left_page);
        self.file.write_page(leaf_page_id, &left_page)?;

        let mut right_page = Page::new();
        right_leaf.write_to_page(&mut right_page);
        self.file.write_page(right_page_id, &right_page)?;

        // Update next leaf's prev pointer if it exists
        if right_leaf.header.next_leaf != 0 {
            let next_page = self.file.read_page(right_leaf.header.next_leaf)?;
            let mut next_leaf = LeafNode::from_page(&next_page)?;
            next_leaf.header.prev_leaf = right_page_id;
            let mut next_page = Page::new();
            next_leaf.write_to_page(&mut next_page);
            self.file.write_page(right_leaf.header.next_leaf, &next_page)?;
        }

        // Propagate the split up the tree
        self.insert_into_parent(leaf_page_id, split_key, right_page_id, leaf.header.parent_page)?;

        Ok(old_value)
    }

    /// Insert a new key into a parent node after a child split.
    fn insert_into_parent(
        &mut self,
        left_child: PageId,
        key: Key,
        right_child: PageId,
        parent_page_id: PageId,
    ) -> Result<(), BTreeError> {
        if parent_page_id == 0 {
            // No parent - need to create a new root
            return self.create_new_root(left_child, key, right_child);
        }

        let page = self.file.read_page(parent_page_id)?;
        let mut parent = InternalNode::from_page(&page)?;

        if parent.is_full() {
            // Need to split the internal node
            parent.insert(key, right_child);
            let (median_key, right_parent) = parent.split();

            let right_parent_page_id = self.file.allocate_pages(1)?;

            // Update children's parent pointers in the right node
            for &child_id in &right_parent.children {
                self.update_parent_pointer(child_id, right_parent_page_id)?;
            }

            // Write both internal nodes
            let mut left_page = Page::new();
            parent.write_to_page(&mut left_page);
            self.file.write_page(parent_page_id, &left_page)?;

            let mut right_page = Page::new();
            right_parent.write_to_page(&mut right_page);
            self.file.write_page(right_parent_page_id, &right_page)?;

            // Recursively insert into grandparent
            self.insert_into_parent(
                parent_page_id,
                median_key,
                right_parent_page_id,
                parent.header.parent_page,
            )?;
        } else {
            // Parent has room
            parent.insert(key, right_child);

            // Update right child's parent pointer
            self.update_parent_pointer(right_child, parent_page_id)?;

            let mut page = Page::new();
            parent.write_to_page(&mut page);
            self.file.write_page(parent_page_id, &page)?;
        }

        Ok(())
    }

    /// Create a new root node after the old root splits.
    fn create_new_root(
        &mut self,
        left_child: PageId,
        key: Key,
        right_child: PageId,
    ) -> Result<(), BTreeError> {
        let new_root = InternalNode::with_children(0, left_child, key, right_child);
        let new_root_page_id = self.file.allocate_pages(1)?;

        let mut page = Page::new();
        new_root.write_to_page(&mut page);
        self.file.write_page(new_root_page_id, &page)?;

        // Update children's parent pointers
        self.update_parent_pointer(left_child, new_root_page_id)?;
        self.update_parent_pointer(right_child, new_root_page_id)?;

        // Update our root
        self.root_page = new_root_page_id;

        Ok(())
    }

    /// Update a node's parent pointer.
    fn update_parent_pointer(&mut self, page_id: PageId, new_parent: PageId) -> Result<(), BTreeError> {
        let page = self.file.read_page(page_id)?;
        let header = NodeHeader::from_page(&page).ok_or(BTreeError::Node(NodeError::InvalidHeader))?;

        match header.node_type {
            NodeType::Leaf => {
                let mut node = LeafNode::from_page(&page)?;
                node.header.parent_page = new_parent;
                let mut page = Page::new();
                node.write_to_page(&mut page);
                self.file.write_page(page_id, &page)?;
            }
            NodeType::Internal => {
                let mut node = InternalNode::from_page(&page)?;
                node.header.parent_page = new_parent;
                let mut page = Page::new();
                node.write_to_page(&mut page);
                self.file.write_page(page_id, &page)?;
            }
        }

        Ok(())
    }

    /// Create a cursor over all entries in key order.
    pub fn cursor(&mut self) -> Result<BTreeIterator<'_>, BTreeError> {
        // Find the leftmost leaf
        let mut current_page_id = self.root_page;

        loop {
            let page = self.file.read_page(current_page_id)?;
            let header = NodeHeader::from_page(&page).ok_or(BTreeError::Node(NodeError::InvalidHeader))?;

            match header.node_type {
                NodeType::Leaf => {
                    return Ok(BTreeIterator {
                        file: self.file,
                        current_page_id,
                        current_index: 0,
                        current_entries: None,
                    });
                }
                NodeType::Internal => {
                    let node = InternalNode::from_page(&page)?;
                    current_page_id = node.children[0];
                }
            }
        }
    }

    /// Create an iterator starting from a given key.
    pub fn iter_from(&mut self, start_key: &Key) -> Result<BTreeIterator<'_>, BTreeError> {
        let leaf_page_id = self.find_leaf(start_key)?;
        let page = self.file.read_page(leaf_page_id)?;
        let leaf = LeafNode::from_page(&page)?;

        // Find the starting index
        let start_index = leaf.find_index(start_key).unwrap_or_else(|i| i);

        Ok(BTreeIterator {
            file: self.file,
            current_page_id: leaf_page_id,
            current_index: start_index,
            current_entries: Some(leaf.entries),
        })
    }

    /// Count the total number of entries in the tree.
    pub fn count(&mut self) -> Result<usize, BTreeError> {
        let mut count = 0;
        let mut current_page_id = self.root_page;

        // Find leftmost leaf
        loop {
            let page = self.file.read_page(current_page_id)?;
            let header = NodeHeader::from_page(&page).ok_or(BTreeError::Node(NodeError::InvalidHeader))?;

            match header.node_type {
                NodeType::Leaf => break,
                NodeType::Internal => {
                    let node = InternalNode::from_page(&page)?;
                    current_page_id = node.children[0];
                }
            }
        }

        // Scan all leaves
        loop {
            let page = self.file.read_page(current_page_id)?;
            let leaf = LeafNode::from_page(&page)?;
            count += leaf.entries.len();

            if leaf.header.next_leaf == 0 {
                break;
            }
            current_page_id = leaf.header.next_leaf;
        }

        Ok(count)
    }
}

/// Iterator over B-tree entries.
pub struct BTreeIterator<'a> {
    file: &'a mut DatabaseFile,
    current_page_id: PageId,
    current_index: usize,
    current_entries: Option<Vec<LeafEntry>>,
}

impl BTreeIterator<'_> {
    /// Get the next entry.
    pub fn next_entry(&mut self) -> Result<Option<(Key, Vec<u8>)>, BTreeError> {
        loop {
            // Load current page entries if needed
            if self.current_entries.is_none() {
                if self.current_page_id == 0 {
                    return Ok(None);
                }

                let page = self.file.read_page(self.current_page_id)?;
                let leaf = LeafNode::from_page(&page)?;
                self.current_entries = Some(leaf.entries);
            }

            let Some(entries) = &self.current_entries else {
                return Ok(None);
            };

            if self.current_index < entries.len() {
                let entry = &entries[self.current_index];
                let result = (entry.key, Vec::from(entry.value.as_slice()));
                self.current_index += 1;
                return Ok(Some(result));
            }

            // Move to next leaf
            let page = self.file.read_page(self.current_page_id)?;
            let leaf = LeafNode::from_page(&page)?;

            if leaf.header.next_leaf == 0 {
                return Ok(None);
            }

            self.current_page_id = leaf.header.next_leaf;
            self.current_index = 0;
            self.current_entries = None;
        }
    }
}

/// Errors that can occur during B-tree operations.
#[derive(Debug)]
pub enum BTreeError {
    /// File I/O error.
    File(FileError),
    /// Node error.
    Node(NodeError),
}

impl std::fmt::Display for BTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(e) => write!(f, "file error: {e}"),
            Self::Node(e) => write!(f, "node error: {e}"),
        }
    }
}

impl std::error::Error for BTreeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(e) => Some(e),
            Self::Node(e) => Some(e),
        }
    }
}

impl From<FileError> for BTreeError {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

impl From<NodeError> for BTreeError {
    fn from(e: NodeError) -> Self {
        Self::Node(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::btree::node::make_key;
    use crate::storage::file::DatabaseFile;
    use tempfile::tempdir;

    fn create_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_btree_basic_operations() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut tree = BTree::new(&mut file, 0).expect("create tree");

        // Insert some entries
        let key1 = make_key(&[1u8; 16], &[1u8; 16]);
        let key2 = make_key(&[2u8; 16], &[2u8; 16]);
        let key3 = make_key(&[3u8; 16], &[3u8; 16]);

        tree.insert(key1, b"value1".to_vec()).expect("insert 1");
        tree.insert(key2, b"value2".to_vec()).expect("insert 2");
        tree.insert(key3, b"value3".to_vec()).expect("insert 3");

        // Look up entries
        assert_eq!(tree.get(&key1).expect("get 1"), Some(b"value1".to_vec()));
        assert_eq!(tree.get(&key2).expect("get 2"), Some(b"value2".to_vec()));
        assert_eq!(tree.get(&key3).expect("get 3"), Some(b"value3".to_vec()));

        // Look up non-existent key
        let key4 = make_key(&[4u8; 16], &[4u8; 16]);
        assert_eq!(tree.get(&key4).expect("get 4"), None);
    }

    #[test]
    fn test_btree_update() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut tree = BTree::new(&mut file, 0).expect("create tree");

        let key = make_key(&[1u8; 16], &[1u8; 16]);

        // Insert
        let old = tree.insert(key, b"original".to_vec()).expect("insert");
        assert!(old.is_none());

        // Update
        let old = tree.insert(key, b"updated".to_vec()).expect("update");
        assert_eq!(old, Some(b"original".to_vec()));

        // Verify update
        assert_eq!(tree.get(&key).expect("get"), Some(b"updated".to_vec()));
    }

    #[test]
    fn test_btree_remove() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut tree = BTree::new(&mut file, 0).expect("create tree");

        let key = make_key(&[1u8; 16], &[1u8; 16]);

        tree.insert(key, b"value".to_vec()).expect("insert");
        assert!(tree.get(&key).expect("get").is_some());

        let removed = tree.remove(&key).expect("remove");
        assert_eq!(removed, Some(b"value".to_vec()));

        assert!(tree.get(&key).expect("get after remove").is_none());
    }

    #[test]
    fn test_btree_iteration() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut tree = BTree::new(&mut file, 0).expect("create tree");

        // Insert entries in non-sorted order
        for i in [5u8, 3, 7, 1, 9, 2, 8, 4, 6, 0] {
            let key = make_key(&[i; 16], &[0u8; 16]);
            tree.insert(key, vec![i]).expect("insert");
        }

        // Iterate and verify sorted order
        let mut iter = tree.cursor().expect("cursor");
        let mut prev: Option<u8> = None;

        while let Some((key, value)) = iter.next_entry().expect("next") {
            let current = key[0];
            if let Some(p) = prev {
                assert!(current > p, "entries should be in sorted order");
            }
            assert_eq!(value, vec![current]);
            prev = Some(current);
        }

        assert_eq!(prev, Some(9)); // Should have seen all entries
    }

    #[test]
    fn test_btree_many_inserts() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut tree = BTree::new(&mut file, 0).expect("create tree");

        // Insert enough entries to cause splits
        let n = 500;
        for i in 0..n {
            let mut entity_id = [0u8; 16];
            entity_id[0..2].copy_from_slice(&(i as u16).to_be_bytes());
            let key = make_key(&entity_id, &[0u8; 16]);
            let value = format!("value_{i}").into_bytes();
            tree.insert(key, value).expect("insert");
        }

        // Verify count
        let count = tree.count().expect("count");
        assert_eq!(count, n);

        // Verify all entries can be retrieved
        for i in 0..n {
            let mut entity_id = [0u8; 16];
            entity_id[0..2].copy_from_slice(&(i as u16).to_be_bytes());
            let key = make_key(&entity_id, &[0u8; 16]);
            let expected = format!("value_{i}").into_bytes();
            let actual = tree.get(&key).expect("get");
            assert_eq!(actual, Some(expected), "mismatch at {i}");
        }
    }

    #[test]
    fn test_btree_iter_from() {
        let (_dir, path) = create_test_db();
        let mut file = DatabaseFile::create(&path).expect("create db");

        let mut tree = BTree::new(&mut file, 0).expect("create tree");

        // Insert 10 entries
        for i in 0..10u8 {
            let key = make_key(&[i; 16], &[0u8; 16]);
            tree.insert(key, vec![i]).expect("insert");
        }

        // Iterate from key 5
        let start_key = make_key(&[5u8; 16], &[0u8; 16]);
        let mut iter = tree.iter_from(&start_key).expect("iter_from");

        let mut values = Vec::new();
        while let Some((_, value)) = iter.next_entry().expect("next") {
            values.push(value[0]);
        }

        assert_eq!(values, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_btree_persistence() {
        let (_dir, path) = create_test_db();

        let root_page;

        // Create and populate tree
        {
            let mut file = DatabaseFile::create(&path).expect("create db");
            let mut tree = BTree::new(&mut file, 0).expect("create tree");

            for i in 0..100u8 {
                let key = make_key(&[i; 16], &[0u8; 16]);
                tree.insert(key, vec![i]).expect("insert");
            }

            root_page = tree.root_page();

            // Persist superblock with root page
            file.superblock_mut().primary_index_root = root_page;
            file.write_superblock().expect("write superblock");
            file.sync().expect("sync");
        }

        // Reopen and verify
        {
            let mut file = DatabaseFile::open(&path).expect("open db");
            let stored_root = file.superblock().primary_index_root;
            assert_eq!(stored_root, root_page);

            let mut tree = BTree::new(&mut file, stored_root).expect("open tree");

            // Verify entries
            for i in 0..100u8 {
                let key = make_key(&[i; 16], &[0u8; 16]);
                let value = tree.get(&key).expect("get");
                assert_eq!(value, Some(vec![i]));
            }
        }
    }
}
