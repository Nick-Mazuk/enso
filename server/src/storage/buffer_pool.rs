//! Buffer pool for pre-allocated page buffers.
//!
//! The buffer pool reduces memory allocation overhead by maintaining a fixed
//! pool of 8KB page buffers that are leased out and returned automatically.
//!
//! # Design
//!
//! - Pre-allocates all buffers at construction time
//! - Uses a free list (Vec) for O(1) lease/return
//! - Returns buffers automatically via RAII (Drop trait on Page)
//! - Thread-safe: uses Mutex for internal synchronization
//!
//! # Invariants
//!
//! - Pool capacity is fixed after construction
//! - All returned buffers must have come from this pool (enforced by type system)
//! - Free list size + leased count == capacity

use std::sync::{Arc, Mutex};

use crate::storage::page::{PAGE_SIZE, Page};

/// Default buffer pool capacity in pages (262,144 pages = 2GB).
/// This is sized for a shared pool across all open databases.
pub const DEFAULT_POOL_CAPACITY: usize = 262_144;

/// A buffer pool that pre-allocates page buffers.
///
/// # Pre-conditions
/// - `capacity` must be > 0 when creating
///
/// # Post-conditions
/// - All buffers are zeroed on creation
/// - Free list contains `capacity` buffers
///
/// # Invariants
/// - `free_list.len() + leased_count == capacity`
pub struct BufferPool {
    /// Free buffers available for leasing.
    /// Invariant: all buffers are `PAGE_SIZE` bytes.
    free_list: Mutex<Vec<Box<[u8; PAGE_SIZE]>>>,
    /// Total capacity (for assertions).
    capacity: usize,
}

impl BufferPool {
    /// Create a new buffer pool with the given capacity.
    ///
    /// # Pre-conditions
    /// - `capacity` > 0
    ///
    /// # Post-conditions
    /// - Pool contains `capacity` zeroed buffers
    /// - All buffers are ready for use
    ///
    /// # Panics
    /// Panics if capacity is 0.
    #[must_use]
    pub fn new(capacity: usize) -> Arc<Self> {
        assert!(capacity > 0, "Buffer pool capacity must be positive");

        let mut free_list = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            free_list.push(Box::new([0u8; PAGE_SIZE]));
        }

        Arc::new(Self {
            free_list: Mutex::new(free_list),
            capacity,
        })
    }

    /// Lease a buffer from the pool.
    ///
    /// # Returns
    /// - `Some(Box<[u8; PAGE_SIZE]>)` if a buffer is available
    /// - `None` if the pool is exhausted
    ///
    /// # Post-conditions
    /// - If Some, `free_list.len()` decreased by 1
    /// - Buffer contents are undefined (may contain stale data)
    #[allow(clippy::expect_used)] // Mutex poisoning indicates unrecoverable state
    pub fn lease(&self) -> Option<Box<[u8; PAGE_SIZE]>> {
        self.free_list.lock().expect("lock poisoned").pop()
    }

    /// Lease a zeroed buffer from the pool.
    ///
    /// # Returns
    /// - `Some(Box<[u8; PAGE_SIZE]>)` if a buffer is available (zeroed)
    /// - `None` if the pool is exhausted
    ///
    /// # Post-conditions
    /// - If Some, `free_list.len()` decreased by 1
    /// - Buffer contents are all zeros
    pub fn lease_zeroed(&self) -> Option<Box<[u8; PAGE_SIZE]>> {
        let mut buffer = self.lease()?;
        buffer.fill(0);
        Some(buffer)
    }

    /// Lease a page from the pool.
    ///
    /// # Returns
    /// - `Some(Page)` if a buffer is available
    /// - `None` if the pool is exhausted
    ///
    /// # Post-conditions
    /// - If Some, `free_list.len()` decreased by 1
    /// - Page contents are undefined (may contain stale data)
    #[allow(clippy::disallowed_methods)] // Arc::clone is required for shared ownership
    pub fn lease_page(self: &Arc<Self>) -> Option<Page> {
        let buffer = self.lease()?;
        Some(Page::from_pool(buffer, Arc::clone(self)))
    }

    /// Lease a zeroed page from the pool.
    ///
    /// # Returns
    /// - `Some(Page)` if a buffer is available (zeroed)
    /// - `None` if the pool is exhausted
    ///
    /// # Post-conditions
    /// - If Some, `free_list.len()` decreased by 1
    /// - Page contents are all zeros
    #[allow(clippy::disallowed_methods)] // Arc::clone is required for shared ownership
    pub fn lease_page_zeroed(self: &Arc<Self>) -> Option<Page> {
        let buffer = self.lease_zeroed()?;
        Some(Page::from_pool(buffer, Arc::clone(self)))
    }

    /// Return a buffer to the pool.
    ///
    /// # Pre-conditions
    /// - Buffer must have come from this pool (enforced by type system)
    ///
    /// # Post-conditions
    /// - `free_list.len()` increased by 1
    ///
    /// # Panics
    /// Panics if returning would exceed capacity (indicates a bug).
    #[allow(clippy::expect_used)] // Mutex poisoning indicates unrecoverable state
    pub fn return_buffer(&self, buffer: Box<[u8; PAGE_SIZE]>) {
        let mut free_list = self.free_list.lock().expect("lock poisoned");
        // Invariant check: we should never exceed capacity
        assert!(
            free_list.len() < self.capacity,
            "Buffer pool overflow: returning buffer to full pool"
        );
        free_list.push(buffer);
    }

    /// Get the number of available buffers.
    #[must_use]
    #[allow(clippy::expect_used)] // Mutex poisoning indicates unrecoverable state
    pub fn available(&self) -> usize {
        self.free_list.lock().expect("lock poisoned").len()
    }

    /// Get the total capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let pool = BufferPool::new(10);
        assert_eq!(pool.capacity(), 10);
        assert_eq!(pool.available(), 10);
    }

    #[test]
    fn test_lease_and_return() {
        let pool = BufferPool::new(2);

        // Lease both buffers
        let buf1 = pool.lease();
        assert!(buf1.is_some());
        assert_eq!(pool.available(), 1);

        let buf2 = pool.lease();
        assert!(buf2.is_some());
        assert_eq!(pool.available(), 0);

        // Pool exhausted
        assert!(pool.lease().is_none());
        assert_eq!(pool.available(), 0);

        // Return buffers
        pool.return_buffer(buf1.unwrap());
        assert_eq!(pool.available(), 1);

        pool.return_buffer(buf2.unwrap());
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_lease_zeroed() {
        let pool = BufferPool::new(1);

        // Lease, write, return
        let mut buf = pool.lease().unwrap();
        buf[0] = 0xFF;
        buf[100] = 0xAB;
        pool.return_buffer(buf);

        // Lease zeroed should give zeros
        let buf = pool.lease_zeroed().unwrap();
        assert_eq!(buf[0], 0);
        assert_eq!(buf[100], 0);
    }

    #[test]
    fn test_lease_page() {
        let pool = BufferPool::new(2);
        assert_eq!(pool.available(), 2);

        {
            let _page1 = pool.lease_page().expect("should lease");
            assert_eq!(pool.available(), 1);

            let _page2 = pool.lease_page().expect("should lease");
            assert_eq!(pool.available(), 0);

            // Pool exhausted
            assert!(pool.lease_page().is_none());
        }

        // Pages dropped, buffers returned
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_lease_page_zeroed() {
        let pool = BufferPool::new(1);

        // Get a page and write to it
        {
            let mut page = pool.lease_page_zeroed().expect("should lease");
            page.write_u8(0, 0xFF);
            page.write_u8(100, 0xAB);
        }

        // Lease zeroed page should give zeros
        let page = pool.lease_page_zeroed().expect("should lease");
        assert_eq!(page.read_u8(0), 0);
        assert_eq!(page.read_u8(100), 0);
    }

    #[test]
    #[should_panic(expected = "capacity must be positive")]
    fn test_zero_capacity_panics() {
        let _ = BufferPool::new(0);
    }

    #[test]
    #[should_panic(expected = "Buffer pool overflow")]
    fn test_return_to_full_pool_panics() {
        let pool = BufferPool::new(1);
        // Don't lease anything, just try to return an extra buffer
        let extra_buffer = Box::new([0u8; PAGE_SIZE]);
        pool.return_buffer(extra_buffer);
    }
}
