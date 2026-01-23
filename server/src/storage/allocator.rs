//! Page allocator using a bitmap to track free/used pages.
//!
//! The allocation bitmap is stored in dedicated pages after the superblock.
//! Each bit represents one page: 0 = free, 1 = used.

// Page IDs are u64 but bitmap indices are usize. On 64-bit systems these are the same size.
// On 32-bit systems, we would need to handle databases larger than 4GB pages differently,
// but that's not a practical concern for this storage engine.
#![allow(clippy::cast_possible_truncation)]

use crate::storage::page::{Page, PageHeader, PageId, PageType};

/// Number of bits per byte.
const BITS_PER_BYTE: usize = 8;

/// Usable bytes per bitmap page (after header).
const BITMAP_BYTES_PER_PAGE: usize = PageHeader::USABLE_SPACE;

/// Number of pages tracked by a single bitmap page.
pub const PAGES_PER_BITMAP_PAGE: usize = BITMAP_BYTES_PER_PAGE * BITS_PER_BYTE;

/// Calculate how many bitmap pages are needed for a given number of total pages.
#[must_use]
#[allow(clippy::manual_div_ceil)] // div_ceil is not const-stable
pub const fn bitmap_pages_needed(total_pages: u64) -> u64 {
    let total = total_pages as usize;
    let pages_needed = (total + PAGES_PER_BITMAP_PAGE - 1) / PAGES_PER_BITMAP_PAGE;
    pages_needed as u64
}

/// A page allocation bitmap.
///
/// Tracks which pages are free or in use. The bitmap itself is stored
/// across one or more pages starting at page 1 (after the superblock).
#[derive(Debug)]
pub struct PageAllocator {
    /// The bitmap data (in-memory representation).
    bitmap: Vec<u8>,
    /// Total number of pages being tracked.
    total_pages: u64,
    /// Number of free pages.
    free_count: u64,
    /// Hint for next free page search (optimization).
    next_search_hint: u64,
}

impl PageAllocator {
    /// Create a new allocator for a fresh database.
    ///
    /// Initially marks page 0 (superblock) and the bitmap pages themselves as used.
    #[must_use]
    pub fn new(total_pages: u64) -> Self {
        let bitmap_pages = bitmap_pages_needed(total_pages);
        let reserved_pages = 1 + bitmap_pages; // superblock + bitmap pages

        // Calculate bitmap size (round up to bytes)
        let total_usize = total_pages as usize;
        let bitmap_bytes = total_usize.div_ceil(BITS_PER_BYTE);
        let mut bitmap = vec![0u8; bitmap_bytes];

        // Mark reserved pages as used
        for page_id in 0..reserved_pages {
            set_bit(&mut bitmap, page_id as usize);
        }

        let free_count = total_pages.saturating_sub(reserved_pages);

        Self {
            bitmap,
            total_pages,
            free_count,
            next_search_hint: reserved_pages,
        }
    }

    /// Load an allocator from bitmap pages.
    #[must_use]
    pub fn from_pages(pages: &[Page], total_pages: u64) -> Self {
        let total_usize = total_pages as usize;
        let bitmap_bytes = total_usize.div_ceil(BITS_PER_BYTE);
        let mut bitmap = vec![0u8; bitmap_bytes];

        // Copy bitmap data from pages
        let mut offset = 0;
        for page in pages {
            let src = &page.as_bytes()[PageHeader::SIZE..];
            let copy_len = (bitmap_bytes - offset).min(BITMAP_BYTES_PER_PAGE);
            bitmap[offset..offset + copy_len].copy_from_slice(&src[..copy_len]);
            offset += BITMAP_BYTES_PER_PAGE;
            if offset >= bitmap_bytes {
                break;
            }
        }

        // Count free pages
        let used_count: u64 = bitmap.iter().map(|b| u64::from(b.count_ones())).sum();
        let free_count = total_pages.saturating_sub(used_count);

        // Find first free page for hint
        let next_search_hint = (0..total_pages)
            .find(|&p| !get_bit(&bitmap, p as usize))
            .unwrap_or(0);

        Self {
            bitmap,
            total_pages,
            free_count,
            next_search_hint,
        }
    }

    /// Serialize the bitmap to pages.
    #[must_use]
    pub fn to_pages(&self) -> Vec<Page> {
        let num_pages = bitmap_pages_needed(self.total_pages);
        let mut pages = Vec::with_capacity(num_pages as usize);

        let mut offset = 0;
        for _ in 0..num_pages {
            let mut page = Page::new();

            // Write page header
            let header = PageHeader {
                page_type: PageType::AllocationBitmap,
                flags: 0,
                checksum: 0, // Will be computed later
            };
            page.write_bytes(0, &header.to_bytes());

            // Write bitmap data
            let copy_len = (self.bitmap.len() - offset).min(BITMAP_BYTES_PER_PAGE);
            page.write_bytes(PageHeader::SIZE, &self.bitmap[offset..offset + copy_len]);
            offset += BITMAP_BYTES_PER_PAGE;

            pages.push(page);
        }

        pages
    }

    /// Allocate a single free page.
    ///
    /// Returns `None` if no free pages are available.
    pub fn allocate(&mut self) -> Option<PageId> {
        if self.free_count == 0 {
            return None;
        }

        // Search from hint
        for page_id in self.next_search_hint..self.total_pages {
            if !get_bit(&self.bitmap, page_id as usize) {
                set_bit(&mut self.bitmap, page_id as usize);
                self.free_count -= 1;
                self.next_search_hint = page_id + 1;
                return Some(page_id);
            }
        }

        // Wrap around and search from beginning
        for page_id in 0..self.next_search_hint {
            if !get_bit(&self.bitmap, page_id as usize) {
                set_bit(&mut self.bitmap, page_id as usize);
                self.free_count -= 1;
                self.next_search_hint = page_id + 1;
                return Some(page_id);
            }
        }

        None
    }

    /// Allocate multiple contiguous pages.
    ///
    /// Returns `None` if no contiguous run of the requested size is available.
    pub fn allocate_contiguous(&mut self, count: u64) -> Option<PageId> {
        if count == 0 {
            return None;
        }
        if count > self.free_count {
            return None;
        }

        // Simple linear search for contiguous free pages
        let mut run_start = 0u64;
        let mut run_length = 0u64;

        for page_id in 0..self.total_pages {
            if get_bit(&self.bitmap, page_id as usize) {
                // Page is used, reset run
                run_start = page_id + 1;
                run_length = 0;
            } else {
                run_length += 1;
                if run_length >= count {
                    // Found a suitable run, mark all as used
                    for p in run_start..run_start + count {
                        set_bit(&mut self.bitmap, p as usize);
                    }
                    self.free_count -= count;
                    self.next_search_hint = run_start + count;
                    return Some(run_start);
                }
            }
        }

        None
    }

    /// Free a previously allocated page.
    pub fn free(&mut self, page_id: PageId) {
        if page_id >= self.total_pages {
            return;
        }

        if get_bit(&self.bitmap, page_id as usize) {
            clear_bit(&mut self.bitmap, page_id as usize);
            self.free_count += 1;

            // Update hint if this page is before current hint
            if page_id < self.next_search_hint {
                self.next_search_hint = page_id;
            }
        }
    }

    /// Check if a page is allocated.
    #[must_use]
    pub fn is_allocated(&self, page_id: PageId) -> bool {
        if page_id >= self.total_pages {
            return false;
        }
        get_bit(&self.bitmap, page_id as usize)
    }

    /// Get the number of free pages.
    #[must_use]
    pub const fn free_count(&self) -> u64 {
        self.free_count
    }

    /// Get the total number of pages being tracked.
    #[must_use]
    pub const fn total_pages(&self) -> u64 {
        self.total_pages
    }

    /// Expand the allocator to track more pages.
    ///
    /// The new pages are marked as free.
    pub fn expand(&mut self, new_total_pages: u64) {
        if new_total_pages <= self.total_pages {
            return;
        }

        let new_total_usize = new_total_pages as usize;
        let new_bitmap_bytes = new_total_usize.div_ceil(BITS_PER_BYTE);
        self.bitmap.resize(new_bitmap_bytes, 0);

        let added_pages = new_total_pages - self.total_pages;
        self.free_count += added_pages;
        self.total_pages = new_total_pages;
    }

    /// Get the first page ID used by the bitmap itself.
    #[must_use]
    pub const fn first_bitmap_page() -> PageId {
        1 // Right after superblock
    }

    /// Get the number of pages used by the bitmap.
    #[must_use]
    pub const fn bitmap_page_count(&self) -> u64 {
        bitmap_pages_needed(self.total_pages)
    }
}

/// Get a bit from the bitmap.
fn get_bit(bitmap: &[u8], index: usize) -> bool {
    let byte_index = index / BITS_PER_BYTE;
    let bit_index = index % BITS_PER_BYTE;

    if byte_index >= bitmap.len() {
        return false;
    }

    (bitmap[byte_index] & (1 << bit_index)) != 0
}

/// Set a bit in the bitmap (mark as used).
fn set_bit(bitmap: &mut [u8], index: usize) {
    let byte_index = index / BITS_PER_BYTE;
    let bit_index = index % BITS_PER_BYTE;

    if byte_index < bitmap.len() {
        bitmap[byte_index] |= 1 << bit_index;
    }
}

/// Clear a bit in the bitmap (mark as free).
fn clear_bit(bitmap: &mut [u8], index: usize) {
    let byte_index = index / BITS_PER_BYTE;
    let bit_index = index % BITS_PER_BYTE;

    if byte_index < bitmap.len() {
        bitmap[byte_index] &= !(1 << bit_index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_pages_needed() {
        // Each bitmap page can track PAGES_PER_BITMAP_PAGE pages
        assert_eq!(bitmap_pages_needed(1), 1);
        assert_eq!(bitmap_pages_needed(100), 1);
        assert_eq!(bitmap_pages_needed(PAGES_PER_BITMAP_PAGE as u64), 1);
        assert_eq!(bitmap_pages_needed(PAGES_PER_BITMAP_PAGE as u64 + 1), 2);
    }

    #[test]
    fn test_new_allocator() {
        let alloc = PageAllocator::new(100);

        // Page 0 (superblock) and page 1 (bitmap) should be allocated
        assert!(alloc.is_allocated(0));
        assert!(alloc.is_allocated(1));
        assert!(!alloc.is_allocated(2));

        assert_eq!(alloc.free_count(), 98); // 100 - 2 reserved
    }

    #[test]
    fn test_allocate_single() {
        let mut alloc = PageAllocator::new(100);
        let initial_free = alloc.free_count();

        let page = alloc.allocate().expect("should allocate");
        assert!(page >= 2); // Should skip reserved pages
        assert!(alloc.is_allocated(page));
        assert_eq!(alloc.free_count(), initial_free - 1);
    }

    #[test]
    fn test_allocate_and_free() {
        let mut alloc = PageAllocator::new(100);

        let page1 = alloc.allocate().expect("allocate 1");
        let page2 = alloc.allocate().expect("allocate 2");
        let page3 = alloc.allocate().expect("allocate 3");

        assert!(alloc.is_allocated(page1));
        assert!(alloc.is_allocated(page2));
        assert!(alloc.is_allocated(page3));

        let free_before = alloc.free_count();

        alloc.free(page2);

        assert!(alloc.is_allocated(page1));
        assert!(!alloc.is_allocated(page2));
        assert!(alloc.is_allocated(page3));
        assert_eq!(alloc.free_count(), free_before + 1);

        // Next allocation should reuse page2
        let page4 = alloc.allocate().expect("allocate 4");
        assert_eq!(page4, page2);
    }

    #[test]
    fn test_allocate_contiguous() {
        let mut alloc = PageAllocator::new(100);

        // Allocate some scattered pages
        let _ = alloc.allocate();
        let p = alloc.allocate().expect("alloc");
        let _ = alloc.allocate();
        alloc.free(p); // Create a hole

        // Try to allocate 5 contiguous pages
        let start = alloc.allocate_contiguous(5).expect("allocate contiguous");

        // Verify they are all allocated
        for i in 0..5 {
            assert!(alloc.is_allocated(start + i));
        }
    }

    #[test]
    fn test_allocate_exhaustion() {
        let mut alloc = PageAllocator::new(10);

        // Allocate all free pages
        while alloc.allocate().is_some() {}

        assert_eq!(alloc.free_count(), 0);
        assert!(alloc.allocate().is_none());
    }

    #[test]
    fn test_expand() {
        let mut alloc = PageAllocator::new(100);
        let free_before = alloc.free_count();

        alloc.expand(200);

        assert_eq!(alloc.total_pages(), 200);
        assert_eq!(alloc.free_count(), free_before + 100);

        // Can allocate the new pages
        for _ in 0..100 {
            assert!(alloc.allocate().is_some());
        }
    }

    #[test]
    fn test_roundtrip_to_pages() {
        let mut alloc = PageAllocator::new(1000);

        // Allocate some pages
        for _ in 0..50 {
            alloc.allocate();
        }

        let pages = alloc.to_pages();
        let restored = PageAllocator::from_pages(&pages, 1000);

        assert_eq!(restored.total_pages(), alloc.total_pages());
        assert_eq!(restored.free_count(), alloc.free_count());

        // Verify same pages are allocated
        for i in 0..1000 {
            assert_eq!(
                restored.is_allocated(i),
                alloc.is_allocated(i),
                "mismatch at page {i}"
            );
        }
    }

    #[test]
    fn test_bit_operations() {
        let mut bitmap = vec![0u8; 10];

        assert!(!get_bit(&bitmap, 0));
        assert!(!get_bit(&bitmap, 7));
        assert!(!get_bit(&bitmap, 8));
        assert!(!get_bit(&bitmap, 79));

        set_bit(&mut bitmap, 0);
        assert!(get_bit(&bitmap, 0));

        set_bit(&mut bitmap, 7);
        assert!(get_bit(&bitmap, 7));

        set_bit(&mut bitmap, 8);
        assert!(get_bit(&bitmap, 8));

        set_bit(&mut bitmap, 79);
        assert!(get_bit(&bitmap, 79));

        clear_bit(&mut bitmap, 7);
        assert!(!get_bit(&bitmap, 7));
        assert!(get_bit(&bitmap, 0)); // Others unchanged
    }
}
