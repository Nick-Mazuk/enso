//! ID types for entities and attributes.
//!
//! This module provides newtype wrappers for entity and attribute IDs,
//! ensuring type safety and providing convenient methods for creation
//! and display.

use std::fmt;

/// A unique identifier for an entity.
///
/// Wraps a 16-byte array. The inner field is public to allow direct access
/// to the bytes when needed for serialization or storage operations.
///
/// # Invariants
///
/// - The ID is exactly 16 bytes.
/// - The ID may contain any byte values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct EntityId(pub [u8; 16]);

impl EntityId {
    /// Create an entity ID from a string.
    ///
    /// Uses the first 16 bytes of the string, zero-padded if shorter.
    /// Truncates if longer than 16 bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use server::types::EntityId;
    /// let id = EntityId::from_string("user1");
    /// assert_eq!(&id.0[..5], b"user1");
    /// ```
    #[must_use]
    pub fn from_string(s: &str) -> Self {
        let mut bytes = [0u8; 16];
        let src = s.as_bytes();
        let len = src.len().min(16);
        bytes[..len].copy_from_slice(&src[..len]);
        Self(bytes)
    }

    /// Create an entity ID from a u64.
    ///
    /// The u64 is stored in little-endian format in the first 8 bytes,
    /// with the remaining 8 bytes zeroed.
    #[must_use]
    pub fn from_u64(n: u64) -> Self {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&n.to_le_bytes());
        Self(bytes)
    }

    /// Get the underlying byte array.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Try to display as string if valid UTF-8, otherwise as hex
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "{}", s.trim_end_matches('\0'))
        } else {
            write!(f, "{:02x?}", &self.0[..])
        }
    }
}

impl From<[u8; 16]> for EntityId {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl From<EntityId> for [u8; 16] {
    fn from(id: EntityId) -> Self {
        id.0
    }
}

/// A field/attribute identifier.
///
/// Wraps a 16-byte array. The inner field is public to allow direct access
/// to the bytes when needed for serialization or storage operations.
///
/// # Invariants
///
/// - The ID is exactly 16 bytes.
/// - The ID may contain any byte values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct AttributeId(pub [u8; 16]);

impl AttributeId {
    /// Create an attribute ID from a string.
    ///
    /// Uses the first 16 bytes of the string, zero-padded if shorter.
    /// Truncates if longer than 16 bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use server::types::AttributeId;
    /// let id = AttributeId::from_string("name");
    /// assert_eq!(&id.0[..4], b"name");
    /// ```
    #[must_use]
    pub fn from_string(s: &str) -> Self {
        let mut bytes = [0u8; 16];
        let src = s.as_bytes();
        let len = src.len().min(16);
        bytes[..len].copy_from_slice(&src[..len]);
        Self(bytes)
    }

    /// Get the underlying byte array.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for AttributeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "{}", s.trim_end_matches('\0'))
        } else {
            write!(f, "{:02x?}", &self.0[..])
        }
    }
}

impl From<[u8; 16]> for AttributeId {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl From<AttributeId> for [u8; 16] {
    fn from(id: AttributeId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_id_from_str() {
        let id = EntityId::from_string("test");
        assert_eq!(&id.0[..4], b"test");
        assert_eq!(&id.0[4..], &[0u8; 12]);
    }

    #[test]
    fn test_entity_id_from_str_long() {
        let id = EntityId::from_string("this_is_a_very_long_string");
        assert_eq!(id.0.len(), 16);
        assert_eq!(&id.0[..], b"this_is_a_very_l");
    }

    #[test]
    fn test_entity_id_from_u64() {
        let id = EntityId::from_u64(42);
        assert_eq!(u64::from_le_bytes(id.0[..8].try_into().unwrap()), 42);
        assert_eq!(&id.0[8..], &[0u8; 8]);
    }

    #[test]
    fn test_entity_id_display_utf8() {
        let id = EntityId::from_string("user1");
        assert_eq!(format!("{id}"), "user1");
    }

    #[test]
    fn test_entity_id_display_binary() {
        let id = EntityId([
            0xFF, 0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ]);
        let s = format!("{id}");
        // Should fall back to hex display since it's not valid UTF-8
        assert!(s.contains("ff") || s.contains("FF") || s.contains("0xff"));
    }

    #[test]
    fn test_attribute_id_from_str() {
        let id = AttributeId::from_string("name");
        assert_eq!(&id.0[..4], b"name");
        assert_eq!(&id.0[4..], &[0u8; 12]);
    }

    #[test]
    fn test_entity_id_from_bytes() {
        let bytes = [1u8; 16];
        let id = EntityId::from(bytes);
        assert_eq!(id.0, bytes);
    }

    #[test]
    fn test_attribute_id_into_bytes() {
        let id = AttributeId::from_string("test");
        let bytes: [u8; 16] = id.into();
        assert_eq!(&bytes[..4], b"test");
    }

    #[test]
    fn test_entity_id_equality() {
        let id1 = EntityId::from_string("test");
        let id2 = EntityId::from_string("test");
        let id3 = EntityId::from_string("other");
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }
}
