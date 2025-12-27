pragma foreign_keys = ON;

pragma journal_mode = wal;

CREATE TABLE triples (
  -- The entity's ID. It's a 128-bit UUID stored as bytes.
  entity_id blob,
  -- The 128-bit hash of the attribute, stored as bytes.
  attribute_id blob,
  -- If the triple's value is a number, it'll be stored here. Else this column will be empty.
  number_value REAL,
  -- If the triple's value is a string, it'll be stored here. Else this column will be empty.
  string_value TEXT,
  -- If the triple's value is a boolean, it'll be stored here (0 = false, 1 = true, other values are invalid).
  -- Else this column will be empty.
  boolean_value INTEGER,
  -- Ensure there's only a single entity_id / attribute_id pair for each triple.
  PRIMARY KEY (entity_id, attribute_id)
);

CREATE INDEX idx_triples_attribute_number_value ON triples (attribute_id, number_value)
WHERE
  number_value IS NOT NULL;

CREATE INDEX idx_triples_attribute_string_value ON triples (attribute_id, string_value)
WHERE
  string_value IS NOT NULL;

CREATE INDEX idx_triples_attribute_boolean_value ON triples (attribute_id, boolean_value)
WHERE
  boolean_value IS NOT NULL;
