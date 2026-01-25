# Protocol

## Operations

- Clients do 1-time queries
- Clients can subscribe to triple updates ("send me triples that look like X"). *(Not yet implemented)*
- On subscribing to triple updates, clients can optionally say "and start providing updates from HLC X" *(Not yet implemented)*
- Clients can unsubscribe from triple updates *(Not yet implemented)*
- Clients can send triple updates. Each triple must include an HLC timestamp. The server uses the HLC to determine whether the update should be applied (see HLC-Based Conflict Resolution below). On success, the server responds with OK status and returns the current values of all written triples (which may differ from the submitted values if the submitted HLC was older). On failure, the server returns an error status.

## Data Constraints

The following constraints are enforced by the server:

### Entity and Attribute IDs

- **Length**: Must be exactly 16 bytes
- Requests with IDs of other lengths are rejected with `InvalidArgument`

### String Values

- **Maximum length**: 1024 characters
- Strings exceeding this limit are rejected with `InvalidArgument`

### Numeric Values

- Represented as IEEE 754 double-precision floating point numbers

### Boolean Values

- Standard true/false values

## HLC-Based Conflict Resolution

The server uses Hybrid Logical Clock (HLC) timestamps to resolve conflicts when multiple clients update the same triple.

### HLC Timestamp Format

Each HLC timestamp contains three components:

- **physical_time_ms** (uint64): Physical time in milliseconds since Unix epoch
- **logical_counter** (uint32): Logical counter for ordering events at the same physical time
- **node_id** (uint32): Node identifier for distributed uniqueness

### Conflict Resolution Rules

When a triple update request is received:

1. **New triple**: If no existing value exists for the (entity_id, attribute_id) pair, the update is always applied.

2. **Existing triple**: The server compares the client's HLC with the stored HLC using total ordering:
   - First compare `physical_time_ms`
   - If equal, compare `logical_counter`
   - If still equal, compare `node_id`

3. **Update applied**: If the client's HLC is strictly greater than the stored HLC, the triple is updated with the new value and HLC.

4. **Update rejected**: If the client's HLC is less than or equal to the stored HLC, the update is rejected and the existing value is retained.

### Per-Triple Resolution

Conflict resolution is applied independently to each triple in a batch update request. This means:

- Some triples in a single request may be updated while others are rejected
- The response always contains the current values for all triples in the request
- Each triple in the response includes its current HLC timestamp

### Missing HLC Validation

All triples in an update request must include an HLC timestamp. Requests containing triples without HLC timestamps are rejected with `InvalidArgument`.
