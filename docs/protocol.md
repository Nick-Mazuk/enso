# Protocol

## Operations

- Clients do 1-time queries
- Clients can subscribe to triple updates ("send me triples that look like X"). *(Not yet implemented)*
- On subscribing to triple updates, clients can optionally say "and start providing updates from HLC X" *(Not yet implemented)*
- Clients can unsubscribe from triple updates *(Not yet implemented)*
- Clients can send triple updates. On success, the server responds with OK status and returns the current values of all written triples. On failure, the server returns an error status.

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
