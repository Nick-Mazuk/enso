# Protocol

## Operations

- Clients do 1-time queries
- Clients can subscribe to triple updates and receive streaming notifications
- On subscribing, clients can optionally specify a `since_hlc` to receive historical changes
- Clients can unsubscribe from triple updates
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

## Subscriptions

Clients can subscribe to receive real-time notifications when triples are modified.

### SubscribeRequest

To subscribe, send a `SubscribeRequest` with:

- **subscription_id** (uint32): Client-assigned identifier for this subscription. Must be unique per connection. Used for matching updates and unsubscribing.
- **since_hlc** (optional HlcTimestamp): If provided, the server will first send all changes since this timestamp as an initial `SubscriptionUpdate`, then continue with real-time updates.

On success, the server responds with `ServerResponse` containing OK status.

### SubscriptionUpdate

When triples are modified, the server sends `SubscriptionUpdate` messages to all subscribers:

- **subscription_id** (uint32): The subscription this update belongs to
- **changes** (repeated ChangeRecord): The changes that occurred

Each `ChangeRecord` contains:

- **change_type** (ChangeType): One of `INSERT`, `UPDATE`, or `DELETE`
- **triple** (Triple): The affected triple. For `DELETE` operations, only `entity_id`, `attribute_id`, and `hlc` are populated; `value` is not included.

### UnsubscribeRequest

To stop receiving updates, send an `UnsubscribeRequest` with:

- **subscription_id** (uint32): The subscription to cancel

On success, the server responds with OK status. On failure (e.g., subscription not found), the server returns an error status.

### Subscription Lifecycle

1. Client sends `SubscribeRequest` with a unique `subscription_id`
2. Server validates the ID is not already in use for this connection
3. If `since_hlc` is provided, server sends historical changes as initial `SubscriptionUpdate`
4. Server sends ongoing `SubscriptionUpdate` messages as changes occur
5. Client sends `UnsubscribeRequest` to cancel, or subscription ends on disconnect

### Change Types

- **INSERT**: A new triple was created
- **UPDATE**: An existing triple was modified with a newer HLC
- **DELETE**: A triple was removed

### Broadcast Semantics

All subscriptions on a connection receive the same change notifications. Changes are broadcast immediately after the transaction is committed, ensuring durability before notification.
