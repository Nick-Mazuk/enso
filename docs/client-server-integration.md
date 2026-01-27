# Plan: Connect TypeScript Client to Server

## Overview

Replace the in-memory Store with a WebSocket client that communicates with the server via protobuf, supporting optimistic updates and reactive subscriptions.

## Design Decisions

- **Optimistic updates:** Writes update local state immediately, sync to server in background
- **Queries:** All queries go to server (no local query engine)
- **Filters:** Keep type definitions but throw "not implemented" for complex filters. Only equality filters work with the server.
- **Reactive subscriptions:** Subscribe to changes and notify listeners when server data changes

## Current Architecture

```
User Code → Database<S> → Store (in-memory) → Local indices
```

## Target Architecture

```
User Code → Database<S> → Connection → Server
                            ↓
                     Pending writes (optimistic)
                            ↑
                     Subscription updates
```

**Key simplification:** No local query engine. All queries go to server. Local state only tracks pending writes for optimistic UI.

## Existing Infrastructure

- **Protobuf types:** `client/typescript/proto/protocol_pb.ts` (already generated)
- **@bufbuild/protobuf:** Already installed for serialization

## Implementation Steps

### Step 1: Create ID Conversion Utilities

The server requires exactly 16-byte IDs. Create utilities to convert between client IDs and server format.

**File:** `client/typescript/internal/id/index.ts`

- `stringToBytes(id: string): Uint8Array` - Convert nanoid (21 chars) to 16 bytes (hash/truncate)
- `bytesToString(bytes: Uint8Array): string` - Convert 16 bytes back to string (base64 or hex)
- `fieldToAttributeId(entityName: string, fieldName: string): Uint8Array` - Hash "users/name" → 16 bytes

### Step 2: Create HLC Timestamp Utilities

Generate HLC timestamps for conflict resolution.

**File:** `client/typescript/internal/hlc/index.ts`

- Generate unique node ID on client initialization
- `createTimestamp(): HlcTimestamp` - Current physical time + counter + node ID
- Maintain logical counter for ordering

### Step 3: Create Connection Class

WebSocket management with request/response correlation.

**File:** `client/typescript/internal/connection/index.ts`

```typescript
class Connection {
  private ws: WebSocket;
  private pendingRequests: Map<number, {resolve, reject}>;
  private nextRequestId: number = 1;
  private subscriptionHandlers: Map<number, (update) => void>;

  constructor(url: string, apiKey: string);

  connect(): Promise<void>;  // Send ConnectRequest

  send<T>(message: ClientMessage): Promise<ServerResponse>;

  subscribe(handler: (update: SubscriptionUpdate) => void): Promise<void>;

  close(): void;
}
```

Key behaviors:
- Connect to WebSocket, send ConnectRequest immediately
- Correlate responses via `request_id`
- Route SubscriptionUpdate messages to handlers
- Handle reconnection (queue pending operations)

### Step 4: Create NetworkStore Class

Replace in-memory Store with server-backed implementation. No local query engine - all queries go to server.

**File:** `client/typescript/internal/store/network-store.ts`

```typescript
class NetworkStore {
  private connection: Connection;
  private pendingWrites: Map<string, Triple[]>;  // Track optimistic writes by entity ID

  async add(...triples: Triple[]): Promise<void> {
    // 1. Track in pendingWrites (for optimistic UI if needed)
    // 2. Convert to TripleUpdateRequest
    // 3. Send to server
    // 4. On success, remove from pendingWrites
    // 5. On failure, notify error handler
  }

  async query(query: Query): Promise<Datom[][]> {
    // Convert to QueryRequest and send to server
    // Server handles all query execution
  }

  async deleteAllById(id: Id): Promise<void> {
    // 1. Track deletion in pendingWrites
    // 2. Send delete triples to server
  }

  handleSubscriptionUpdate(update: SubscriptionUpdate): void {
    // Notify listeners of changes (for reactive updates)
  }
}
```

### Step 5: Convert Internal Query Format to Protocol

Map the current internal query format to QueryRequest proto.

**Current internal format (from store/index.ts):**
```typescript
type Query = {
  find: Pattern[];
  where: Pattern[];
  optional: Pattern[];
  whereNot: Pattern[];
  filters: FilterFn[];
}
```

**Mapping:**
- `find` patterns → `QueryRequest.find` (QueryPatternVariable[])
- `where` patterns → `QueryRequest.where` (QueryPattern[])
- `optional` patterns → `QueryRequest.optional`
- `whereNot` patterns → `QueryRequest.where_not`
- `filters` → Keep type definitions, throw "not implemented" for complex filters

### Step 6: Update Database Layer

The Database layer needs to be aware of async operations.

**File:** `client/typescript/internal/database/index.ts`

Changes needed:
- Keep filter type definitions, throw "not implemented" for complex filters (greaterThan, lessThan, etc.)
- Keep `query()` async (already is)
- Make `create()` async (currently sync)
- Make `delete()` async (currently sync)

### Step 7: Update Client API

**File:** `client/typescript/index.ts`

```typescript
export const createClient = async <S>({
  schema,
  serverUrl,
  apiKey,
}: {
  schema: S;
  serverUrl: string;
  apiKey: string;
}): Promise<Client<S>> => {
  const connection = new Connection(serverUrl, apiKey);
  await connection.connect();
  // Subscribe to all changes
  await connection.subscribe(/* handler */);
  return new Client(schema, connection);
};
```

### Step 8: Add Subscription Handling

When receiving SubscriptionUpdate from server:
1. Parse ChangeRecord (INSERT/UPDATE/DELETE)
2. Update local cache
3. Notify any reactive listeners (if implemented)

### Step 9: Tests

- Connection tests (mock WebSocket)
- ID conversion tests
- HLC timestamp tests
- Round-trip serialization tests
- Integration tests with running server

## Files to Create

| File | Purpose |
|------|---------|
| `client/typescript/internal/id/index.ts` | ID conversion (string ↔ 16-byte) |
| `client/typescript/internal/hlc/index.ts` | HLC timestamp generation |
| `client/typescript/internal/connection/index.ts` | WebSocket management |
| `client/typescript/internal/store/network-store.ts` | Server-backed store |

## Files to Modify

| File | Changes |
|------|---------|
| `client/typescript/index.ts` | Add serverUrl/apiKey params, make createClient async |
| `client/typescript/internal/database/index.ts` | Make create/delete async, throw "not implemented" for complex filters |
| `client/typescript/internal/store/index.ts` | Replace with NetworkStore (or delete and use new file) |

## Breaking Changes

1. `createClient()` becomes async
2. `database.entity.create()` becomes async
3. `database.entity.delete()` becomes async
4. Complex filters (greaterThan, lessThan, contains, etc.) throw "not implemented" - only equality supported

## Implementation Order

1. ID conversion utilities
2. HLC timestamp utilities
3. Connection class (can test against server)
4. NetworkStore class
5. Update Database layer
6. Update Client API
7. Add subscription handling
8. Tests
