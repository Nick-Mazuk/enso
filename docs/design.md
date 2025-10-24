# System Design - Local-First Data Replication and Sync System

## Overview

This system is a local-first data replication and sync system designed for web browsers and server environments. The core principle is that all reads and writes happen locally on the client, then replicate globally to the server for synchronization across all clients.

## High-Level Architecture

```txt
┌─────────────────────────────────────────────────────────────────┐
│                           Client (Browser)                      │
├─────────────────────────────────────────────────────────────────┤
│  Application Layer                                              │
│  ├─ React/Svelte Framework Integration                          │
│  └─ Client API (as defined in client/README.md)                 │
├─────────────────────────────────────────────────────────────────┤
│  Sync Engine                                                    │
│  ├─ Local-first Operations                                      │
│  ├─ Conflict Resolution (HLC)                                   │
│  ├─ Replication Protocol                                        │
│  └─ Connection Management                                       │
├─────────────────────────────────────────────────────────────────┤
│  Triple Store (Client)                                          │
│  ├─ Query Engine (Datalog-style)                                │
│  ├─ Schema Validation                                           │
│  └─ Change Tracking                                             │
├─────────────────────────────────────────────────────────────────┤
│  Persistence Layer                                              │
│  └─ IndexedDB Adapter                                           │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    │ WebSocket/HTTP
                                    │ Sync Protocol
                                    │
┌─────────────────────────────────────────────────────────────────┐
│                            Server                               │
├─────────────────────────────────────────────────────────────────┤
│  API Layer                                                      │
│  ├─ Authentication & Authorization                              │
│  ├─ Tenant Resolution                                           │
│  └─ Sync Protocol Handler                                       │
├─────────────────────────────────────────────────────────────────┤
│  Sync Engine                                                    │
│  ├─ Multi-tenant Coordination                                   │
│  ├─ Conflict Resolution (HLC)                                   │
│  ├─ Change Broadcasting                                         │
│  └─ Connection Management                                       │
├─────────────────────────────────────────────────────────────────┤
│  Triple Store (Server)                                          │
│  ├─ Query Engine (Datalog-style)                                │
│  ├─ Schema Validation                                           │
│  └─ Change Tracking                                             │
├─────────────────────────────────────────────────────────────────┤
│  Persistence Layer                                              │
│  └─ Per-tenant SQLite (Turso)                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### Why Triple Store Architecture?

- **Field-level operations**: Enables fine-grained conflict resolution and updates
- **Schema evolution**: Natural support for backwards compatibility through individual field storage
- **Query flexibility**: Supports complex relational queries through Datalog patterns
- **Shared implementation**: Same query engine works on both client and server

### Why Field-Level Hybrid Logical Clock?

- **Automatic merging**: Concurrent updates to different fields merge without conflicts
- **Reduced data loss**: Conflicts only occur on same-field updates, not entire documents
- **Causal ordering**: Maintains proper ordering across distributed clients
- **Granular timestamps**: Each triple gets its own timestamp for precise conflict resolution

### Why Schema-Agnostic Server?

- **Simplified server**: Server operates purely on triples without schema knowledge
- **Client-driven evolution**: Schema changes happen only on client side
- **Reduced coupling**: Server and client can evolve independently
- **Universal storage**: Same server can handle any schema from any client

### Why Local-First Operations?

- **Instant responsiveness**: All operations execute immediately on local data
- **Offline capability**: Full functionality without network connectivity
- **Optimistic UI**: No loading states for user operations
- **Eventual consistency**: Background sync ensures global coherence

## Data Layer

### Triple Store Format

**Core Triple Structure**: `(subject, predicate, object)`

- **Subject**: Document ID (e.g., `abc123def456`)
- **Predicate**: Entity type + field name (e.g., `"user/name"`, `"post/title"`)
- **Object**: Field value with proper typing

**Data Type Mapping**:

- **Strings**: stored as strings
- **Numbers**: stored as numbers
- **Booleans**: stored as booleans
- **Dates**: stored as UTC strings
- **Relations** (`t.ref`): object contains referenced document ID
- **Relations** (`t.refMany`): multiple triples with same subject/predicate, different object IDs

**Example Triple Set**:

```jsonc
("abc123", "user/name", "John Doe")
("abc123", "user/age", 30)
("abc123", "user/isActive", true)
("abc123", "user/createdAt", "2024-01-15T10:30:00.000Z")
("def456", "post/authorId", "abc123")  // t.ref
("def456", "post/tagIds", "1")          // t.refMany
("def456", "post/tagIds", "2")          // t.refMany
```

### Storage Implementation

**Client Storage** (Phase 1):

- In-memory Map structures for triple storage
- Simple indexing on subject, predicate, and object
- No IndexedDB persistence initially

**Server Storage** (SQLite):

```sql
CREATE TABLE triples (
    subject TEXT,
    predicate TEXT,
    object TEXT,
    object_type INTEGER,  -- Enum: 0=string, 1=number, 2=boolean, 3=date, 4=ref
    hlc TEXT,
    PRIMARY KEY (subject, predicate, object)
);

CREATE INDEX idx_subject ON triples (subject);
CREATE INDEX idx_predicate ON triples (predicate);
CREATE INDEX idx_object ON triples (object);
CREATE INDEX idx_hlc ON triples (hlc);
```

**Database Architecture**:

- Per-app SQLite databases with Turso for distributed sync
- Configurable: single database vs per-user databases per app
- Object type stored as integer enum for efficiency

## Identity and Addressing

### Document ID Generation

- **Client-side Generation**: nano IDs prevent conflicts across distributed clients
- **ID Format**: `nanoId` (e.g., `1StGXR8_Z5jdHi6B-myT`)
- **Collision Handling**: Extremely low probability due to large ID space and multi-tenancy

### App ID Management

- **Purpose**: Associate applications with developers (not strict multi-tenancy)
- **Generation**: Server-generated, distributed through management dashboard
- **Scope**: Each App ID has its own schema definition and data isolation
- **Future Dashboard**: Management frontend for app creation and configuration

### Room ID Handling

- **ID Options**: Auto-generated nano IDs or developer-specified custom IDs

## Synchronization Layer

### Hybrid Logical Clock (HLC)

**Core Principles**:

- **Field-Level Timestamps**: Each triple gets its own HLC timestamp
- **Concurrent Field Updates**: Different fields of same document merge automatically
- **Conflict Resolution**: Last-write-wins ordering at the field level
- **Causal Ordering**: Maintains proper event ordering across distributed clients

**Implementation Details**: HLC format and clock synchronization are implementation-specific

### Conflict Resolution

**Field-Level Last-Write-Wins**:

- Only same-field updates conflict with each other
- Concurrent updates to different fields automatically merge
- All operations report success (being overwritten is expected behavior)

**Delete Semantics**:

- **Tombstones**: Internal-only triples mark deleted entities
- **Format**: `(entityId, "_deleted", true, hlc_timestamp)`
- **Delete Wins**: Deletion always wins over concurrent updates regardless of timing
- **Privacy**: Tombstones never exposed through public APIs

### Replication Protocol

**Protocol Design**:

- **HTTP**: Bulk synchronization operations and initial data loading
- **WebSocket**: Real-time updates and live collaboration features
- **Hybrid Approach**: Different protocols optimized for different use cases

**Change Format**:

```typescript
{
  triple: [subject, predicate, object],
  hlc: timestamp,
  operation: "set" | "delete"
}
```

**Sync Strategy**:

- **On-Demand Loading**: Clients start empty, sync only queried data
- **Delta Sync**: Client provides last-known HLC, server sends incremental updates
- **Targeted Updates**: Server broadcasts only to clients with relevant active queries

**Network Resilience**:

- **Connection Recovery**: Clients include last-known timestamp for gap-free resumption
- **Partition Handling**: 1-minute timeout before stopping sync (longer partitions deferred)
- **Connection Protocols**: WebSocket for real-time sync, HTTP for bulk operations and initial loading
- **Automatic Reconnection**: Timestamp-based gap recovery on connection restore

**Multi-client Coordination**:

- Server tracks active queries per client for targeted updates
- Efficient broadcasting only to clients with relevant subscriptions
- Connection state management for presence and cleanup

### Error Handling

**Data Integrity**:

- **Partial Sync**: Field-level success/failure is normal operation pattern
- **Corruption Handling**: Invalid data treated as undefined on client
- **Type Coercion**: Mismatched types handled gracefully through fallbacks

## Application Layer

### Query Engine

**Datalog Extension**: Extended Datalog patterns support complex client API queries

**Query Execution**:

- **Aggregations**: On-demand execution using map-reduce over fetched data
- **Relation Joins**: Standard Datalog join patterns for multi-hop queries
- **Filtering**: Applied during query execution for optimal performance

**Shared Implementation**: Identical query engine runs on both client and server

### Schema and Validation

**Client-Only Schema**:

- Schema definition and validation happens exclusively on client
- Server operates as schema-agnostic triple store
- No schema distribution or handshake required

**Compatibility Strategy**:

- **Backwards Compatible**: Old clients ignore unknown fields (never fetch them)
- **Forwards Compatible**: New clients handle missing fields through fallbacks
- **Field-Level Storage**: Enables natural schema evolution over time

### Client API Integration

**Preload Operations**: Server-side preloading uses identical triple store queries as client operations

**Subscription Management**:

- **Delta Communication**: Only field-level changes transmitted over network
- **Result Set Assembly**: Client library merges deltas with cached data
- **Application Interface**: Complete result sets returned to application code

**Query Result Caching**:

- **Client-side**: Results cached in memory for efficient subscription updates
- **Server-side**: Subscription model where clients register interest for targeted updates

**Bootstrap Process**:

- Client self-identifies with App ID and User ID (no formal registration)
- Optimistic connection model with no schema handshake required

## Infrastructure Layer

### Real-time Rooms

**Room Architecture**:

- **Room Status**: In-memory room-wide state (lost on restart - desired behavior)
- **User Status**: Per-user ephemeral state while connected to room
- **Events**: Fire-and-forget broadcasts with HLC timestamps for ordering

**Multi-tenant Rooms**:

- Room access requires both App ID and Room ID for authorization
- Authorization prevents cross-tenant room access
- Support for both auto-generated and custom Room IDs

**Event Consistency**:

- Eventual consistency model for room events
- HLC timestamps enable client-side ordering of out-of-order events
- Room cleanup when all users disconnect

## Implementation Philosophy

### Core Principles

- **Zero Dependencies**: Build all components from scratch for reliability and learning
- **TypeScript Everywhere**: Shared code between client and server environments
- **Deterministic Testing**: Simulation testing for complex distributed scenarios

### Shared Components

- Triple store query engine (custom Datalog implementation)
- HLC implementation and conflict resolution logic
- Schema validation and type coercion
- Serialization and communication protocols

### Deferred Implementation Details

**Protocol Specifics**:

- HLC string format and serialization approach
- WebSocket message format and structure
- Nano ID collision handling (extremely low probability)

**Advanced Features**:

- Long-term network partition handling
- Storage optimization and cleanup strategies
- Advanced query optimization and caching
- Management dashboard for developers

**Authentication & Authorization**:

- Client authentication (JWT tokens, refresh handling)
- Row-level security with relation-based access control
- Currently: tenant-level authorization only

## Data Flow

1. **Local Operations**: All CRUD operations execute immediately on local triple store
2. **Change Tracking**: Operations generate change events with HLC timestamps
3. **Background Sync**: Changes replicate to server when online
4. **Conflict Resolution**: Server applies last-write-wins using HLC at field level
5. **Change Broadcasting**: Server notifies relevant clients of changes
6. **Local Updates**: Clients receive and apply remote changes to local store

## Quick Reference

### Key Formats

```typescript
// Triple Format
[subject: string, predicate: string, object: any]

// Document ID Format
"nanoId"

// Change Log Entry
{
  triple: [subject, predicate, object],
  hlc: string,
  operation: "set" | "delete"
}

// Tombstone Triple
[entityId, "_deleted", true]
```

### Object Type Enum

```typescript
enum ObjectType {
  STRING = 0,
  NUMBER = 1,
  BOOLEAN = 2,
  DATE = 3,
  REF = 4,
}
```
