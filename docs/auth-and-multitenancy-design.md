# User Authentication and Hybrid Multi-Tenancy for Enso

## Overview

Add user authentication (JWT-based) and hybrid multi-tenancy (shared + user-scoped databases) to Enso.

## Key Design Decisions

| Aspect | Decision |
|--------|----------|
| User Auth | Bring Your Own JWT (verified via app's registered public key) |
| User ID | Standard JWT `sub` claim |
| App Config | "Admin app" stores configs for other apps |
| Tenancy | Hybrid: `{ shared: {...}, user: {...} }` schema sections |
| Queries | Unified queries spanning both shared and user data |
| Permissions | Deferred to later phase |

## Architecture

### Schema Syntax (Client)
```typescript
const schema = createSchema({
  shared: {
    posts: { title: t.string(), content: t.string() },
    comments: { text: t.string(), postId: t.ref('posts') }
  },
  user: {
    drafts: { content: t.string() },
    settings: { theme: t.string() }
  }
})
```

### Database File Layout (Server)
```
./data/
  {admin_app}.db                  # Admin app database (stores app configs)
  {app_api_key}.db                # Shared app database
  {app_api_key}/
    {user_id_hash}.db             # Per-user database
```

### JWT Trust Model
1. App registration: Developer provides JWT verification public key to admin app
2. Runtime: Client sends JWT in ConnectRequest
3. Server verifies signature using app's registered key
4. User ID extracted from `sub` claim

---

## Phase 1: Server Configuration & Auth Infrastructure

### Files to Create
- `server/src/config.rs` - Server configuration (admin_app_api_key, ports, etc.)
- `server/src/auth/mod.rs` - Auth module
- `server/src/auth/jwt.rs` - JWT verification using `jsonwebtoken` crate
- `server/src/auth/app_config.rs` - AppConfig struct (jwt_algorithm, jwt_public_key)
- `server/src/auth/config_registry.rs` - Load app configs from admin database

### Files to Modify
- `server/Cargo.toml` - Add `jsonwebtoken = "9"` dependency
- `server/src/main.rs` - Load config, bootstrap admin app

### Key Types
```rust
pub struct ServerConfig {
    pub admin_app_api_key: String,
    pub database_directory: PathBuf,
    pub listen_port: u16,
}

pub struct AppConfig {
    pub app_api_key: String,
    pub jwt_config: Option<JwtConfig>,
}

pub enum JwtConfig {
    Hs256 { secret: Vec<u8> },
    Rs256 { public_key: String },
}
```

---

## Phase 2: Protocol Changes

### Files to Modify
- `proto/protocol.proto` - Add fields to ConnectRequest

### Changes
```protobuf
message ConnectRequest {
  string app_api_key = 1;
  optional string auth_token = 2;  // JWT
}
```

Run `bun proto-gen` after changes.

---

## Phase 3: Hybrid Database Registry

### Files to Modify
- `server/src/database_registry.rs` - Add user database support

### New Methods
```rust
impl DatabaseRegistry {
    pub fn get_shared_database(&self, app_api_key: &str) -> Result<Arc<RwLock<Database>>>;
    pub fn get_user_database(&self, app_api_key: &str, user_id: &str) -> Result<Arc<RwLock<Database>>>;
}
```

User IDs are SHA-256 hashed for filesystem-safe paths.

---

## Phase 4: Connection State & Auth Flow

### Files to Modify
- `server/src/client_connection.rs` - Add auth flow, dual database refs

### Connection State Changes
```rust
pub enum ConnectionState {
    AwaitingConnect,
    Connected {
        app_api_key: String,
        user_id: Option<String>,  // From verified JWT
    },
}

pub struct ClientConnection {
    shared_database: Option<Arc<RwLock<Database>>>,
    user_database: Option<Arc<RwLock<Database>>>,
    // ...
}
```

### Auth Flow
1. Validate app_api_key format
2. Load app config from admin database
3. If app has JWT config, verify token
4. Extract user_id from `sub` claim
5. Open shared + user databases
6. Transition to Connected state

---

## Phase 5: Multi-Database Query Engine

### Files to Create
- `server/src/query/multi_database.rs` - Unified query across databases

### Strategy
- Entity-specific queries: Route to appropriate database based on entity scope
- Pattern queries: Execute on both databases, merge results
- Client includes scope hint in requests (from schema definition)

---

## Phase 6: Client Schema Types

### Files to Modify
- `client/typescript/internal/schema/types.ts` - New schema structure
- `client/typescript/internal/schema/create.ts` - Handle shared/user sections

### Type Changes
```typescript
export type SchemaDefinition = {
  shared?: Record<string, EntityDefinition>;
  user?: Record<string, EntityDefinition>;
};

export type Schema<S extends SchemaDefinition> = {
  readonly shared: S['shared'] extends Record<string, EntityDefinition> ? S['shared'] : {};
  readonly user: S['user'] extends Record<string, EntityDefinition> ? S['user'] : {};
  readonly entities: /* flattened view for internal use */;
};
```

### Validation
- Entity names must be unique across shared and user scopes
- Reserved fields checked in both scopes

---

## Phase 7: Client JWT Connection

### Files to Modify
- `client/typescript/internal/connection/types.ts` - Add JWT types
- `client/typescript/internal/connection/index.ts` - JWT in connect
- `client/typescript/index.ts` - Update createClient signature

### New API
```typescript
export const createClient = async <S extends SchemaDefinition>(opts: {
  schema: Schema<S>;
  serverUrl: string;
  jwt?: string;
  jwtProvider?: () => string | Promise<string>;
}): Promise<Client<S>>
```

---

## Phase 8: Client Store & Database Layer

### Files to Modify
- `client/typescript/internal/store/types.ts` - Add scope to operations
- `client/typescript/internal/store/network-store.ts` - Include scope in requests
- `client/typescript/internal/database/index.ts` - Handle both scopes

### Database Type
```typescript
export type Database<S extends SchemaDefinition> =
  { [K in keyof S['shared']]: DbEntity<S['shared'][K]> } &
  { [K in keyof S['user']]: DbEntity<S['user'][K]> };
```

Access is unified: `db.posts` and `db.drafts` work the same way.

---

## Backward Compatibility

- Apps without JWT config allow anonymous access (current behavior)
- Old schema syntax `{ entities: {...} }` detected and treated as all-shared
- `auth_token` is optional in ConnectRequest
- Existing database files remain valid

---

## Implementation Order (Incremental)

### Milestone 1: JWT Authentication (No Hybrid Tenancy)
Focus: Add user authentication while keeping single-database model

1. Server config and admin app bootstrap (Phases 1-2)
2. JWT verification flow (Phase 4, without user database)
3. Client JWT connection (Phase 7)
4. Protocol changes for auth_token

Deliverable: Users authenticate via JWT, but all data still in shared app database.

### Milestone 2: Hybrid Multi-Tenancy
Focus: Add per-user databases and unified queries

5. Schema syntax changes (Phase 6)
6. Database registry for user databases (Phase 3)
7. Multi-database query engine (Phase 5)
8. Client store scope hints (Phase 8)

Deliverable: Full hybrid model with shared + user-scoped entities.

---

## Scope Routing Decision

**Client sends scope hint in each request.** The schema is the source of truth - the client knows at compile time whether an entity is shared or user-scoped, and includes this in protocol messages. Server trusts the hint (user is already authenticated via JWT).
