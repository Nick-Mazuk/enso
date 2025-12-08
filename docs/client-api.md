# Client API

The following shows example usage of the client API.

```ts
// 0. Create the schema

import { createSchema, t } from "@sync-engine/core";

const schema = createSchema({
  entities: {
    $users: {
      name: t.string({ fallback: "" }),
      email: t.string({ fallback: "" }),
      age: t.number({ optional: true }),
      isAdult: t.boolean({ fallback: false }),
      status: t.string({ fallback: "active" }),
      lastLogin: t.date({ optional: true }),
      isSenior: t.boolean({ optional: true, fallback: false }),
    },
    posts: {
      title: t.string({ fallback: "" }),
      content: t.string({ fallback: "" }),
      published: t.boolean({ fallback: false }),
      viewCount: t.number({ fallback: 0 }),
      authorId: t.ref("users"),
      tagIds: t.refMany("tags"),
    },
    tags: {
      name: t.string({ fallback: "" }),
    },
  },
  rooms: {
    documentEditor: {
      // Fire-and-forget events (broadcast once, then forgotten)
      events: {
        like: t.object({
          targetId: t.string({ fallback: "" }),
          userId: t.string({ fallback: "" }),
        }),
        celebration: t.object({
          type: t.string({ fallback: "confetti" }),
          x: t.number({ fallback: 0 }),
          y: t.number({ fallback: 0 }),
        }),
      },
      // Per-user session state (each user has their own copy)
      userStatus: {
        cursor: t.object({
          x: t.number({ fallback: 0 }),
          y: t.number({ fallback: 0 }),
          selection: t.string({ optional: true }),
        }),
        isTyping: t.boolean({ fallback: false }),
        activeSelection: t.array(t.string({ fallback: "" })),
      },
      // Room-wide shared state (single shared state for the room)
      roomStatus: {
        documentTitle: t.string({ optional: true }),
        lastSaved: t.date({ optional: true }),
        collaboratorCount: t.number({ fallback: 0 }),
      },
    },
  },
});

// Schema field types:
// - Required fields must have a `fallback` value for backwards compatibility
// - Optional fields use `{ optional: true }` and can optionally have fallbacks
// - Optional fields with fallbacks will always exist when reading, but are not required when writing
// - Available types: t.string(), t.number(), t.boolean(), t.date(), t.json()
// - Relations: t.ref('entityName') for single references, t.refMany('entityName') for arrays
// - Relations are always optional (t.ref can be undefined, t.refMany defaults to [])
// - Special fallback: 'now' for current timestamp on dates

// Write vs Read behavior:
// - CREATE/REPLACE operations must provide ALL required fields explicitly
// - UPDATE operations only modify specified fields (partial updates)
// - READ operations apply fallbacks for missing fields in stored documents
// - This ensures consistent data without surprise changes when fallbacks change

// Note: Every document automatically includes these read-only fields:
// - `id`: Auto-generated unique identifier, never changes
// - `createdAt`: Timestamp when document was created, never changes
// - `updatedAt`: Timestamp automatically updated on any document modification
// - `createdBy`: The userID of the user that created this document
// These fields are handled automatically but must be explicitly requested in queries to be returned

// Rooms provide real-time pub/sub capabilities for ephemeral data that doesn't persist to disk:
// - `events`: Fire-and-forget broadcasts that are sent to all room participants and immediately forgotten
// - `userStatus`: Per-user session state that persists while the user is connected to the room
// - `roomStatus`: Room-wide shared state that all participants can read and modify
// - Room IDs are specified when joining/using a room (e.g., document ID, chat channel ID)
// - Perfect for presence indicators, live cursors, reactions, typing indicators, and real-time collaboration
// - Runtime validation ensures no key collisions between userStatus and roomStatus

// 1. Initialize the client by passing a configuration object.
// The returned `client` object is now fully typed automatically.
const client = createClient({
  schema,
  // other config options can be added here in the future
});

// === Basic CRUD Operations (Immediate Execution) ===

// CREATE a new document (must provide ALL required fields)
const { data: newUser } = await client.database.users.create({
  // All required fields must be provided explicitly
  name: "Jane Doe",
  email: "jane.doe@example.com",
  isAdult: false,
  status: "active",
  // Optional fields can be omitted
  age: 30,
});

// `newUser` contains the entire created document with all fields
// including auto-generated id, createdAt, updatedAt
console.log("Created new user:", newUser);

// UPDATE a document (partial update)
await client.database.users.update({
  id: newUser.id,
  fields: {
    // payload is inferred to be `Partial<User>`
    age: 46,
  },
});

// UPDATE with functional fields (atomic operations)
await client.database.posts.update({
  id: postId,
  fields: (prev) => ({
    // Atomic increment to prevent race conditions
    viewCount: prev.viewCount + 1,
    // Complex logic with previous values
    status: prev.viewCount >= 1000 ? "popular" : prev.status,
    // Regular field updates
    lastViewed: new Date(),
  }),
});

// REPLACE a document (must provide ALL required fields)
await client.database.users.replace({
  id: newUser.id,
  fields: {
    // All required fields must be provided for replace
    name: "Jane Smith",
    email: "jane.smith@example.com",
    isAdult: true,
    status: "active",
    // Optional fields can be omitted or included
    age: 43,
  },
});

// REPLACE with functional fields (atomic operations)
await client.database.users.replace({
  id: newUser.id,
  fields: (prev) => ({
    // All required fields must be provided for replace
    name: prev.name,
    email: prev.email,
    isAdult: prev.age >= 18, // Computed from previous age
    status: prev.loginCount > 10 ? "premium" : "active",
    // Atomic increment
    loginCount: prev.loginCount + 1,
    // Optional fields
    age: prev.age,
    lastLogin: new Date(),
  }),
});

// DELETE a document
await client.database.users.delete(newUser.id);

// === Bulk Operations ===

// UPDATE MANY documents
const { data: updateResult } = await client.database.users.updateMany({
  where: {
    age: { greaterThan: 18 },
  },
  fields: {
    isAdult: true,
  },
});

console.log(`Updated ${updateResult.count} users`);

// UPDATE MANY with functional fields
await client.database.posts.updateMany({
  where: { published: true },
  fields: (prev) => ({
    // Each document gets function called with its own prev values
    viewCount: prev.viewCount + 1,
    popularity: Math.min(prev.viewCount / 1000, 1.0), // Cap at 1.0
  }),
});

// REPLACE MANY documents (must provide ALL required fields)
const { data: replaceResult } = await client.database.users.replaceMany({
  where: {
    status: { equals: "inactive" },
  },
  fields: {
    // All required fields must be provided for replace operations
    name: "Archived User",
    email: "archived@example.com",
    isAdult: false,
    status: "archived",
    // Optional fields (age, lastLogin, isSenior) will use fallbacks on read
  },
});

console.log(`Replaced ${replaceResult.count} users`);

// REPLACE MANY with functional fields
await client.database.users.replaceMany({
  where: { status: { equals: "trial" } },
  fields: (prev) => ({
    // All required fields must be provided for replace operations
    name: prev.name,
    email: prev.email,
    isAdult: prev.age >= 18,
    status: prev.loginCount > 5 ? "active" : "expired",
    // Atomic operations with previous values
    loginCount: prev.loginCount,
    lastLogin: prev.lastLogin,
  }),
});

// DELETE MANY documents
const { data: deleteResult } = await client.database.users.deleteMany({
  where: {
    lastLogin: { lessThan: new Date("2023-01-01") },
  },
});

console.log(`Deleted ${deleteResult.count} users`);

// === Query Operations ===

// QUERY multiple documents with filtering
const { data: users } = await client.database.users.query({
  fields: {
    name: true,
    email: true,
    age: true,
  },
  where: {
    age: { greaterThan: 25 },
    email: { endsWith: "@example.com" },
  },
});

// `users` is inferred to be `User[]`
console.log("Found users:", users);

// === Reactive Subscriptions ===

// SUBSCRIBE to query results with real-time updates
const subscription = client.database.users.subscribe(
  {
    fields: {
      name: true,
      email: true,
      age: true,
    },
    where: {
      age: { greaterThan: 25 },
      email: { endsWith: "@example.com" },
    },
  },
  (data, error, loading) => {
    // Called immediately: data=undefined, error=undefined, loading=true
    // Called when data arrives: data=[...], error=undefined, loading=false
    // Called on future updates when data changes: data=[...], error=undefined, loading=false
    // data: Entity[] | undefined, error: DatabaseError | undefined, loading: boolean
    console.log("Data updated:", { data, error, loading });
  }
);

// Check current state at any time
const currentState = subscription.getCurrentState();
console.log("Current state:", currentState); // { data, error, loading }

// Unsubscribe when done
subscription.unsubscribe();

// SUBSCRIBE ONE to single document with real-time updates
const singleSubscription = client.database.users.subscribeOne(
  {
    fields: {
      name: true,
      email: true,
      age: true,
    },
    where: {
      email: { equals: "jane.doe@example.com" },
    },
  },
  (data, error, loading) => {
    // Called immediately: data=undefined, error=undefined, loading=true
    // Called when data arrives: data=User|undefined, error=undefined, loading=false
    // Called on future updates when data changes: data=User|undefined, error=undefined, loading=false
    // data: Entity | undefined, error: DatabaseError | undefined, loading: boolean
    console.log("Single user updated:", { data, error, loading });
  }
);

singleSubscription.unsubscribe();

// SUBSCRIBE to aggregations with real-time updates
const aggregationSubscription = client.database.posts.subscribe(
  {
    aggregations: {
      totalViews: { sum: "viewCount" },
      postCount: { count: "*" },
    },
    where: { published: true },
  },
  (data, error, loading) => {
    // Called immediately: data=undefined, error=undefined, loading=true
    // Called when data arrives: data={ totalViews: 50000, postCount: 150 }, error=undefined, loading=false
    // Called on future updates when aggregations change: data={ totalViews: 51000, postCount: 152 }, error=undefined, loading=false
    console.log("Aggregation updated:", { data, error, loading });
  }
);

aggregationSubscription.unsubscribe();

// SUBSCRIBE with preloaded data (no loading state)
// 1. First preload the data (typically server-side)
const { data: preloadedUsers, error: preloadError } =
  await client.database.users.preload({
    fields: {
      name: true,
      email: true,
      age: true,
    },
    where: {
      age: { greaterThan: 25 },
    },
  });

if (preloadError) {
  console.error("Preload failed:", preloadError);
} else {
  // 2. Subscribe using preloaded data (typically client-side)
  const preloadedSubscription = client.database.users.subscribe(
    preloadedUsers, // Contains both data and original query options
    (data, error, loading) => {
      // Called immediately: data=[...], error=undefined, loading=false
      // Called on future updates: data=[...], error=undefined, loading=false
      console.log("Preloaded data updated:", { data, error, loading });
    }
  );

  // preloadedSubscription.getCurrentState() -> immediate data, no loading
  // preloadedSubscription.unsubscribe()
}

// QUERY ONE document (returns first match)
const { data: user } = await client.database.users.queryOne({
  fields: {
    name: true,
    email: true,
    age: true,
  },
  where: {
    email: { equals: "jane.doe@example.com" },
  },
});

// `user` is inferred to be `User | undefined`
console.log("Found user:", user);

// === Pagination ===

// PAGINATED query for large datasets
const paginatedUsers = client.database.users.paginated({
  fields: {
    name: true,
    email: true,
    age: true,
  },
  where: { age: { greaterThan: 18 } },
  orderBy: [["name","asc"]],
  pageSize: 20,
});

// Subscribe to pagination state
paginatedUsers.subscribe((state) => {
  console.log("Current page:", state.data); // Array of 20 users
  console.log(
    "Page info:",
    state.currentPage,
    state.estimatedTotalPages,
    state.estimatedTotalCount
  );
  console.log("Navigation:", state.hasNext, state.hasPrevious);
  console.log("Loading:", state.loading); // Only true initially
});

// Navigation (instant due to prefetching)
await paginatedUsers.next();
await paginatedUsers.previous();

// === Advanced Query Features ===

// LIMITING results
const { data: limitedUsers, error: limitError } =
  await client.database.users.query({
    fields: {
      name: true,
      email: true,
      age: true,
    },
    where: {
      age: { greaterThan: 18 },
    },
    limit: 10,
    orderBy: [["age", "desc"], ["name", "asc"]],
  });

// ORDERING results
const { data: orderedPosts, error: orderError } =
  await client.database.posts.query({
    fields: {
      title: true,
      createdAt: true,
      viewCount: true,
    },
    where: {
      published: true,
    },
    orderBy: [
      ["createdAt", "desc"], // Most recent first
      ["viewCount", "desc"], // Then by view count
    ],
    limit: 5,
  });

// === Aggregations ===

// Simple aggregations (no groupBy)
const { data: stats } = await client.database.posts.query({
  aggregations: {
    totalPosts: { count: "*" },
    totalViews: { sum: "viewCount" },
    avgViews: { avg: "viewCount" },
    maxViews: { max: "viewCount" },
    minViews: { min: "viewCount" },
  },
  where: { published: true },
});
// Returns: { totalPosts: 150, totalViews: 50000, avgViews: 333, maxViews: 2000, minViews: 5 }

// Grouped aggregations
const { data: byAuthor } = await client.database.posts.query({
  aggregations: {
    postCount: { count: "*" },
    totalViews: { sum: "viewCount" },
    avgViews: { avg: "viewCount" },
  },
  groupBy: ["authorId"],
  where: { published: true },
  orderBy: [["totalViews", "desc"]],
  limit: 5,
});
// Returns: Array<{ authorId: string, postCount: number, totalViews: number, avgViews: number }>

// === Filter Operators ===

// Example: Adults under retirement age
const { data: workingAgeUsers } = await client.database.users.query({
  fields: {
    name: true,
    age: true,
  },
  where: {
    age: { greaterThan: 18, lessThan: 65 },
  },
});

// === Querying Relations ===

// Query with related data (joins resolved automatically at query time)
const { data: usersWithPosts } = await client.database.users.query({
  fields: {
    name: true,
    email: true,
    posts: {
      // Auto-resolved: find posts where posts.authorId = users.id
      fields: {
        title: true,
        createdAt: true,
        published: true,
      },
      where: {
        published: true,
      },
      orderBy: ["createdAt", "desc"],
      limit: 10, // Only get 10 most recent posts
    },
  },
  where: {
    age: { greaterThan: 25 },
  },
  orderBy: ["name","asc"],
});

// === Batch Write Operations ===

// To perform a batch write, you first create an array of "operation objects".
// You use the `client.database.batch` namespace, which mirrors the structure of `client.database`
// but its methods return a serializable object instead of a Promise.

// 1. Create descriptions of the operations using the `client.database.batch` object.
const updateUserOp = client.database.batch.users.update({
  id: newUser.id,
  fields: { name: "Batched Name" },
});
const createPostOp = client.database.batch.posts.create({
  title: "Welcome to my blog",
  content: "This is my first post",
  published: false,
  viewCount: 0,
  authorId: newUser.id,
  tagIds: ["tag1", "tag2"],
});

// 2. Build your array of operations.
const operations = [updateUserOp, createPostOp];

// 3. Pass the array to the `client.database.batch.execute()` method.
const { data: batchResults } = await client.database.batch.execute(operations);

// batchResults is an array of DatabaseResult corresponding to each operation
// in the same order as the operations array
console.log(`Batch completed! Results:`, batchResults);

// **Important**: Batch operations are NOT atomic. Each operation executes independently,
// so some operations may succeed while others fail. Check individual results for errors.

// === Real-time Rooms ===

// JOIN a room with optional ID
const documentRoom = client.rooms.documentEditor("doc-123"); // Specific room
const globalRoom = client.rooms.documentEditor(); // Global singleton room

// EMIT events (fire-and-forget broadcasts)
await documentRoom.emit("like", {
  targetId: "paragraph-1",
  userId: "user-456",
});
await documentRoom.emit("celebration", { type: "confetti", x: 100, y: 200 });

// SET room-wide status (setting variables on the room itself)
await documentRoom.set("documentTitle", "My Document");
await documentRoom.set("lastSaved", new Date());
await documentRoom.set("collaboratorCount", 5);

// SET per-user status (setting my own user status)
await documentRoom.setUserStatus("cursor", {
  x: 150,
  y: 300,
  selection: "paragraph-2",
});
await documentRoom.setUserStatus("isTyping", true);
await documentRoom.setUserStatus("activeSelection", ["elem1", "elem2"]);

// LISTEN to events (explicit listeners for each event type)
documentRoom.on("like", (data, fromUserId) => {
  console.log(`${fromUserId} liked:`, data.targetId);
});

documentRoom.on("celebration", (data, fromUserId) => {
  console.log(`${fromUserId} celebrated at:`, data.x, data.y);
});

// LISTEN to room status changes (current state, not deltas)
documentRoom.onRoomStatus((currentRoomStatus) => {
  console.log("Current room status:", currentRoomStatus);
  // currentRoomStatus: { documentTitle: string, lastSaved: Date, collaboratorCount: number }
});

// LISTEN to user status changes (current state of all users, not deltas)
documentRoom.onUserStatus((allUserStatuses) => {
  console.log("All user statuses:", allUserStatuses);
  // allUserStatuses: { [userId]: { cursor: {...}, isTyping: boolean, activeSelection: [...] } }
});

// GET current state (declarative snapshots)
const currentRoomStatus = documentRoom.getRoomStatus();
// Returns: { documentTitle: string, lastSaved: Date, collaboratorCount: number }

const allUserStatuses = documentRoom.getUserStatuses();
// Returns: { [userId]: { cursor: {...}, isTyping: boolean, activeSelection: [...] } }

const myStatus = documentRoom.getMyUserStatus();
// Returns: { cursor: {...}, isTyping: boolean, activeSelection: [...] }

// LEAVE room
documentRoom.leave();
```

## API Reference

### Query Structure

All queries follow one of these structures:

```typescript
// Entity queries (returns entities)
interface EntityQueryOptions {
  fields: FieldSelection; // Required: what data to return
  where?: FilterConditions; // Optional: filter conditions
  orderBy?: OrderByClause; // Optional: sorting
  limit?: number; // Optional: max results to return
}

// Aggregation queries (returns aggregated data)
interface AggregationQueryOptions {
  aggregations: AggregationSelection; // Required: what aggregations to compute
  where?: FilterConditions; // Optional: filter conditions
  groupBy?: string[]; // Optional: group by fields
  orderBy?: OrderByClause; // Optional: sorting (on aggregated results)
  limit?: number; // Optional: max results to return
}
```

**TypeScript Enforcement Rules:**

- ✅ `fields` only (entity query)
- ✅ `aggregations` only (simple aggregation)
- ✅ `aggregations` + `groupBy` (grouped aggregation)
- ❌ `fields` + `aggregations` (not allowed)
- ❌ `fields` + `groupBy` (not allowed)
- ❌ `groupBy` without `aggregations` (not allowed)

### Field Selection

The `fields` object specifies what data to return. You must explicitly opt-in to each field by setting it to `true`:

```typescript
fields: {
  name: true,        // Include name
  email: true,       // Include email
  id: true,          // Must explicitly request auto-generated fields
  createdAt: true,   // Must explicitly request auto-generated fields
  updatedAt: true    // Must explicitly request auto-generated fields
  // age: omitted fields are not returned
}
```

**Important**: Only `true` values are allowed. You cannot set fields to `false` - simply omit fields you don't want returned. This explicit opt-in approach ensures you only receive the data you actually need.

### Aggregation Selection

The `aggregations` object specifies what aggregations to compute:

```typescript
aggregations: {
  totalPosts: { count: '*' },          // Count all records
  publishedPosts: { count: 'published' }, // Count non-null published values
  totalViews: { sum: 'viewCount' },    // Sum numeric values
  avgViews: { avg: 'viewCount' },      // Average of numeric values
  maxViews: { max: 'viewCount' },      // Maximum value
  minViews: { min: 'viewCount' }       // Minimum value
}
```

**Available Aggregation Functions:**

- `count: '*'` - Count all records (including nulls)
- `count: 'fieldName'` - Count non-null values in field
- `sum: 'fieldName'` - Sum numeric values
- `avg: 'fieldName'` - Average of numeric values
- `min: 'fieldName'` - Minimum value
- `max: 'fieldName'` - Maximum value

### Relation Queries

For relation fields (defined with `t.ref()` or `t.refMany()`), you can nest full query options:

```typescript
fields: {
  name: true,
  posts: {  // t.refMany('posts')
    fields: {
      title: true,
      createdAt: true
    },
    where: {
      published: true
    },
    orderBy: ['createdAt', 'desc'],
    limit: 10
  },
  profile: {  // t.ref('profiles')
    fields: {
      bio: true
    },
    where: {
      isPublic: true
    }
    // orderBy, limit allowed but typically meaningless for single refs
  }
}
```

**Note**: All query options (`where`, `orderBy`, `limit`) are allowed on both single (`t.ref`) and array (`t.refMany`) relations for API consistency, even though `limit` is typically only useful for array relations.

**Relation Filtering**: If a `where` clause on a relation filters out the related document entirely, the relation field will be `undefined`, just as if the related document didn't exist.

### Filter Operators

#### String Operators

- `equals` - Exact match
- `notEquals` - Not equal
- `contains` - Contains substring
- `startsWith` - Starts with string
- `endsWith` - Ends with string

#### Number Operators

- `equals` - Exact match
- `notEquals` - Not equal
- `greaterThan` - Greater than value
- `greaterThanOrEqual` - Greater than or equal
- `lessThan` - Less than value
- `lessThanOrEqual` - Less than or equal

#### Array Operators

- `in` - Value is in array
- `notIn` - Value is not in array

#### Undefined Operators

- `isUndefined` - Field is undefined
- `isDefined` - Field has a value

### Ordering

Specify sorting with the `orderBy` clause.

For single field sorting, use a tuple `[fieldName, direction]`:

```typescript
orderBy: ["name", "asc"];
```

For multiple field sorting, use an array of tuples `[[fieldName, direction], ...]`:

```typescript
orderBy: [
  ["age", "desc"], // Primary sort: age descending
  ["name", "asc"], // Secondary sort: name ascending
];
```

### Limiting Results

Use `limit` to control the maximum number of results returned:

```typescript
{
  limit: 10; // Return max 10 results
}
```

### Bulk Operations

Bulk operations allow you to update, replace, or delete multiple documents at once using the same filter patterns as queries.

#### Update Many

Update multiple documents that match the where clause:

```typescript
await client.database.users.updateMany({
  where: {
    age: { greaterThan: 18 },
    status: { equals: "active" },
  },
  fields: {
    isAdult: true,
  },
});
```

#### Replace Many

Replace multiple documents completely (all fields must be provided):

```typescript
await client.database.users.replaceMany({
  where: {
    status: { equals: "inactive" },
  },
  fields: {
    name: "Archived User",
    email: "archived@example.com",
    age: 0,
    status: "archived",
  },
});
```

#### Delete Many

Delete multiple documents that match the where clause:

```typescript
await client.database.users.deleteMany({
  where: {
    lastLogin: { lessThan: new Date("2023-01-01") },
  },
});
```

### Functional Updates

For `update`, `replace`, `updateMany`, and `replaceMany` operations, the `fields` parameter can be a function instead of an object. The function receives the previous document values and must return the new field values:

```typescript
// Single document update
await client.database.posts.update({
  id: postId,
  fields: (prev) => ({
    viewCount: prev.viewCount + 1, // Atomic increment
    lastViewed: new Date(), // Regular update
    featured: prev.viewCount > 1000, // Conditional logic
  }),
});

// Bulk update (function called for each matching document)
await client.database.users.updateMany({
  where: { status: "trial" },
  fields: (prev) => ({
    status: prev.loginCount > 5 ? "active" : "inactive",
    engagementScore: prev.engagementScore + 0.1,
  }),
});
```

**Key Benefits:**

- **Atomic operations** - Prevents race conditions between read and write
- **Type safety** - `prev` parameter is fully typed based on entity schema
- **Flexibility** - Supports any JavaScript logic for computing new values

### Pagination

Use `.paginated()` for large datasets that need to be displayed in pages:

```typescript
const paginatedUsers = client.database.users.paginated({
  fields: { name: true, email: true },
  where: { age: { greaterThan: 18 } },
  orderBy: [["name","asc"]],
  pageSize: 20, // Required for pagination
});

// Returns pagination controller with reactive state
paginatedUsers.subscribe((state) => {
  console.log("Current page data:", state.data); // Array of items for current page
  console.log("Page number:", state.currentPage);
  console.log(
    "Estimated totals:",
    state.estimatedTotalPages,
    state.estimatedTotalCount
  );
  console.log("Can navigate:", state.hasNext, state.hasPrevious);
});

// Navigation (instant due to prefetching)
await paginatedUsers.next();
await paginatedUsers.previous();

// PRELOAD PAGINATED data (typically server-side)
const { data: preloadedPagination } =
  await client.database.users.preloadPaginated({
    fields: {
      name: true,
      email: true,
      age: true,
    },
    where: { age: { greaterThan: 18 } },
    orderBy: [["name","asc"]],
    pageSize: 20,
  });

// Use preloaded pagination (typically client-side)
const preloadedPaginatedUsers =
  client.database.users.paginated(preloadedPagination);

// Subscribe to pagination state (no loading state - immediate data)
preloadedPaginatedUsers.subscribe((state) => {
  console.log("First page data:", state.data); // Immediate, no loading
  console.log("Navigation available:", state.hasNext, state.hasPrevious);
});
```

**Pagination Features:**

- **Automatic prefetching** - Next/previous pages preloaded for instant navigation
- **Real-time updates** - Pagination adjusts when underlying data changes
- **Estimated counts** - Provides estimates for total pages and items
- **Simple navigation** - Only next/previous, no arbitrary page jumping

### Preloading

Preloading allows you to fetch data before components render, eliminating loading states.

#### Preloading Methods

```typescript
// Preload multiple results
const { data: preloadedUsers, error } = await client.database.users.preload({
  fields: { name: true, email: true },
  where: { age: { greaterThan: 18 } },
  orderBy: ["name","asc"],
});

// Preload single result
const { data: preloadedUser, error } = await client.database.users.preloadOne({
  fields: { name: true, email: true },
  where: { id: { equals: userId } },
});

// Preload paginated results
const { data: preloadedPagination, error } =
  await client.database.users.preloadPaginated({
    fields: { name: true, email: true },
    where: { age: { greaterThan: 18 } },
    orderBy: ["name","asc"],
    pageSize: 20,
  });
```

#### Preload Data Type

```typescript
// Type for preloaded data (complete for now, may be extended in future)
type PreloadData<T> = {
  data: T;
  /* other fields we may eventually need for preloading */
};
```

#### Framework Hook Usage

```typescript
// Using preloaded data (no loading state):
const { data, error } = useQuery(preloadedUsers); // preloadedUsers is PreloadData<User[]>
const { data, error } = useQueryOne(preloadedUser); // preloadedUser is PreloadData<User | undefined>
const { data, currentPage, hasNext, hasPrevious, next, previous } =
  usePaginated(preloadedPagination); // preloadedPagination is PreloadData<PaginationState<User>>

// Direct queries (with loading state):
const { data, loading, error } = useQuery(client.database.users, {
  fields: { name: true },
  where: { age: { greaterThan: 18 } },
});
const {
  data,
  loading,
  error,
  currentPage,
  hasNext,
  hasPrevious,
  next,
  previous,
} = usePaginated(client.database.users, {
  fields: { name: true },
  where: { age: { greaterThan: 18 } },
  pageSize: 20,
});
```

**Preloading Behavior**: When using preloaded data, the hook starts with the preloaded data immediately (no loading state), then automatically fetches any updates that occurred since preloading and seamlessly updates the returned data to keep it current.

### Return Types

Most database operations return a consistent result structure:

```typescript
type DatabaseResult<T> =
  | {
      data: T;
      error: undefined;
    }
  | {
      data: undefined;
      error: DatabaseError;
    };
```

Here are the exact return types, where `Entity` is the shape of the data inferred from the `fields` field in the query:

#### Single Document Operations

- `entity.query()` returns `DatabaseResult<Entity[]>`
- `entity.queryOne()` returns `DatabaseResult<Entity | undefined>`
- `entity.paginated()` returns `PaginationController<Entity>`
- `entity.create()` returns `DatabaseResult<CompleteEntity>` (entire document with all fields including auto-generated ones)
- `entity.update()`, `entity.replace()`, `entity.delete()` return `DatabaseResult<void>`

#### Subscription Operations

- `entity.subscribe(queryOptions, callback)` returns `SubscriptionObject`
- `entity.subscribe(preloadedData, callback)` returns `SubscriptionObject`
- `entity.subscribeOne(queryOptions, callback)` returns `SubscriptionObject`
- `entity.subscribeOne(preloadedData, callback)` returns `SubscriptionObject`

Where `SubscriptionObject` provides:

- `getCurrentState()` returns `{ data: Entity[] | undefined, error: DatabaseError | undefined, loading: boolean }` for subscribe
- `getCurrentState()` returns `{ data: Entity | undefined, error: DatabaseError | undefined, loading: boolean }` for subscribeOne
- `unsubscribe()` stops the subscription

#### Pagination Operations

Where `PaginationController` provides:

- `subscribe(callback)` for reactive pagination state updates
- `next()` navigates to next page (instant due to prefetching)
- `previous()` navigates to previous page (instant due to prefetching)
- `getCurrentState()` returns `{ data: Entity[], currentPage: number, estimatedTotalPages: number, estimatedTotalCount: number, hasNext: boolean, hasPrevious: boolean, loading: boolean }`

#### Bulk Operations

- `entity.updateMany()` returns `DatabaseResult<{ count: number }>`
- `entity.replaceMany()` returns `DatabaseResult<{ count: number }>`
- `entity.deleteMany()` returns `DatabaseResult<{ count: number }>`

#### Batch Operations

- `client.database.batch.execute(operations)` returns `DatabaseResult<DatabaseResult[]>` (array of results in same order as operations)

#### Preloading Operations

- `entity.preload()` returns `DatabaseResult<PreloadData<Entity[]>>`
- `entity.preloadOne()` returns `DatabaseResult<PreloadData<Entity | undefined>>`
- `entity.preloadPaginated()` returns `DatabaseResult<PreloadData<PaginationState<Entity>>>`

## Framework Integrations

The core client provides one-time query execution and reactive subscriptions. Framework-specific wrappers use the `subscribe()` method to provide reactive data that automatically updates when the underlying data changes.

### React Integration

#### Option 1: With Loading States

```tsx
import { useQuery, useQueryOne, usePaginated } from "@sync-engine/react";
import { client } from "@/lib/sync-client";

function UserList() {
  // Reactive query - data updates automatically
  const {
    data: users,
    loading,
    error,
  } = useQuery(client.database.users, {
    fields: {
      name: true,
      email: true,
      age: true,
    },
    where: {
      age: { greaterThan: 18 },
    },
    orderBy: ["name","asc"],
  });

  if (loading) return <div>Loading users...</div>;
  if (error) return <div>Error: {error.message}</div>;

  return (
    <ul>
      {users.map((user) => (
        <li key={user.id}>
          {user.name} ({user.email}) - Age: {user.age}
        </li>
      ))}
    </ul>
  );
}

function PaginatedUserList() {
  // Reactive paginated query
  const {
    data: users,
    currentPage,
    estimatedTotalPages,
    hasNext,
    hasPrevious,
    next,
    previous,
    loading,
    error,
  } = usePaginated(client.database.users, {
    fields: {
      name: true,
      email: true,
      age: true,
    },
    where: {
      age: { greaterThan: 18 },
    },
    orderBy: ["name","asc"],
    pageSize: 20,
  });

  if (loading) return <div>Loading users...</div>;
  if (error) return <div>Error: {error.message}</div>;

  return (
    <div>
      <ul>
        {users.map((user) => (
          <li key={user.id}>
            {user.name} ({user.email}) - Age: {user.age}
          </li>
        ))}
      </ul>
      <div>
        <button onClick={previous} disabled={!hasPrevious}>
          Previous
        </button>
        <span>
          Page {currentPage} of ~{estimatedTotalPages}
        </span>
        <button onClick={next} disabled={!hasNext}>
          Next
        </button>
      </div>
    </div>
  );
}
```

#### Option 2: No Loading States (Preloaded Data)

```tsx
// app/users/page.tsx (Next.js App Router)
import { client } from "@/lib/sync-client";
import { UserListClient } from "./UserListClient";

export default async function UsersPage() {
  // Preload data - define query once
  const { data: preloadedUsers, error } = await client.database.users.preload({
    fields: {
      name: true,
      email: true,
      age: true,
    },
    where: {
      age: { greaterThan: 18 },
    },
    orderBy: ["name","asc"],
  });

  if (error) {
    // Handle preload error
    return <div>Failed to load users: {error.message}</div>;
  }

  return <UserListClient preloadedUsers={preloadedUsers} />;
}
```

```tsx
// app/users/UserListClient.tsx
"use client";
import { useQuery } from "@sync-engine/react";
import type { PreloadData } from "@sync-engine/core";

interface Props {
  preloadedUsers: PreloadData<User[]>;
}

export function UserListClient({ preloadedUsers }: Props) {
  // Reactive query using preloaded data - no duplication, no loading state!
  const { data: users, error } = useQuery(preloadedUsers);

  if (error) return <div>Error: {error.message}</div>;

  return (
    <ul>
      {users.map((user) => (
        <li key={user.id}>
          {user.name} ({user.email}) - Age: {user.age}
        </li>
      ))}
    </ul>
  );
}
```

```tsx
// app/users/paginated/page.tsx (Next.js App Router)
import { client } from "@/lib/sync-client";
import { PaginatedUserListClient } from "./PaginatedUserListClient";

export default async function PaginatedUsersPage() {
  // Preload paginated data - define query once
  const { data: preloadedPagination, error } =
    await client.database.users.preloadPaginated({
      fields: {
        name: true,
        email: true,
        age: true,
      },
      where: {
        age: { greaterThan: 18 },
      },
      orderBy: ["name","asc"],
      pageSize: 20,
    });

  if (error) {
    return <div>Failed to load users: {error.message}</div>;
  }

  return <PaginatedUserListClient preloadedPagination={preloadedPagination} />;
}
```

```tsx
// app/users/paginated/PaginatedUserListClient.tsx
"use client";
import { usePaginated } from "@sync-engine/react";
import type { PreloadData, PaginationState } from "@sync-engine/core";

interface Props {
  preloadedPagination: PreloadData<PaginationState<User>>;
}

export function PaginatedUserListClient({ preloadedPagination }: Props) {
  // Reactive pagination using preloaded data - no loading state!
  const {
    data: users,
    currentPage,
    estimatedTotalPages,
    hasNext,
    hasPrevious,
    next,
    previous,
    error,
  } = usePaginated(preloadedPagination);

  if (error) return <div>Error: {error.message}</div>;

  return (
    <div>
      <ul>
        {users.map((user) => (
          <li key={user.id}>
            {user.name} ({user.email}) - Age: {user.age}
          </li>
        ))}
      </ul>
      <div>
        <button onClick={previous} disabled={!hasPrevious}>
          Previous
        </button>
        <span>
          Page {currentPage} of ~{estimatedTotalPages}
        </span>
        <button onClick={next} disabled={!hasNext}>
          Next
        </button>
      </div>
    </div>
  );
}
```

### Svelte Integration

```typescript
// src/routes/users/+page.server.ts
import type { PageServerLoad } from "./$types";
import { client } from "$lib/sync-client";

export const load: PageServerLoad = async () => {
  // Preload data - define query once
  const { data: preloadedUsers, error } = await client.database.users.preload({
    fields: {
      name: true,
      email: true,
      age: true,
    },
    where: {
      age: { greaterThan: 18 },
    },
    orderBy: ["name","asc"],
  });

  // Preload paginated data
  const { data: preloadedPagination, error: paginationError } =
    await client.database.users.preloadPaginated({
      fields: {
        name: true,
        email: true,
        age: true,
      },
      where: {
        age: { greaterThan: 18 },
      },
      orderBy: ["name","asc"],
      pageSize: 20,
    });

  return {
    preloadedUsers,
    preloadedPagination,
  };
};
```

```svelte
<!-- src/routes/users/+page.svelte -->
<script lang="ts">
  import { query, paginated } from '@sync-engine/svelte'
  import { client } from '$lib/sync-client'
  import type { PageData } from './$types'

  interface Props {
    data: PageData
  }

  let { data }: Props = $props()

  // Option 1: Reactive query using preloaded data - no loading state!
  const users = query(data.preloadedUsers)

  // Option 2: Direct query with loading state
  // const users = query(client.database.users, {
  //   fields: { name: true, email: true, age: true },
  //   where: { age: { greaterThan: 18 } }
  // })

  // Option 3: Paginated query (with loading state)
  const paginatedUsers = paginated(client.database.users, {
    fields: { name: true, email: true, age: true },
    where: { age: { greaterThan: 18 } },
    orderBy: ['name', 'asc'],
    pageSize: 20
  })

  // Option 4: Preloaded paginated query (no loading state)
  const preloadedPaginatedUsers = paginated(data.preloadedPagination)
</script>

<div class="user-list">
  {#if users.error}
    <p>Error: {users.error.message}</p>
  {:else}
    <ul>
      {#each users.data as user (user.id)}
        <li>
          {user.name} ({user.email}) - Age: {user.age}
        </li>
      {/each}
    </ul>
  {/if}
</div>

<div class="paginated-user-list">
  {#if paginatedUsers.error}
    <p>Error: {paginatedUsers.error.message}</p>
  {:else if paginatedUsers.loading}
    <p>Loading users...</p>
  {:else}
    <ul>
      {#each paginatedUsers.data as user (user.id)}
        <li>
          {user.name} ({user.email}) - Age: {user.age}
        </li>
      {/each}
    </ul>
    <div class="pagination">
      <button onclick={paginatedUsers.previous} disabled={!paginatedUsers.hasPrevious}>
        Previous
      </button>
      <span>
        Page {paginatedUsers.currentPage} of ~{paginatedUsers.estimatedTotalPages}
      </span>
      <button onclick={paginatedUsers.next} disabled={!paginatedUsers.hasNext}>
        Next
      </button>
    </div>
  {/if}
</div>

<div class="preloaded-paginated-user-list">
  {#if preloadedPaginatedUsers.error}
    <p>Error: {preloadedPaginatedUsers.error.message}</p>
  {:else}
    <ul>
      {#each preloadedPaginatedUsers.data as user (user.id)}
        <li>
          {user.name} ({user.email}) - Age: {user.age}
        </li>
      {/each}
    </ul>
    <div class="pagination">
      <button onclick={preloadedPaginatedUsers.previous} disabled={!preloadedPaginatedUsers.hasPrevious}>
        Previous
      </button>
      <span>
        Page {preloadedPaginatedUsers.currentPage} of ~{preloadedPaginatedUsers.estimatedTotalPages}
      </span>
      <button onclick={preloadedPaginatedUsers.next} disabled={!preloadedPaginatedUsers.hasNext}>
        Next
      </button>
    </div>
  {/if}
</div>
```

```typescript
// src/routes/users/[id]/+page.server.ts
import type { PageServerLoad } from "./$types";
import { client } from "$lib/sync-client";

export const load: PageServerLoad = async ({ params }) => {
  const { data: preloadedUser, error } = await client.database.users.preloadOne(
    {
      fields: {
        name: true,
        email: true,
        posts: {
          title: true,
          createdAt: true,
        },
      },
      where: {
        id: { equals: params.id },
      },
    }
  );

  return {
    preloadedUser,
  };
};
```

```svelte
<!-- src/routes/users/[id]/+page.svelte -->
<script lang="ts">
  import { queryOne } from '@sync-engine/svelte'
  import type { PageData } from './$types'

  interface Props {
    data: PageData
  }

  let { data }: Props = $props()

  // Reactive single query using preloaded data - no duplication!
  const user = queryOne(data.preloadedUser)
</script>

{#if user.error}
  <p>Error: {user.error.message}</p>
{:else if user.data}
  <div class="user-profile">
    <h1>{user.data.name}</h1>
    <p>{user.data.email}</p>
    <h2>Posts</h2>
    {#each user.data.posts as post (post.id)}
      <article>
        <h3>{post.title}</h3>
        <time>{post.createdAt.toDateString()}</time>
      </article>
    {/each}
  </div>
  {:else}
    <p>User not found</p>
  {/if}
```

#### Rooms Integration

```tsx
// React - Real-time collaboration
import { useRoom } from "@sync-engine/react";

function DocumentEditor() {
  const {
    // Actions (imperative)
    emit,
    set,
    setUserStatus,

    // State (reactive)
    roomStatus,
    userStatuses,

    // Event listeners
    on,

    // Meta
    isConnected,
    error,
  } = useRoom(client.rooms.documentEditor, "doc-123");

  // Set up event handling
  on("like", (data, fromUserId) => {
    showToast(`${fromUserId} liked: ${data.targetId}`);
  });

  on("celebration", (data, fromUserId) => {
    showConfetti(data.x, data.y);
  });

  const handleMouseMove = (e: MouseEvent) => {
    setUserStatus("cursor", { x: e.clientX, y: e.clientY });
  };

  if (!isConnected) {
    return <div>Connecting to room... {error && `Error: ${error}`}</div>;
  }

  return (
    <div onMouseMove={handleMouseMove}>
      <h1>{roomStatus.documentTitle}</h1>
      <p>Collaborators: {roomStatus.collaboratorCount}</p>

      {/* Render other users' cursors */}
      {Object.entries(userStatuses)
        .filter(([userId]) => userId !== currentUser.id)
        .map(([userId, status]) => (
          <Cursor
            key={userId}
            x={status.cursor.x}
            y={status.cursor.y}
            isTyping={status.isTyping}
          />
        ))}

      <button
        onClick={() =>
          emit("like", { targetId: "document", userId: currentUser.id })
        }
      >
        Like Document
      </button>
    </div>
  );
}
```

```svelte
<!-- Svelte - Real-time collaboration -->
<script lang="ts">
  import { useRoom } from '@sync-engine/svelte'

    const {
    emit,
    set,
    setUserStatus,
    roomStatus,
    userStatuses,
    on,
    isConnected,
    error
  } = useRoom(client.rooms.documentEditor, 'doc-123')

  // Event handling
  on('like', (data, fromUserId) => {
    showToast(`${fromUserId} liked: ${data.targetId}`)
  })

  on('celebration', (data, fromUserId) => {
    showConfetti(data.x, data.y)
  })

  const handleMouseMove = (e) => {
    setUserStatus('cursor', { x: e.clientX, y: e.clientY })
  }
</script>

{#if !isConnected}
  <div>Connecting to room... {error ? `Error: ${error}` : ''}</div>
{:else}
  <div on:mousemove={handleMouseMove}>
    <h1>{roomStatus.documentTitle}</h1>
    <p>Collaborators: {roomStatus.collaboratorCount}</p>

    <!-- Render other users' cursors -->
    {#each Object.entries(userStatuses).filter(([userId]) => userId !== currentUser.id) as [userId, status] (userId)}
      <Cursor
        x={status.cursor.x}
        y={status.cursor.y}
        isTyping={status.isTyping}
      />
    {/each}

    <button on:click={() => emit('like', { targetId: 'document', userId: currentUser.id })}>
      Like Document
    </button>
  </div>
{/if}
```
