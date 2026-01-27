This server is a sync engine

Code style:

- Use explicit imports instead of wildcard imports
- Run `cargo fmt` after Rust code changes
- Validate all Rust code changes with `cargo test`, `cargo check`, and `cargo clippy`
- Place end-to-end tests in `src/e2e_tests/` with one test file per test case
- Always write an end-to-end test for new or modified API behavior
- Do not ingore doc tests
- Put all protobuf serialization / deserialization code in @server/src/types and use the `ProtoSerializable` and `ProtoDeserializable` traits
- NEVER surpress a clippy finding that panics (e.g., never add `#[allow(clippy::expect_used)]`).
- Use type driven design and the type-state pattern.
- Avoid extra memory allocation.

Design constraints:

- Optimize for having thousands of database files. Keep memory usage low.
- Assume no long-running processes for the database. Databases may be evicted from memory anytime when not in use.
- Keep database open and close as fast as possible.

Documentation:

- Storage spec: @docs/storage-engine.md
- API protocol spec: @docs/protocol.md
- API protocol: @proto/protocol.proto
