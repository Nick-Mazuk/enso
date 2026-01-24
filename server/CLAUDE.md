This server is a sync engine for 

Code style:

- Use explicit imports instead of wildcard imports
- Run `cargo fmt` after Rust code changes
- Validate all Rust code changes with `cargo test`, `cargo check`, and `cargo clippy`
- Place end-to-end tests in `src/e2e_tests/` with one test file per test case
- Always write an end-to-end test for new or modified API behavior
- Do not ingore doc tests

Documentation:

- Storage spec: @docs/storage-engine.md
- API protocol spec: @docs/protocol.md
- API protocol: @proto/protocol.proto
