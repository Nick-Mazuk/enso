## About

Enso is a fast, relational sync engine for delightful front-end web apps.

- Reactive updates
- Instant writes and reads
- End-to-end type safety
- No schema migrations
- Drizzle-like relational queries
- Lightweight (<10kb compressed)

## Code style

Safety:

- Design by contract: on all functions / classes, document pre-conditions, post-conditions, and invariants.
- Assertions detect programmer errors. Unlike operating errors, which are expected and which must be handled, assertion failures are unexpected. The only correct way to handle corrupt code is to crash. Assertions downgrade catastrophic correctness bugs into liveness bugs.
- Use assertions (from `packages/shared/assert.ts`) to assert all function arguments and return values, pre/postconditions, and invariants.
- Split compound assertions: prefer `assert(a); assert(b);` over `assert(a and b);`. The former is simpler to read, and provides more precise information if the condition fails.
- Pair assertions. For every property you want to enforce, try to find at least two different code paths where an assertion can be added. For example, assert validity of data right before writing it to disk, and also immediately after reading from disk.

Testing:

- Always include tests for new features or bug fixes.
- Test with invalid data and states, not just the happy path.

Simplicity:

- Minimize dependencies. Do not add new dependencies without approval.

## Repository structure

Important files / directories:

- `.github/workflows`: GitHub actions configuration.
- `package.json`: dependencies and scripts.
- `docs/client-api.md`: documents how developers will use the library.
- `docs/design.md`: documents the project's system design.
- `packages/client`: the code that runs on the client (web browsers).
- `packages/shared`: code shared across multiple other packages.

## Scripts

For JavaScript / TypeScript, always use `bun`. Never use `npm`.

- `bun install`: install dependencies
- `bun test`: run all tests
- `bun check`: lint and check formatting
- `bun format`: format code
- `bun typecheck`: check TypeScript types
