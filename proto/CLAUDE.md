## API styleguide

Follow Google's AIPs when possible (not AIP is the correct spelling and not a typo of API). Here's a summary of the most important guidelines:

- Naming conventions
    - **Field Names:** Use `lower_snake_case`.
        - Avoid prepositions (e.g., use `error_reason`, not `reason_for_error`).
        - Use recognized abbreviations (e.g., `config`, `spec`, `stats`).
        - Fields representing URIs should be named `uri`; use `url` only if strictly limited to URLs.
    - **Messages & RPCs:** Use `UpperCamelCase`.
    - **Units:** Include units in the field name for quantities (e.g., `distance_km`, `duration_seconds`).
    - **Compound Units**: Use plural forms and underscores if units are unabbreviated (e.g., `energy_kwh` or `energy_kw_fortnights`). For inverse units, use `per` (e.g., `speed_miles_per_hour` rather than `speed_mph`).
    - **Counts:** Use the suffix `_count` for quantities of items (e.g., `node_count`), not `num_nodes`.
    - **Booleans**: Do not use the `is_` prefix. Use the adjective or noun directly (e.g., `disabled` instead of `is_disabled`, `required` instead of `is_required`). Exception: You may use `is_` if the name conflicts with a reserved keyword (e.g., `is_new` is allowed to avoid the keyword `new`).
- Data types and fields
    - **Half-Open ranges (standard)**: Use `start_` and `end_` prefixes (e.g., `start_page`, `end_page`).
    - **Closed ranges (dates/days)**: Use `first_` and `last_` prefixes (e.g., `first_day`, `last_day`) if the range includes the final value.
    - **Enums:**
        - Must use `UPPER_SNAKE_CASE` values.
        - The 0 value must be `ENUM_NAME_UNSPECIFIED`.
        - For "states", use a `State` enum, not `Status`.
    - **Time:**
        - Use `google.protobuf.Timestamp` for points in time (suffix `_time`, e.g., `create_time`).
        - Use `google.protobuf.Duration` for spans of time.
        - Do not use the past tense. For instance, `publish_time` instead of `published_time`.
    - **Standardized Codes:** Use `region_code` (CLDR), `language_code` (BCP-47), and `currency_code` (ISO-4217). Suffix all codes with `_code`.
    - **Server-Modified Values**: If a field represents a server-calculated value derived from a user's input, use the `effective_` prefix (e.g., `effective_ip_address`).
- Errors
    - - **Format:** Return `google.rpc.Status`. [48]
    - **Details:** Must include `google.rpc.ErrorInfo` in the `details` field for machine-readable identification (domain/reason).
    - **Localization:** Use `LocalizedMessage` for user-facing, localized error text.
- Documentation and structure
    - **Versioning:** Do not introduce breaking changes (removing fields, changing IDs)
    - **Comments:** Must be present on every component (service, message, field). Use third-person present tense (e.g., "Creates a book...").
    - **Ordering:** Service definitions first, then messages (Request, then Response).
    
## Code changes

After all protobuf code changes, run the following:

- `bun proto-gen` for protobuf codegen
- `buf lint` to enforce code style, ignore any lint errors from `proto/google/`
- All commands listed in @client/CLAUDE.md to verify the TypeScript code still works
- All commands listed in @server/CLAUDE.md to verify the Rust code still works

## External documentation

Reference the following documentation if you're unsure about any specifics:

- Protocol buffers: https://protobuf.dev/
- Google's AIPs: https://aip.dev/
- Buf's lint rules: https://buf.build/docs/lint/rules/
