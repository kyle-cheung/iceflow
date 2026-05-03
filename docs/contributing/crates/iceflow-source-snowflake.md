# iceflow-source-snowflake

## Purpose

`iceflow-source-snowflake` owns Snowflake source config parsing, single-table v1 binding validation, managed stream bootstrap and resume behavior, one-batch snapshot capture, incremental `CHANGES` capture, and Snowflake checkpoint token encoding.

## Key Files

- `crates/iceflow-source-snowflake/src/lib.rs`
- `crates/iceflow-source-snowflake/src/checkpoint.rs`
- `crates/iceflow-source-snowflake/src/config.rs`
- `crates/iceflow-source-snowflake/src/binding.rs`
- `crates/iceflow-source-snowflake/src/client.rs`
- `crates/iceflow-source-snowflake/src/metadata.rs`
- `crates/iceflow-source-snowflake/src/session.rs`
- `crates/iceflow-source-snowflake/src/value.rs`

## Binding and Validation Notes

- Snowflake v1 binds exactly one selected table and requires `table_mode = "append_only"`.
- The managed stream name is connector/schema/table scoped. It lowercases and sanitizes each component to `[a-z0-9_]`, caps the identifier at Snowflake's 255-character limit, and keeps a stable hash suffix so truncation still carries a deterministic suffix that reduces collision risk.
- Table metadata must expose a declared primary key and at least one described column.
- Current v1 configs and examples assume unquoted uppercase Snowflake object names. Full case-sensitive quoted identifier grant parsing remains future work.
- Stream bootstrap validation is side-effect-free. It requires database `USAGE` or `OWNERSHIP`, schema `CREATE STREAM` or `OWNERSHIP`, and table `SELECT` or `OWNERSHIP`.
- Effective grants are resolved across inherited account roles and database roles with cycle protection.
- The grant check cannot prove change tracking is already enabled unless the table privilege is `OWNERSHIP`; if change tracking is off, non-owners may still need an operator to enable it before the first managed stream is created.

## Capture Notes

- Snapshot checkpoints encode as `snowflake:v1:snapshot:<query_id>`. Incremental checkpoints encode as `snowflake:v1:stream:<query_id>`.
- Fresh bootstrap recreates the managed stream on the bound table, then snapshots with `AT (STREAM => ...)`.
- The first incremental after that snapshot reads `CHANGES(INFORMATION => DEFAULT) AT (STREAM => ...) END(STATEMENT => ...)`, using the managed stream as the start boundary and a fresh statement boundary captured when the incremental poll runs as the end boundary.
- Later incrementals move statement-to-statement with `AT (STATEMENT => ...) END(STATEMENT => ...)`.
- Snapshot resume skips stream recreation and assumes the managed stream at that name still points at the original anchor. If it was dropped or replaced, operators must rebootstrap.
- Stream resume recreates the managed stream at the saved statement boundary before polling more changes.
- Incremental capture rechecks the table schema fingerprint. Schema drift fails closed with `Snowflake schema drift detected; rebootstrap required`.
- The source emits append-only mutation-log records. Snowflake `Insert`/`Upsert`/`Delete` semantics are preserved in that log, but current real sinks still do not converge mutable row state.

## Row Shape and Auth Notes

- Snapshot and incremental queries project source columns through `TO_VARCHAR(...)` before crossing the v1 `RowSet` boundary. Incremental queries do the same for `METADATA$ROW_ID`, `METADATA$ACTION`, and `METADATA$ISUPDATE`.
- The only accepted config value today is `auth_method = "password"`. Config parsing still allows an empty `password`; in practice that is intended for externally injected ADBC auth.
- `source check` and `connector check` warn when `password` is empty unless the external ADBC JWT auth environment is already present with `ADBC_SNOWFLAKE_SQL_AUTH_TYPE=auth_jwt` and at least one supported JWT private-key env var.
- Auth behavior intentionally tolerates externally injected ADBC/JWT environment credentials in addition to password-configured auth. Explicit key-pair/JWT config mode remains future work.

## Tests To Run

- `cargo test -p iceflow-source-snowflake`
- `cargo clippy -p iceflow-source-snowflake --tests -- -D warnings`
- `cargo test -p iceflow-source-snowflake --test boundary_probe -- --ignored`
- `cargo test -p iceflow-source-snowflake --test live_capture -- --ignored`
- `cargo test -p iceflow-cli --test source_check --test connector_check`
- `cargo test -p iceflow-cli --test snowflake_connector_live -- --ignored`
