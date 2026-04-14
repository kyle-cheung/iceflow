# iceflow-source-snowflake

## Purpose

`iceflow-source-snowflake` owns Snowflake source config parsing, connector-bound table validation, managed stream bootstrap, one-batch snapshot capture, incremental `CHANGES` capture, and Snowflake checkpoint token encoding.

## Key Files

- `crates/iceflow-source-snowflake/src/lib.rs`
- `crates/iceflow-source-snowflake/src/config.rs`
- `crates/iceflow-source-snowflake/src/binding.rs`
- `crates/iceflow-source-snowflake/src/client.rs`
- `crates/iceflow-source-snowflake/src/metadata.rs`
- `crates/iceflow-source-snowflake/src/session.rs`
- `crates/iceflow-source-snowflake/src/value.rs`

## Capture Notes

- Fresh bootstrap recreates the managed stream and snapshots with `AT (STREAM => ...)`, so the managed stream anchor is driven by the bootstrap statement itself.
- The first incremental after that snapshot begins from the managed stream anchor and is bounded by the durable statement boundary that produced the snapshot, while later incrementals move statement-to-statement.
- Incremental capture reads `CHANGES(INFORMATION => DEFAULT)` between durable statement checkpoints.
- The source emits append-only mutation-log records in this repo version. Snowflake update/delete metadata is preserved, but current real sinks do not yet converge mutable row state.
- ADBC row results are projected through `TO_VARCHAR(...)` in Snowflake SQL before entering the v1 row-oriented `RowSet` boundary.
- Auth behavior intentionally tolerates externally injected ADBC/JWT environment credentials in addition to password-configured auth; explicit key-pair/JWT config mode remains future work.

## Tests To Run

- `cargo test -p iceflow-source-snowflake`
- `cargo clippy -p iceflow-source-snowflake --tests -- -D warnings`
- `cargo test -p iceflow-source-snowflake --test boundary_probe -- --ignored`
- `cargo test -p iceflow-source-snowflake --test live_capture -- --ignored`
- `cargo test -p iceflow-cli --test snowflake_connector_live -- --ignored`
