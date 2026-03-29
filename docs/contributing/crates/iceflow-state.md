# iceflow-state

## Purpose

`iceflow-state` is the durable SQLite-backed control plane. It owns persisted batch, attempt, and checkpoint state plus recovery and orphan bookkeeping.

## Owns

- the `StateStore` trait and checkpoint/commit state types
- SQLite-backed persistence and test store behavior
- schema migrations for the state database
- recovery candidate and orphan cleanup queries
- query-oriented reconcile and quarantine support

## Key Files

- `crates/iceflow-state/src/lib.rs`
- `crates/iceflow-state/src/sqlite.rs`
- `crates/iceflow-state/src/migrations.rs`
- `crates/iceflow-state/src/recovery.rs`
- `crates/iceflow-state/src/reconcile.rs`
- `crates/iceflow-state/tests/recovery.rs`
- `crates/iceflow-state/tests/reconcile.rs`

## Change Here When

- batch lifecycle states change
- commit-attempt resolution or checkpoint durability rules change
- a migration is required for persisted state
- recovery and orphan-cleanup behavior changes

## Avoid

- putting sink-specific remote lookup logic in the state store
- letting sink implementations update attempt state directly instead of the orchestrator
- bypassing explicit batch and attempt status transitions
- changing persistence semantics without matching recovery tests

## Tests To Run

- `cargo test -p iceflow-state`
- `cargo test -p iceflow-state --test recovery`
- `cargo test -p iceflow-state --test reconcile`

## Related Crates

- `iceflow-worker-duckdb` produces files that get registered here
- `iceflow-sink` consumes commit requests while the orchestrator records attempt outcomes here
- `iceflow-runtime` reacts to unresolved and durable batch state tracked here
- `iceflow-cli` is the current main caller of these transitions
