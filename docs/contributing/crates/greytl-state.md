# greytl-state

## Purpose

`greytl-state` is the durable SQLite-backed control plane. It owns persisted batch, attempt, and checkpoint state plus recovery and orphan bookkeeping.

## Owns

- the `StateStore` trait and checkpoint/commit state types
- SQLite-backed persistence and test store behavior
- schema migrations for the state database
- recovery candidate and orphan cleanup queries
- query-oriented reconcile and quarantine support

## Key Files

- `crates/greytl-state/src/lib.rs`
- `crates/greytl-state/src/sqlite.rs`
- `crates/greytl-state/src/migrations.rs`
- `crates/greytl-state/src/recovery.rs`
- `crates/greytl-state/src/reconcile.rs`
- `crates/greytl-state/tests/recovery.rs`
- `crates/greytl-state/tests/reconcile.rs`

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

- `cargo test -p greytl-state`
- `cargo test -p greytl-state --test recovery`
- `cargo test -p greytl-state --test reconcile`

## Related Crates

- `greytl-worker-duckdb` produces files that get registered here
- `greytl-sink` consumes commit requests while the orchestrator records attempt outcomes here
- `greytl-runtime` reacts to unresolved and durable batch state tracked here
- `greytl-cli` is the current main caller of these transitions
