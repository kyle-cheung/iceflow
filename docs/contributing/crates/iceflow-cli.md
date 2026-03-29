# iceflow-cli

## Purpose

`iceflow-cli` is the operator-facing entrypoint crate. It parses subcommands, wires the source, worker, state, sink, and runtime crates together, and exposes the current `run` and `compact` flows for the reference workloads. `run` knows about the append-only and keyed-upsert reference workload names, but successful end-to-end execution is still limited by real sink support, which is append-only only today; `compact` is append-only only today.

## Owns

- top-level command parsing and `run_env`
- the `run` orchestration flow for the current reference workload names
- the `compact` command and related catalog/history helpers
- end-to-end command tests that are strongest for append-only local flows

## Key Files

- `crates/iceflow-cli/src/lib.rs`
- `crates/iceflow-cli/src/main.rs`
- `crates/iceflow-cli/src/commands/run.rs`
- `crates/iceflow-cli/src/commands/compact.rs`
- `crates/iceflow-cli/src/commands/compact_catalog.rs`
- `crates/iceflow-cli/src/commands/compact_history.rs`
- `crates/iceflow-cli/tests/run.rs`
- `crates/iceflow-cli/tests/compact.rs`

## Change Here When

- a user-facing command contract changes
- new command wiring is required across crates
- `compact` acceptance changes or Polaris auth wiring changes
- CLI-backed Polaris paths still work only against unauthenticated endpoints today
- integration logic belongs at the executable boundary instead of a library crate
- an end-to-end local flow fails even though crate-local contracts still pass

## Avoid

- moving shared domain logic into the CLI just because multiple commands use it
- introducing new sink or state semantics here that should live in library crates
- letting tests cover only parsing when the integrated flow changed

## Tests To Run

- `cargo test -p iceflow-cli`
- `cargo test -p iceflow-cli --test run`
- `cargo test -p iceflow-cli --test compact`

## Related Crates

- `iceflow-source`, `iceflow-worker-duckdb`, `iceflow-state`, `iceflow-sink`, and `iceflow-runtime` are all composed here
- CLI end-to-end coverage is currently strongest for append-only flows
- changes here often require confirming the crate-local guides still point to the right ownership boundaries
