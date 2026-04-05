# iceflow-cli

## Purpose

`iceflow-cli` is the operator-facing entrypoint crate. It parses subcommands, owns the config-driven source/destination/catalog/connector model, wires the source, worker, state, sink, and runtime crates together, and exposes the current `source check`, `connector check`, `connector run`, legacy `run`, and `compact` flows. `run` remains the deterministic reference-workload path; `connector run` is the additive config-driven path that future real connectors will use. Successful end-to-end execution is still strongest for append-only sinks; `compact` is append-only only today.

## Owns

- top-level command parsing and `run_env`
- config loading and factory wiring for source, destination, optional catalog, and connector files
- the `source check` and `connector check` validation flows
- the `connector run` orchestration flow for config-driven replication
- the legacy `run` orchestration flow for the current reference workload names
- the `compact` command and related catalog/history helpers
- end-to-end command tests for config-driven and reference-workload local flows

## Key Files

- `crates/iceflow-cli/src/lib.rs`
- `crates/iceflow-cli/src/main.rs`
- `crates/iceflow-cli/src/config/`
- `crates/iceflow-cli/src/commands/source_cmd.rs`
- `crates/iceflow-cli/src/commands/connector_cmd.rs`
- `crates/iceflow-cli/src/commands/run.rs`
- `crates/iceflow-cli/src/commands/compact.rs`
- `crates/iceflow-cli/src/commands/compact_catalog.rs`
- `crates/iceflow-cli/src/commands/compact_history.rs`
- `crates/iceflow-cli/tests/config_loading.rs`
- `crates/iceflow-cli/tests/source_check.rs`
- `crates/iceflow-cli/tests/connector_check.rs`
- `crates/iceflow-cli/tests/connector_run.rs`
- `crates/iceflow-cli/tests/run.rs`
- `crates/iceflow-cli/tests/compact.rs`

## Change Here When

- a user-facing command contract changes
- a config surface under `sources/`, `destinations/`, `catalogs/`, or `connectors/` changes
- command wiring is required across the config layer and the runtime path
- new command wiring is required across crates
- `compact` acceptance changes or Polaris auth wiring changes
- CLI-backed Polaris paths still work only against unauthenticated endpoints today
- integration logic belongs at the executable boundary instead of a library crate
- an end-to-end local flow fails even though crate-local contracts still pass

## Avoid

- moving shared domain logic into the CLI just because multiple commands use it
- introducing new sink or state semantics here that should live in library crates
- treating config parsing as a thin afterthought when the command contract changed
- letting tests cover only parsing when the integrated flow changed

## Tests To Run

- `cargo test -p iceflow-cli`
- `cargo test -p iceflow-cli --test config_loading`
- `cargo test -p iceflow-cli --test source_check`
- `cargo test -p iceflow-cli --test connector_check`
- `cargo test -p iceflow-cli --test connector_run`
- `cargo test -p iceflow-cli --test run`
- `cargo test -p iceflow-cli --test compact`

`cargo test -p iceflow-cli` exercises a mock Polaris server in the compact tests, so restricted sandboxes need local loopback bind permission for the full package suite.

## Related Crates

- `iceflow-source`, `iceflow-worker-duckdb`, `iceflow-state`, `iceflow-sink`, and `iceflow-runtime` are all composed here
- `fixtures/config_samples/` is the sample config root for the config-driven CLI surface
- `fixtures/reference_workload_v0/` still backs the legacy `run` path and the routed file-source tests
- CLI end-to-end coverage is currently strongest for append-only flows
- changes here often require confirming the crate-local guides still point to the right ownership boundaries
