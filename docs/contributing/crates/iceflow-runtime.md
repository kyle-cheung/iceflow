# iceflow-runtime

## Purpose

`iceflow-runtime` owns local coordination decisions: intake admission, backpressure, unresolved-commit blocking, and checkpoint gating. It is deliberately small and should stay focused on sequencing policy rather than business logic from other crates.

## Owns

- table-level intake and checkpoint decisions
- recovery queue saturation behavior
- in-memory and durable-pending batch tracking
- failpoints used to exercise runtime behavior under stress

## Key Files

- `crates/iceflow-runtime/src/lib.rs`
- `crates/iceflow-runtime/src/backpressure.rs`
- `crates/iceflow-runtime/src/pipeline.rs`
- `crates/iceflow-runtime/src/failpoints.rs`
- `crates/iceflow-runtime/tests/backpressure.rs`

## Change Here When

- table admission rules change
- checkpoint advancement policy changes
- recovery queue throttling changes
- a runtime-only sequencing regression appears without a state-store or sink contract change

## Avoid

- moving durable persistence into the runtime crate
- embedding source parsing or sink commit logic here
- letting failpoints become the only way a behavior is expressed or tested

## Tests To Run

- `cargo test -p iceflow-runtime`
- `cargo test -p iceflow-runtime --test backpressure`

## Related Crates

- `iceflow-types` provides the table identifiers used here
- `iceflow-state` and `iceflow-sink` drive the external conditions this crate reacts to
- `iceflow-cli` currently coordinates this crate during command execution
