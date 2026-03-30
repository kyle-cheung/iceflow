# iceflow-types

## Purpose

`iceflow-types` is the shared vocabulary crate for the workspace. If multiple crates need to agree on an identifier, manifest shape, logical mutation contract, schema policy rule, or shared reference workload metadata, it belongs here.

## Owns

- table, batch, checkpoint, and commit identifier types
- logical mutation modeling and validation
- batch manifest and manifest file contracts
- schema policy evaluation inputs and outputs
- static shared reference workload descriptors / metadata

## Key Files

- `crates/iceflow-types/src/ids.rs`
- `crates/iceflow-types/src/mutation.rs`
- `crates/iceflow-types/src/manifest.rs`
- `crates/iceflow-types/src/schema_policy.rs`
- `crates/iceflow-types/src/reference_workload.rs`
- `crates/iceflow-types/tests/canonical_model.rs`
- `crates/iceflow-types/tests/manifest_contract.rs`

## Change Here When

- a new crate needs a shared domain type instead of a local private struct
- source, worker, state, sink, and runtime need to agree on a changed batch or mutation contract
- manifest semantics or schema policy rules change
- shared reference workload descriptors or metadata change

## Avoid

- putting runtime orchestration or sink-specific behavior here
- adding helper logic that only one downstream crate needs
- duplicating types across crates when the contract is part of the shared engine vocabulary

## Tests To Run

- `cargo test -p iceflow-types`
- `cargo test -p iceflow-types --test canonical_model`
- `cargo test -p iceflow-types --test manifest_contract`

## Related Crates

- `iceflow-source` consumes the shared mutation and batch contract types
- `iceflow-worker-duckdb` materializes batches described by these types
- `iceflow-state` persists manifest and checkpoint data shaped here
- `iceflow-sink` commits manifests built from these contracts
- `iceflow-runtime` uses shared table identifiers for coordination
