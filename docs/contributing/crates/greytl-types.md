# greytl-types

## Purpose

`greytl-types` is the shared vocabulary crate for the workspace. If multiple crates need to agree on an identifier, manifest shape, logical mutation contract, schema policy rule, or shared reference workload metadata, it belongs here.

## Owns

- table, batch, checkpoint, and commit identifier types
- logical mutation modeling and validation
- batch manifest and manifest file contracts
- schema policy evaluation inputs and outputs
- static shared reference workload descriptors / metadata

## Key Files

- `crates/greytl-types/src/ids.rs`
- `crates/greytl-types/src/mutation.rs`
- `crates/greytl-types/src/manifest.rs`
- `crates/greytl-types/src/schema_policy.rs`
- `crates/greytl-types/src/reference_workload.rs`
- `crates/greytl-types/tests/canonical_model.rs`
- `crates/greytl-types/tests/manifest_contract.rs`

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

- `cargo test -p greytl-types`
- `cargo test -p greytl-types --test canonical_model`
- `cargo test -p greytl-types --test manifest_contract`

## Related Crates

- `greytl-source` consumes the shared mutation and batch contract types
- `greytl-worker-duckdb` materializes batches described by these types
- `greytl-state` persists manifest and checkpoint data shaped here
- `greytl-sink` commits manifests built from these contracts
- `greytl-runtime` uses shared table identifiers for coordination
