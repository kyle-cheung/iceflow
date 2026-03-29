# greytl-sink

## Purpose

`greytl-sink` owns destination-facing commit semantics. It defines the deterministic sink protocol and provides append-only filesystem and Polaris implementations plus a test double for commit, lookup, and uncertain-resolution behavior. Real filesystem and Polaris sinks are append-only only in v0; keyed-upsert behavior is only covered in the protocol planning and test-double paths today.

## Owns

- the sink trait and commit protocol types
- destination URI normalization and idempotency behavior
- filesystem sink behavior for local append-only runs
- Polaris sink behavior for real-stack append-only tests
- test-double sink behavior for protocol verification
- keyed-upsert protocol coverage in planning and test-double tests

## Key Files

- `crates/greytl-sink/src/commit_protocol.rs`
- `crates/greytl-sink/src/filesystem.rs`
- `crates/greytl-sink/src/polaris.rs`
- `crates/greytl-sink/src/test_double.rs`
- `crates/greytl-sink/tests/sink_protocol.rs`
- `crates/greytl-sink/tests/deterministic_append_only.rs`
- `crates/greytl-sink/tests/deterministic_keyed_upsert.rs`
- `crates/greytl-sink/tests/deterministic_recovery.rs`

## Change Here When

- commit preparation, lookup, or uncertain-resolution semantics change
- destination-specific append-only behavior changes for filesystem or Polaris
- idempotency rules or snapshot identity derivation change
- keyed-upsert support is added to the real sinks
- sink-facing end-to-end failures appear in `run` or real-stack tests

## Avoid

- duplicating control-plane state transitions that belong in `greytl-state`
- weakening idempotency guarantees to simplify a destination implementation
- adding CLI argument parsing or runtime throttling here

## Tests To Run

- `cargo test -p greytl-sink`
- `cargo test -p greytl-sink --test sink_protocol`
- `cargo test -p greytl-sink deterministic_`
- `just test-real-stack`

## Related Crates

- `greytl-types` defines the manifests and identifiers committed here
- `greytl-state` tracks the attempt lifecycle around sink calls
- `greytl-cli` selects and configures sink implementations for commands
