# Task 10 Benchmark Follow-Ups

These items do not block the first-pass `G1` baseline harness itself, but they should be cleaned up so future verification and review workflows are smoother.

## Deferred Reviewer Gate Cleanup

- The strict reviewer workflow currently stops before diff review if it runs `cargo clippy --workspace --all-targets -- -D warnings`.
- That workspace-wide clippy gate is still blocked by pre-existing `clippy::too_many_arguments` findings in `crates/greytl-types/src/mutation.rs`.
- The current failures are at:
  - `crates/greytl-types/src/mutation.rs:175`
  - `crates/greytl-types/src/mutation.rs:247`
  - `crates/greytl-types/src/mutation.rs:275`
  - `crates/greytl-types/src/mutation.rs:302`
  - `crates/greytl-types/src/mutation.rs:329`
- This lint debt predates Task 10, but it prevented the automated reviewer from reaching the benchmark harness diff.
- If strict workspace review is the desired default, fix or explicitly waive this clippy debt in a separate cleanup change.

## Deferred Live Benchmark Validation

- Task 10 verification covered the Python smoke tests and the dry-run benchmark entrypoint.
- The live PyIceberg path that uploads landed Parquet into the local object store and commits through Polaris was not exercised in-session against a running local stack.
- Re-run the live path in a normal developer shell or CI environment with the local stack up:
  - `just stack-up`
  - `just benchmark-baseline --workload append_only.orders_events`

## Known Local Harness Caveat

- The current Codex harness still replaces `just` with a shim that does not expose arbitrary recipes, so direct `just benchmark-baseline ...` verification may need a normal shell or CI environment even when the underlying module invocation is healthy.
