# Task 10 Benchmark Follow-Ups

These items do not block the first-pass `G1` baseline harness itself, but they should be cleaned up so future verification and review workflows are smoother.

## Deferred Reviewer Gate Cleanup

- The strict reviewer workflow currently stops before diff review if it runs `cargo clippy --workspace --all-targets -- -D warnings`.
- That workspace-wide clippy gate is still blocked by pre-existing `clippy::too_many_arguments` findings in `crates/iceflow-types/src/mutation.rs`.
- The current failures are at:
  - `crates/iceflow-types/src/mutation.rs:175`
  - `crates/iceflow-types/src/mutation.rs:247`
  - `crates/iceflow-types/src/mutation.rs:275`
  - `crates/iceflow-types/src/mutation.rs:302`
  - `crates/iceflow-types/src/mutation.rs:329`
- This lint debt predates Task 10, but it prevented the automated reviewer from reaching the benchmark harness diff.
- If strict workspace review is the desired default, fix or explicitly waive this clippy debt in a separate cleanup change.

## Deferred Live Benchmark Validation

- Task 10 verification covered the Python smoke tests and the dry-run benchmark entrypoint.
- Re-run the live path in a normal developer shell or CI environment with the local stack up:
  - `just stack-up`
  - `just benchmark-baseline --workload append_only.orders_events`
- Follow-up verification on March 27, 2026 exercised the live path against the local stack and fixed the first harness-side bug: generated Parquet now uses engine-shaped landed columns instead of `null`-typed JSON structs that PyIceberg rejects during schema conversion.
- Follow-up verification on March 28, 2026 cleared the remaining local-stack blockers:
  - the local Polaris bootstrap now marks the S3-compatible RustFS catalog storage as `stsUnavailable: true`, which avoids the `Failed to get subscoped credentials` path during `catalog.create_table(...)`
  - the benchmark harness now lets Polaris assign the table location instead of forcing an explicit `s3://.../benchmarks/.../table` path outside the catalog's allowed locations
- With those two changes in place, the live `append_only.orders_events` PyIceberg baseline now completes successfully against the default local stack.
- Remaining caveat: developers with an older pre-RustFS `infra/local/.env` may still have stale `MINIO_*` credentials. Refresh the file from `infra/local/.env.example` or rely on the `just benchmark-baseline` wrapper, which now exports the current RustFS-compatible `OBJECT_STORE_*` defaults.

## Known Local Harness Caveat

- The current Codex harness still replaces `just` with a shim that does not expose arbitrary recipes, so direct `just benchmark-baseline ...` verification may need a normal shell or CI environment even when the underlying module invocation is healthy.
