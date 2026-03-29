## Principles
If you do not know something or you think that a direction we're taking is wrong. It is okay to say no or I don't know. Gather more information. We want to build and ship a quality product, if we're ever uncertain of something, speak up.

## Quick Reference

- `cargo build`
- `cargo fmt`
- `cargo clippy --workspace --all-targets -- --deny=warnings`
- `just stack-up`
- `just stack-down`
- `just test-compact`
- `just test-fast`
- `cargo test -p iceflow-state --test recovery`
- `cargo test -p iceflow-sink --test sink_protocol`
- `cargo test -p iceflow-cli --test run`
- `just test-real-stack`

## Workspace Structure

- `crates/iceflow-types`: shared engine vocabulary, manifests, mutation contracts, and reference workload types
- `crates/iceflow-source`: source adapter boundary plus the file-backed reference source
- `crates/iceflow-worker-duckdb`: normalization, Parquet writing, and offline compaction helpers
- `crates/iceflow-state`: SQLite-backed control plane, persisted batch/attempt/checkpoint state, recovery/orphan bookkeeping, and migrations
- `crates/iceflow-sink`: sink protocol plus append-only filesystem/Polaris implementations and test-double coverage; real filesystem and Polaris sinks are append-only only today, while keyed-upsert has planning/test-double coverage
- `crates/iceflow-runtime`: intake gating, backpressure, failpoints, and checkpoint sequencing
- `crates/iceflow-cli`: operator-facing `run` and `compact` command wiring; `compact` is append-only only today
- `fixtures/reference_workload_v0/`: checked-in deterministic workloads used by the file source and tests
- `infra/local/`: local Polaris and object-store stack for real-stack testing
- `docs/contributing/`: deeper contributor and per-crate guides

## Where To Look

- Domain model, IDs, manifests, or schema policy: start with `crates/iceflow-types` and `docs/contributing/crates/iceflow-types.md`
- Source contracts or fixture playback: start with `crates/iceflow-source` and `docs/contributing/crates/iceflow-source.md`
- Batch normalization, Parquet output, or rewrite-kernel changes: start with `crates/iceflow-worker-duckdb` and `docs/contributing/crates/iceflow-worker-duckdb.md`
- Offline compaction, compaction catalog/history, or rewrite selection: start with `crates/iceflow-cli` and `docs/contributing/crates/iceflow-cli.md`
- Batch registration, commit attempts, recovery, or checkpoint durability: start with `crates/iceflow-state` and `docs/contributing/crates/iceflow-state.md`
- Commit semantics, idempotency, filesystem sink, or Polaris sink: start with `crates/iceflow-sink` and `docs/contributing/crates/iceflow-sink.md`
- Admission control, backpressure, or checkpoint blocking: start with `crates/iceflow-runtime` and `docs/contributing/crates/iceflow-runtime.md`
- Command wiring or end-to-end local execution: start with `crates/iceflow-cli` and `docs/contributing/crates/iceflow-cli.md`
- Reference workload JSONL, fixture naming, or workload routing: start with `fixtures/reference_workload_v0/`, then update `crates/iceflow-source`, `crates/iceflow-cli/src/commands/run.rs`, `crates/iceflow-types/src/reference_workload.rs`, and `benchmarks/pyiceberg_baseline/` when workload names or fixture routing change
- Local stack bootstrap, Polaris sandbox wiring, or object-store setup: start with `infra/local/` and the stack commands in `justfile`
- Polaris-backed CLI flows: assume unauthenticated endpoints unless the relevant guide says otherwise

## Contributor Guides

- `docs/contributing/README.md`
- `docs/contributing/crates/iceflow-types.md`
- `docs/contributing/crates/iceflow-source.md`
- `docs/contributing/crates/iceflow-worker-duckdb.md`
- `docs/contributing/crates/iceflow-state.md`
- `docs/contributing/crates/iceflow-sink.md`
- `docs/contributing/crates/iceflow-runtime.md`
- `docs/contributing/crates/iceflow-cli.md`

## Delegation Preference

You may use installed ECC agent roles proactively in this repo when they are clearly the best fit.

Preferred roles:
- `ecc_architect` for design and architecture trade-offs

Use inline work when delegation would not materially help.
Do not enable full ECC sync, MCP changes, or global hooks unless explicitly requested.
Do not expand the managed ECC set without asking first.

Use this file as the repo-level routing map. Once you know which crate you are changing, open the matching guide in docs/contributing/crates/ before making non-trivial edits.

## Rust Rules

Apply the managed ECC Rust guidance when working in this repo's Rust code. These can be found in /.claude/

Primary ECC skills:
- `rust-patterns` for Rust implementation, review, refactors, and crate/module design
- `rust-testing` for Rust tests, TDD workflow, coverage, async tests, mocks, and benchmarks
- `security-review` when a change touches secrets, untrusted input, auth, FFI, persistence, or network boundaries

### Style and Ownership

- Run `cargo fmt` before committing Rust changes
- Prefer `cargo clippy -- -D warnings` for touched Rust code when practical
- Use `let` by default; introduce `let mut` only when mutation is required
- Borrow by default; take ownership only when a function needs to store or consume a value
- Prefer `&str` over `String` and `&[T]` over `Vec<T>` in function parameters
- Use `impl Into<String>` for constructors and APIs that need to own strings
- Do not clone just to satisfy the borrow checker without understanding the root cause
- Prefer iterator chains for straightforward transformations; use loops for complex control flow
- Keep modules organized by domain, not by type
- Default to private visibility; use `pub(crate)` for internal sharing and reserve `pub` for real public API

### Error Handling and Modeling

- Use `Result` and `?` for fallible paths; do not use `unwrap()` or `expect()` in production code
- In library-style crates, prefer typed errors with `thiserror`
- In application/CLI entrypoints, prefer contextual error propagation with `anyhow`
- Add context to fallible operations with `.with_context(...)` where it materially improves debugging
- Model invalid states out of existence with enums and newtypes
- Match business-critical enums exhaustively; do not hide cases behind `_`
- Prefer builders for structs with many optional fields
- Use trait-based boundaries for repositories, adapters, and other pluggable components when it improves testability or separation of concerns

### Testing

- Follow a RED -> GREEN -> REFACTOR workflow for new behavior and bug fixes when practical
- Put unit tests in `#[cfg(test)]` modules in the same file as the code under test
- Put integration tests in `tests/`
- Use `rstest` for parameterized tests and fixtures where it improves clarity
- Use `proptest` for property-style invariants and parser/normalization edge cases where appropriate
- Mock through traits with `mockall` when isolating a unit is valuable
- Use `#[tokio::test]` for async tests
- Prefer descriptive test names such as `creates_user_with_valid_email`
- Target meaningful business-logic coverage; use `cargo llvm-cov` when coverage measurement matters

### Security

- Never hardcode API keys, tokens, credentials, or other secrets
- Validate all untrusted input at system boundaries; parse into typed structures early
- Use parameterized queries; never interpolate user input into SQL
- Keep `unsafe` minimal and documented with a `// SAFETY:` comment for every unsafe block
- Do not use `unsafe` to bypass the borrow checker for convenience
- Avoid leaking internal paths, stack traces, or raw database errors to users; log details server-side
- Run `cargo audit` and `cargo deny check` when dependency or security-sensitive changes warrant it
