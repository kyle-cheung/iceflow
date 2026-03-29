# Contributing Guide

This directory is the deep-reference companion to the repo root `AGENTS.md`.

Start in `AGENTS.md` when you need to orient yourself quickly. Use the files in this directory when you already know which crate or subsystem you need to touch and want crate-specific guidance.

## How To Use These Guides

1. Read `AGENTS.md` first for repo-wide rules, quick commands, and routing.
2. Open the relevant crate guide in `docs/contributing/crates/`, or use `## Non-Crate Surfaces` below if you are changing fixtures, local stack wiring, or the root docs.
3. Run the targeted crate tests before widening verification.
4. Update the matching guide if your change materially shifts ownership, entrypoints, or test expectations.

## Main Crates

- `crates/greytl-types`: shared domain vocabulary, manifest types, mutation modeling, and static reference workload metadata.
- `crates/greytl-source`: source adapter boundary plus the deterministic file-backed reference source.
- `crates/greytl-worker-duckdb`: normalization, Parquet materialization, and offline compaction helpers.
- `crates/greytl-state`: SQLite-backed control plane, migrations, persisted batch/attempt/checkpoint state, and recovery/orphan bookkeeping.
- `crates/greytl-sink`: sink protocol plus append-only filesystem/Polaris implementations and test-double coverage.
- `crates/greytl-runtime`: local intake/backpressure and checkpoint gating.
- `crates/greytl-cli`: operator-facing command wiring for `run` and append-only `compact`.

## Crate Guides

- `docs/contributing/crates/greytl-types.md`
- `docs/contributing/crates/greytl-source.md`
- `docs/contributing/crates/greytl-worker-duckdb.md`
- `docs/contributing/crates/greytl-state.md`
- `docs/contributing/crates/greytl-sink.md`
- `docs/contributing/crates/greytl-runtime.md`
- `docs/contributing/crates/greytl-cli.md`

## Non-Crate Surfaces

- `fixtures/reference_workload_v0/`: checked-in reference workloads; when workload names or routing change, update `greytl-source`, `greytl-cli`, `greytl-types`, and `benchmarks/pyiceberg_baseline/` alongside the fixtures.
- `infra/local/`: local Polaris/object-store bootstrap, env templates, and stack scripts; pair this with `just stack-up`, `just stack-down`, and `just test-real-stack`.
- `README.md`: product architecture, current CLI contract, and operator-facing examples.

## Shared Expectations

- Keep crate boundaries explicit. If a change crosses crate boundaries, update all affected guides.
- Prefer targeted tests first, then widen only as needed.
- Document routing changes in `AGENTS.md` whenever the right place to make a change becomes non-obvious.
- Keep these guides short. They should explain ownership, edit hotspots, and validation, not duplicate API docs line-for-line.
