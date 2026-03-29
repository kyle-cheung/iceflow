# Contributing Guide

This directory is the deep-reference companion to the repo root `AGENTS.md`.

Start in `AGENTS.md` when you need to orient yourself quickly. Use the files in this directory when you already know which crate or subsystem you need to touch and want crate-specific guidance.

## How To Use These Guides

1. Read `AGENTS.md` first for repo-wide rules, quick commands, and routing.
2. Open the relevant crate guide in `docs/contributing/crates/`, or use `## Non-Crate Surfaces` below if you are changing fixtures, local stack wiring, or the root docs.
3. Run the targeted crate tests before widening verification.
4. Update the matching guide if your change materially shifts ownership, entrypoints, or test expectations.

## Main Crates

- `crates/iceflow-types`: shared domain vocabulary, manifest types, mutation modeling, and static reference workload metadata.
- `crates/iceflow-source`: source adapter boundary plus the deterministic file-backed reference source.
- `crates/iceflow-worker-duckdb`: normalization, Parquet materialization, and offline compaction helpers.
- `crates/iceflow-state`: SQLite-backed control plane, migrations, persisted batch/attempt/checkpoint state, and recovery/orphan bookkeeping.
- `crates/iceflow-sink`: sink protocol plus append-only filesystem/Polaris implementations and test-double coverage.
- `crates/iceflow-runtime`: local intake/backpressure and checkpoint gating.
- `crates/iceflow-cli`: operator-facing command wiring for `run` and append-only `compact`.

## Crate Guides

- `docs/contributing/crates/iceflow-types.md`
- `docs/contributing/crates/iceflow-source.md`
- `docs/contributing/crates/iceflow-worker-duckdb.md`
- `docs/contributing/crates/iceflow-state.md`
- `docs/contributing/crates/iceflow-sink.md`
- `docs/contributing/crates/iceflow-runtime.md`
- `docs/contributing/crates/iceflow-cli.md`

## Non-Crate Surfaces

- `fixtures/reference_workload_v0/`: checked-in reference workloads; when workload names or routing change, update `iceflow-source`, `iceflow-cli`, `iceflow-types`, and `benchmarks/pyiceberg_baseline/` alongside the fixtures.
- `infra/local/`: local Polaris/object-store bootstrap, env templates, and stack scripts; pair this with `just stack-up`, `just stack-down`, and `just test-real-stack`.
- `README.md`: product architecture, current CLI contract, and operator-facing examples.

## Shared Expectations

- Keep crate boundaries explicit. If a change crosses crate boundaries, update all affected guides.
- Prefer targeted tests first, then widen only as needed.
- Document routing changes in `AGENTS.md` whenever the right place to make a change becomes non-obvious.
- Keep these guides short. They should explain ownership, edit hotspots, and validation, not duplicate API docs line-for-line.
