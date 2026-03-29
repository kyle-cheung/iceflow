# ICEFLOW

An open-source, Iceberg-native ingestion engine optimized for correctness first and performance second. ICEFLOW proves the control plane and write path under failure before broadening source coverage.

## What It Does

ICEFLOW is a destination-optimized ingestion engine — not a generic ELT wrapper. The differentiation target is the normalize → file layout → commit → compaction path against Iceberg-native semantics.

The default pipeline:

```
source adapter → Arrow batch → normalize/write worker → Parquet batch + manifest → commit coordinator → Iceberg catalog → reconciliation/offline compaction
```

## Architecture

- **Rust coordinator** with an in-process **DuckDB** worker runtime
- **Arrow** for low-latency in-memory handoff
- **Parquet + manifest** as the durable replay boundary
- **SQLite WAL** control plane as the source of truth for batch identity, commit attempts, checkpoint linkage, and recovery
- **Iceberg** sink abstraction defined in terms of deterministic commit and lookup operations

### Crates

| Crate | Purpose |
|---|---|
| `greytl-types` | Canonical IDs, mutation model, manifest model, schema policy |
| `greytl-state` | SQLite-backed control plane: batch ledger, commit attempts, checkpoints, quarantine |
| `greytl-source` | Source adapter SDK and deterministic file-based reference source |
| `greytl-worker-duckdb` | Arrow normalization, Parquet materialization, ordering validation |
| `greytl-sink` | Sink trait, commit protocol, test doubles, filesystem and Polaris implementations |
| `greytl-runtime` | Pipeline coordinator, backpressure, single-writer-per-table execution |
| `greytl-cli` | CLI for running pipelines and offline compaction |

### Repo Layout

| Path | Purpose |
|---|---|
| `crates/` | Rust implementation split by domain: types, state, source, worker, sink, runtime, and CLI |
| `fixtures/reference_workload_v0/` | Canonical JSONL reference workloads used by the file source and benchmark fixture generation |
| `infra/local/` | Local Polaris plus S3-compatible stack, `.env` template, and bootstrap scripts |
| `benchmarks/` | Benchmark harnesses and benchmark-specific documentation |
| `docs/` | Session briefs plus deferred follow-up notes that are intentionally kept out of the root README |

## Table Modes

**`append_only`** — Insert-only ingestion for logs, snapshots, and file drops. Each committed batch is applied at most once; replay converges on the same batch identity.

**`keyed_upsert`** — Maintains latest state by key for mutable source tables under ordered change input. Uses equality-delete-plus-append semantics. Later source position wins for a given key.

## Key Design Decisions

- **Correctness over performance.** A source checkpoint advances only after a destination commit is durably resolved and linked to that checkpoint.
- **Durable control plane.** Workers are stateless executors. The control plane owns commit attempt sequencing, retry state, and recovery.
- **Replayable batches.** Every batch has a stable `batch_id`, immutable manifest, and durable ordering metadata. The replay boundary is Parquet batch + manifest.
- **Single active writer per table** in v0. Reduces catalog conflict handling while proving commit, recovery, replay, and checkpoint correctness.
- **Local-first testing.** Core correctness paths run locally without cloud dependencies. If a failure mode can't be simulated locally, that's a design smell.

## Recovery

Worker crashes don't make commit outcomes unknowable. The recovery state machine handles:

- Commit succeeds but ack is lost
- Commit fails after files are written
- Retry after uncertain commit
- Duplicate batch replay
- Worker crash between file write and commit
- Schema change during replay or retry
- Orphan file detection and cleanup

## Getting Started

### Prerequisites

- Rust (see `rust-toolchain.toml` for pinned version)
- [Just](https://github.com/casey/just) command runner
- Docker Compose for the local Polaris/object-store stack
- [`uv`](https://docs.astral.sh/uv/) for the Python baseline benchmark harness

### Build

```sh
cargo build
```

### Test

```sh
# Fast unit tests (Tier 0)
just test-fast

# Local Polaris bootstrap + S3-compatible object-store probe
just test-local-object-store

# Real-stack sink tests against Polaris + the local stack
just test-real-stack

# Offline compaction tests
just test-compact
```

### Run

```sh
# Run the append-only reference workload to a local filesystem destination
cargo run -p greytl-cli -- run \
  --workload append_only.orders_events \
  --destination-uri file:///tmp/greytl-demo \
  --sink filesystem

# Offline compaction
cargo run -p greytl-cli -- compact \
  --warehouse-uri file:///tmp/greytl-demo \
  --catalog-uri http://127.0.0.1:8181/api/catalog \
  --catalog demo \
  --namespace orders_events \
  --table orders \
  --table-mode append_only \
  --min-small-file-bytes 1048576 \
  --max-rewrite-files 8
```

Current reference workloads:

- `append_only.orders_events`
- `keyed_upsert.customer_state`

The `run` command defaults to the filesystem sink. Use `--sink polaris` with `--catalog-uri`, `--catalog`, and `--namespace` when targeting the local or real Polaris catalog.

### Local Stack

```sh
# Validate the local stack config
just stack-config

# Start Polaris + the local S3-compatible object store
just stack-up

# Tear it down
just stack-down
```

`infra/local/.env.example` is the source template for local runs. The `just` recipes copy it to `infra/local/.env` if needed and export the Polaris/object-store settings expected by the Rust tests and Python benchmark harness.

Current `infra/local` uses RustFS as the default local S3-compatible backend. That is local-stack scaffolding, not a core runtime dependency. The local object-store probe validates Polaris bootstrap plus raw path-style access, but the current Polaris sink path still stages committed files into a local `file://` warehouse rather than proving end-to-end greytl data-file writes through that S3-compatible store. Production guidance remains direct cloud object storage, especially AWS S3. The current local-stack path does not rely on presigned URLs.

### Benchmarks

`benchmarks/README.md` documents the current baseline harnesses. The active v0 benchmark is the `G1` PyIceberg write-path baseline in `benchmarks/pyiceberg_baseline/`.

```sh
# Resolve workloads, stack settings, and output paths without mutating the stack
just benchmark-baseline --dry-run

# Run the active append-only baseline
just benchmark-baseline --workload append_only.orders_events
```

The first pass enables `append_only.orders_events` and keeps `keyed_upsert.customer_state` scaffolded but skipped until the engine sink path supports that mode.

### Known Follow-Ups

- `docs/task9-offline-compaction-followups.md`
- `docs/task10-benchmark-followups.md`

## V0 Scope

**In scope:**
- Synthetic/file-based source
- Full `append_only` implementation
- Constrained `keyed_upsert` on ordered fixtures
- Durable control plane and commit ledger
- Deterministic local failure injection
- Local real-stack testing (Polaris + a local S3-compatible object store + SQLite)
- Reconciliation and orphan cleanup
- Offline compaction utility

**Out of scope:**
- Arbitrary out-of-order CDC conflict resolution
- Multi-writer per table
- Continuous compaction service
- Broad connector coverage
- Rename/drop schema automation
- Partition evolution and advanced clustering

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
