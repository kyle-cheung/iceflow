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

### Build

```sh
cargo build
```

### Test

```sh
# Fast unit tests (Tier 0)
just test-fast

# Deterministic integration tests (Tier 1)
just test-deterministic

# Real-stack tests against Polaris + a local S3-compatible object store (Tier 2)
just test-real
```

### Run

```sh
# Run the reference pipeline against file fixtures
cargo run -p greytl-cli -- run --fixtures fixtures/reference_workload_v0/

# Offline compaction
cargo run -p greytl-cli -- compact --warehouse <path>
```

### Local Stack

```sh
# Start Polaris + the local S3-compatible object store for real-stack testing
docker compose -f infra/local/docker-compose.yml up -d
```

Current `infra/local` uses RustFS as the current default local S3-compatible backend. That choice is local-stack scaffolding, not a core runtime dependency. Production guidance remains direct cloud object storage, especially AWS S3.

Task 8b audited `crates/greytl-sink/src/polaris.rs` and `infra/local/polaris-bootstrap.sh` and found no presigned URL usage in the current Polaris local-stack path, so the RustFS compatibility gate excludes presigned URL verification for this task.

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
