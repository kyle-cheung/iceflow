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

| Crate | What it owns | Where it sits in the flow |
|---|---|---|
| `iceflow-types` | Shared domain model: table IDs, logical mutations, manifests, schema policy, reference workload descriptors | Used everywhere; this is the vocabulary the rest of the system agrees on |
| `iceflow-source` | Session-based source adapter trait plus the deterministic file-based reference source | Produces `SourceBatch` values from fixture/reference workloads |
| `iceflow-worker-duckdb` | Normalize records, enforce ordering rules, and materialize landed Parquet + batch manifest | Turns a source batch into the durable replay boundary |
| `iceflow-state` | SQLite-backed control plane for batch registration, file tracking, commit attempts, checkpoint linkage, recovery, and quarantine | Records the authoritative lifecycle of a batch before and after sink commits |
| `iceflow-sink` | Sink contract, idempotent commit protocol, commit lookup/recovery APIs, filesystem sink, Polaris sink, and test doubles | Owns the destination-facing write and commit semantics |
| `iceflow-runtime` | In-memory coordinator for table admission, backpressure, durable-pending tracking, and checkpoint gating | Guards when a table may ingest or checkpoint another batch |
| `iceflow-cli` | Thin executable that wires source + worker + state + sink + runtime into runnable commands | Current operator entrypoint for `source check`, `connector check`, `connector run`, the legacy `run`, and `compact` |

### How the Crates Fit Together

The pipeline is intentionally split so each crate owns one boundary:

1. `iceflow-source` opens a capture session for a source table and polls the next `SourceBatch`.
2. `iceflow-worker-duckdb` normalizes that batch into engine-shaped rows and writes landed Parquet plus a deterministic manifest.
3. `iceflow-state` registers the manifest, records the written files, opens a commit attempt, and later records the resolved outcome plus checkpoint linkage.
4. `iceflow-sink` prepares and commits the batch into the destination, with lookup and recovery hooks for uncertain outcomes.
5. `iceflow-runtime` decides whether a table can admit more work and whether checkpoint advancement is still blocked.
6. `iceflow-cli` orchestrates the whole sequence for local runs and offline maintenance tasks.

Said another way:

- `iceflow-types` defines the nouns
- `iceflow-source` produces batches
- `iceflow-worker-duckdb` turns batches into replayable files
- `iceflow-state` remembers what happened
- `iceflow-sink` makes destination changes durable
- `iceflow-runtime` enforces sequencing
- `iceflow-cli` is the operator-facing wrapper around all of it

### Repo Layout

| Path | Purpose |
|---|---|
| `crates/` | Rust implementation split by domain: types, state, source, worker, sink, runtime, and CLI |
| `fixtures/reference_workload_v0/` | Canonical JSONL reference workloads used by the file source and benchmark fixture generation |
| `fixtures/config_samples/` | Sample config roots split into `sources/`, `destinations/`, `catalogs/`, and `connectors/` for the config-driven CLI |
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

# Equivalent fast-suite commands if `just` is unavailable
cargo test -p iceflow-types
cargo test -p iceflow-source --test source_adapter

# Local Polaris bootstrap + S3-compatible object-store probe
just test-local-object-store

# Real-stack sink tests against Polaris + the local stack
just test-real-stack

# Offline compaction tests
just test-compact
```

### Run

The current executable crate is `iceflow-cli`.

During development, run it with Cargo:

```sh
cargo run -p iceflow-cli -- run ...
```

After building, the binary is `iceflow-cli` in `target/debug/` or `target/release/`.

```sh
./target/debug/iceflow-cli run ...
```

The CLI currently exposes five subcommands:

- `source check`: validate a source config and report advertised capabilities
- `connector check`: validate a connector config root before running it
- `connector run`: execute a config-driven connector end to end through source → worker → state → sink
- `run`: execute a checked-in reference workload directly without the config layer
- `compact`: run offline append-only file compaction against an existing table layout

Unlike a typical Clap-based CLI, `iceflow-cli --help` is not implemented yet. Treat the README examples below as the current command contract.

#### Config model

The config-driven commands expect a config root with four surfaces:

```text
<config-root>/
  sources/
  destinations/
  catalogs/
  connectors/
```

- `sources/*.toml`: source adapter configs such as the routed local file source
- `destinations/*.toml`: sink configs such as filesystem and Polaris
- `catalogs/*.toml`: optional catalog configs referenced by destinations or connectors
- `connectors/*.toml`: table mappings plus capture settings that bind a source to a destination

Sample files live in `fixtures/config_samples/`:

- `fixtures/config_samples/sources/local_file.toml`
- `fixtures/config_samples/destinations/local_fs.toml`
- `fixtures/config_samples/destinations/local_polaris.toml`
- `fixtures/config_samples/catalogs/local_polaris.toml`
- `fixtures/config_samples/connectors/orders_append.toml`
- `fixtures/config_samples/connectors/orders_append_polaris.toml`

#### `source check`

Use `source check` to validate a source config in isolation and inspect the source capabilities that a connector can rely on.

```sh
cargo run -p iceflow-cli -- source check \
  --source fixtures/config_samples/sources/local_file.toml
```

#### `connector check`

Use `connector check` to validate a config root before execution. This verifies the referenced source, destination, optional catalog, and table-mode compatibility.

```sh
cargo run -p iceflow-cli -- connector check \
  --connector fixtures/config_samples/connectors/orders_append.toml
```

#### `connector run`

Use `connector run` for the config-driven replication path. It derives the config root from the connector path and resumes each table from the last durable checkpoint stored in `iceflow-state`.

```sh
cargo run -p iceflow-cli -- connector run \
  --connector fixtures/config_samples/connectors/orders_append.toml
```

#### `run`

Use `run` to execute one of the checked-in reference workloads directly. This path is preserved for deterministic regression coverage and benchmark fixture playback.

Required:

- `--workload`
- `--destination-uri`

Optional:

- `--sink filesystem|polaris`
- `--catalog-uri`, `--catalog`, `--namespace` when `--sink polaris`
- `--batch-limit` to stop after a fixed number of source batches

```sh
# Run the append-only reference workload to a local filesystem destination
cargo run -p iceflow-cli -- run \
  --workload append_only.orders_events \
  --destination-uri file:///tmp/iceflow-demo \
  --sink filesystem

# Run the same workload against Polaris
cargo run -p iceflow-cli -- run \
  --workload append_only.orders_events \
  --destination-uri file:///tmp/iceflow-demo \
  --sink polaris \
  --catalog-uri http://127.0.0.1:8181/api/catalog \
  --catalog quickstart_catalog \
  --namespace orders_events
```

What `run` does internally:

- loads the workload through `iceflow-source::FileSource`
- materializes each batch through `iceflow-worker-duckdb::DuckDbWorker`
- records batch/commit/checkpoint state in `iceflow-state::SqliteStateStore`
- commits through either `iceflow-sink::FilesystemSink` or `iceflow-sink::PolarisSink`
- uses `iceflow-runtime::RuntimeCoordinator` to gate intake and checkpoint advancement

#### `compact`

Use `compact` to rewrite small append-only files in an existing table directory and emit a machine-readable JSON report.

Required:

- `--warehouse-uri`
- `--catalog-uri`
- `--catalog`
- `--namespace`
- `--table`
- `--table-mode`
- `--min-small-file-bytes`
- `--max-rewrite-files`

Current limitation:

- `keyed_upsert` compaction is intentionally rejected in this version

```sh
# Offline compaction
cargo run -p iceflow-cli -- compact \
  --warehouse-uri file:///tmp/iceflow-demo \
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

Current `infra/local` uses RustFS as the default local S3-compatible backend. That is local-stack scaffolding, not a core runtime dependency. The local object-store probe validates Polaris bootstrap plus raw path-style access, but the current Polaris sink path still stages committed files into a local `file://` warehouse rather than proving end-to-end iceflow data-file writes through that S3-compatible store. Production guidance remains direct cloud object storage, especially AWS S3. The current local-stack path does not rely on presigned URLs.

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
