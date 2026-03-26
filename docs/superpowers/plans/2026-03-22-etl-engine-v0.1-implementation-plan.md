# ETL Engine v0.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a correctness-first v0 ETL engine slice that can ingest deterministic synthetic or file-based batches, materialize replayable Parquet plus manifest batches, resolve Iceberg commits through a durable SQLite-backed control plane, support `append_only` and constrained `keyed_upsert`, and run locally against both deterministic test doubles and a Polaris plus local S3-compatible reference stack.

**Architecture:** The implementation is a Rust workspace with narrow crates for canonical types, the SQLite control plane, a minimal source SDK plus file source, an in-process DuckDB worker, an Iceberg sink layer, a runtime coordinator, and a CLI. Recovery and replay are driven by the control-plane ledger, while local and real-stack tests reuse the same fixtures and failure injections. Benchmarking and offline compaction are included, but broad connector sufficiency remains out of scope for v0.

**Tech Stack:** Rust, Tokio, DuckDB, Apache Arrow, Parquet, SQLite WAL, Iceberg/Polaris, a local S3-compatible object store (current default RustFS), Docker Compose, Just, PyIceberg for the baseline comparison

---

## File Structure

### Root files

- Create: `Cargo.toml`
  Responsibility: Rust workspace manifest for all crates and shared dependencies.
- Create: `rust-toolchain.toml`
  Responsibility: Pin the Rust toolchain used locally and in CI.
- Create: `justfile`
  Responsibility: One-command developer entrypoints for fast tests, deterministic integration tests, local stack startup, real-stack tests, and benchmarks.
- Create: `.github/workflows/ci.yml`
  Responsibility: Run Tier 0 and Tier 1 on every PR, Tier 2 on every PR for the reference set, and expose nightly Tier 3 hooks.
- Create: `infra/local/docker-compose.yml`
  Responsibility: Start Polaris plus a local S3-compatible object store for real-stack tests.
- Create: `infra/local/object-store-init.sh`
  Responsibility: Create the current default reference test bucket and apply deterministic object-store setup.

### Crates

- Create: `crates/greytl-types/Cargo.toml`
  Responsibility: Package manifest for canonical types.
- Create: `crates/greytl-types/src/lib.rs`
  Responsibility: Re-export canonical IDs, mutation model, manifest model, ordering metadata, schema policy, and table-mode validation.
- Create: `crates/greytl-types/src/ids.rs`
  Responsibility: Strongly typed `BatchId`, `CommitAttemptId`, `TableId`, and checkpoint identifiers.
- Create: `crates/greytl-types/src/mutation.rs`
  Responsibility: Logical mutation model, delete-tombstone shape, table-mode rules, and validation helpers.
- Create: `crates/greytl-types/src/manifest.rs`
  Responsibility: Replayable Parquet batch manifest and checksum helpers.
- Create: `crates/greytl-types/src/schema_policy.rs`
  Responsibility: v0 schema evolution policy, retry-time revalidation rules, and key-column immutability checks.
- Create: `crates/greytl-types/src/reference_workload.rs`
  Responsibility: Reference workload constants used by tests and benchmark harnesses.
- Create: `crates/greytl-types/tests/manifest_contract.rs`
  Responsibility: Validate manifest shape, checksum behavior, and replay identity invariants.

- Create: `crates/greytl-state/Cargo.toml`
  Responsibility: Package manifest for the control-plane crate.
- Create: `crates/greytl-state/src/lib.rs`
  Responsibility: Public API for the control plane, repository traits, and recovery entrypoints.
- Create: `crates/greytl-state/src/sqlite.rs`
  Responsibility: SQLite WAL implementation of batch, attempt, checkpoint, and quarantine persistence.
- Create: `crates/greytl-state/src/migrations.rs`
  Responsibility: Schema creation and migrations for the ledger tables.
- Create: `crates/greytl-state/src/recovery.rs`
  Responsibility: Batch and attempt state transitions, recovery resolution logic, and orphan classification.
- Create: `crates/greytl-state/src/reconcile.rs`
  Responsibility: Recovery-candidate listing, orphan detection, checkpoint-link verification, and durable cleanup recording.
- Create: `crates/greytl-state/tests/recovery.rs`
  Responsibility: Validate recovery state transitions and attempt-to-batch mappings.
- Create: `crates/greytl-state/tests/reconcile.rs`
  Responsibility: Validate orphan classification, cleanup recording, and checkpoint-link verification.

- Create: `crates/greytl-source/Cargo.toml`
  Responsibility: Package manifest for source adapters.
- Create: `crates/greytl-source/src/lib.rs`
  Responsibility: Minimal source SDK surface and adapter contracts for the narrowed v0 slice.
- Create: `crates/greytl-source/src/adapter.rs`
  Responsibility: `spec`, `check`, `discover`, `snapshot`, and `checkpoint` traits and request/response types.
- Create: `crates/greytl-source/src/file_source.rs`
  Responsibility: Deterministic file-based reference source that emits ordered batches from fixtures.
- Create: `crates/greytl-source/tests/source_adapter.rs`
  Responsibility: Validate the connector SDK lifecycle and ordering/checkpoint contract for the file source.

- Create: `crates/greytl-worker-duckdb/Cargo.toml`
  Responsibility: Package manifest for the DuckDB worker.
- Create: `crates/greytl-worker-duckdb/src/lib.rs`
  Responsibility: DuckDB worker façade used by the runtime.
- Create: `crates/greytl-worker-duckdb/src/normalize.rs`
  Responsibility: Arrow normalization, type coercion, ordering validation, and latest-wins reduction.
- Create: `crates/greytl-worker-duckdb/src/parquet_writer.rs`
  Responsibility: Parquet materialization, file rolling, row-group flushing, and manifest output.

- Create: `crates/greytl-sink/Cargo.toml`
  Responsibility: Package manifest for sink implementations.
- Create: `crates/greytl-sink/src/lib.rs`
  Responsibility: Sink trait, commit protocol types, and sink implementations.
- Create: `crates/greytl-sink/src/commit_protocol.rs`
  Responsibility: `prepare_commit`, `commit`, `lookup_commit`, `lookup_snapshot`, and `resolve_uncertain_commit`.
- Create: `crates/greytl-sink/src/test_double.rs`
  Responsibility: Deterministic sink with failpoints for Tier 1 tests.
- Create: `crates/greytl-sink/src/filesystem.rs`
  Responsibility: Local filesystem-backed warehouse sink used in Tier 1 deterministic integration tests.
- Create: `crates/greytl-sink/src/polaris.rs`
  Responsibility: Real-stack sink implementation against the Polaris plus local S3-compatible reference stack.
- Create: `crates/greytl-sink/tests/sink_protocol.rs`
  Responsibility: Validate sink protocol semantics against the deterministic test double.
- Create: `crates/greytl-sink/tests/deterministic_append_only.rs`
  Responsibility: Deterministic `append_only` correctness and recovery tests.
- Create: `crates/greytl-sink/tests/deterministic_keyed_upsert.rs`
  Responsibility: Deterministic constrained `keyed_upsert` correctness tests.
- Create: `crates/greytl-sink/tests/deterministic_recovery.rs`
  Responsibility: Lost ack, failed commit, duplicate replay, and orphan cleanup scenarios.
- Create: `crates/greytl-sink/tests/polaris_append_only.rs`
  Responsibility: Reference real-stack `append_only` correctness tests.
- Create: `crates/greytl-sink/tests/polaris_keyed_upsert.rs`
  Responsibility: Reference real-stack constrained `keyed_upsert` tests.
- Create: `crates/greytl-sink/tests/polaris_recovery.rs`
  Responsibility: Real-stack uncertain-commit and retry-time schema-revalidation tests.

- Create: `crates/greytl-runtime/Cargo.toml`
  Responsibility: Package manifest for the runtime coordinator.
- Create: `crates/greytl-runtime/src/lib.rs`
  Responsibility: Runtime coordinator entrypoints.
- Create: `crates/greytl-runtime/src/pipeline.rs`
  Responsibility: Batch admission, single-writer-per-table execution, and checkpoint sequencing.
- Create: `crates/greytl-runtime/src/backpressure.rs`
  Responsibility: Queue limits, memory budgets, and intake pause behavior.
- Create: `crates/greytl-runtime/src/failpoints.rs`
  Responsibility: Failure injection hooks shared by deterministic and real-stack tests.
- Create: `crates/greytl-runtime/tests/backpressure.rs`
  Responsibility: Queue saturation and intake-pause behavior.

- Create: `crates/greytl-cli/Cargo.toml`
  Responsibility: Package manifest for the CLI.
- Create: `crates/greytl-cli/src/main.rs`
  Responsibility: CLI entrypoint.
- Create: `crates/greytl-cli/src/commands/run.rs`
  Responsibility: Run the v0 reference pipeline against file fixtures.
- Create: `crates/greytl-cli/src/commands/compact.rs`
  Responsibility: Offline compaction command and JSON report output.
- Create: `crates/greytl-cli/tests/compact.rs`
  Responsibility: Validate offline compaction output and CLI contract.

### Fixtures

- Create: `fixtures/reference_workload_v0/orders_events/*.jsonl`
  Responsibility: `append_only.orders_events` reference batches.
- Create: `fixtures/reference_workload_v0/customer_state/*.jsonl`
  Responsibility: Ordered `keyed_upsert.customer_state` batches with updates and deletes.
- Create: `fixtures/reference_workload_v0/expected/*.json`
  Responsibility: Expected ledger snapshots, manifests, and table outcomes.

### Benchmarking

- Create: `benchmarks/pyiceberg_baseline/pyproject.toml`
  Responsibility: Baseline Python environment definition.
- Create: `benchmarks/pyiceberg_baseline/run.py`
  Responsibility: PyIceberg baseline runner against the same landed-Parquet fixtures and local stack.
- Create: `benchmarks/README.md`
  Responsibility: How to run `G1` measurements and interpret output.

## Sequencing Notes

- Do not pull `G4` connector-sufficiency work into this plan. The only source implementation in scope is the deterministic file-based reference source.
- `keyed_upsert` is required in v0, but only under the constrained ordered-input contract from the approved spec.
- Key-column type changes are out of scope in v0; schema revalidation must reject them.
- Offline compaction must be a CLI utility, not a long-running service.

### Task 1: Bootstrap The Workspace And Canonical Types

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `justfile`
- Create: `crates/greytl-types/Cargo.toml`
- Create: `crates/greytl-state/Cargo.toml`
- Create: `crates/greytl-source/Cargo.toml`
- Create: `crates/greytl-worker-duckdb/Cargo.toml`
- Create: `crates/greytl-sink/Cargo.toml`
- Create: `crates/greytl-runtime/Cargo.toml`
- Create: `crates/greytl-cli/Cargo.toml`
- Create: `crates/greytl-types/src/lib.rs`
- Create: `crates/greytl-types/src/ids.rs`
- Create: `crates/greytl-types/src/mutation.rs`
- Create: `crates/greytl-types/src/manifest.rs`
- Create: `crates/greytl-types/src/schema_policy.rs`
- Create: `crates/greytl-types/src/reference_workload.rs`
- Create: `crates/greytl-types/tests/manifest_contract.rs`
- Test: `crates/greytl-types/src/mutation.rs`
- Test: `crates/greytl-types/src/schema_policy.rs`

- [ ] **Step 1: Write failing canonical-model tests**

```rust
#[test]
fn delete_mutation_requires_null_after() {
    let mutation = LogicalMutation::delete(
        table_id("customer_state"),
        key([("tenant_id", "t1"), ("customer_id", "c1")]),
        ordering("source_position", 42),
        checkpoint("batch-0001"),
    )
    .with_after(json!({"name": "bad"}));

    assert!(validate_mutation(&mutation).is_err());
}

#[test]
fn schema_policy_rejects_key_column_widening() {
    let current = schema_with_key("customer_id", DataType::Int32);
    let next = schema_with_key("customer_id", DataType::Int64);

    assert_eq!(
        evaluate_schema_policy(&current, &next),
        SchemaDecision::Quarantine("key-column-type-change")
    );
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p greytl-types delete_mutation_requires_null_after -- --exact`
Expected: FAIL because the crate and validation functions do not exist yet.

Run: `cargo test -p greytl-types schema_policy_rejects_key_column_widening -- --exact`
Expected: FAIL because the crate and validation functions do not exist yet.

- [ ] **Step 3: Create the workspace and implement minimal canonical types**

```rust
pub enum Operation {
    Insert,
    Upsert,
    Delete,
}

pub struct LogicalMutation {
    pub table_id: TableId,
    pub source_id: String,
    pub source_class: SourceClass,
    pub table_mode: TableMode,
    pub op: Operation,
    pub key: StructuredKey,
    pub after: Option<serde_json::Value>,
    pub before: Option<serde_json::Value>,
    pub ordering_field: String,
    pub ordering_value: i64,
    pub source_checkpoint: String,
    pub source_event_id: Option<String>,
    pub schema_version: i32,
    pub ingestion_ts: chrono::DateTime<chrono::Utc>,
    pub source_metadata: BTreeMap<String, String>,
}

pub fn validate_mutation(m: &LogicalMutation) -> anyhow::Result<()> {
    match m.op {
        Operation::Delete if m.after.is_some() => anyhow::bail!("delete requires after=null"),
        Operation::Insert | Operation::Upsert if m.after.is_none() => {
            anyhow::bail!("insert/upsert require after payload")
        }
        _ => Ok(()),
    }
}
```

- [ ] **Step 4: Implement the v0 schema policy and reference workload constants**

```rust
pub struct BatchManifest {
    pub batch_id: BatchId,
    pub table_id: TableId,
    pub table_mode: TableMode,
    pub source_id: String,
    pub source_class: SourceClass,
    pub source_checkpoint_start: String,
    pub source_checkpoint_end: String,
    pub ordering_field: String,
    pub ordering_min: i64,
    pub ordering_max: i64,
    pub schema_version: i32,
    pub schema_fingerprint: String,
    pub record_count: u64,
    pub op_counts: BTreeMap<Operation, u64>,
    pub file_set: Vec<ManifestFile>,
    pub content_hash: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub fn evaluate_schema_policy(current: &Schema, next: &Schema) -> SchemaDecision {
    if key_column_type_changed(current, next) {
        return SchemaDecision::Quarantine("key-column-type-change");
    }
    if only_additive_or_allowed_widening(current, next) {
        SchemaDecision::Allow
    } else {
        SchemaDecision::Quarantine("incompatible-schema-change")
    }
}
```

Manifest ownership boundary:
- source adapter owns `source_id`, `source_class`, and checkpoint span
- DuckDB worker owns ordering span, `record_count`, `op_counts`, `file_set`, and `content_hash`
- control plane freezes the final manifest URI and links the manifest to batch identity before commit starts

- [ ] **Step 5: Run crate tests and fast entrypoint**

Run: `cargo test -p greytl-types`
Expected: PASS

Run: `just test-fast`
Expected: invokes the workspace fast-test command and exits `0`.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml rust-toolchain.toml justfile crates/greytl-types crates/greytl-state/Cargo.toml crates/greytl-source/Cargo.toml crates/greytl-worker-duckdb/Cargo.toml crates/greytl-sink/Cargo.toml crates/greytl-runtime/Cargo.toml crates/greytl-cli/Cargo.toml
git commit -m "feat: add canonical ETL types and schema policy"
```

### Task 2: Implement The SQLite Control Plane And Recovery State Machine

**Files:**
- Create: `crates/greytl-state/src/lib.rs`
- Create: `crates/greytl-state/src/sqlite.rs`
- Create: `crates/greytl-state/src/migrations.rs`
- Create: `crates/greytl-state/src/recovery.rs`
- Create: `crates/greytl-state/src/reconcile.rs`
- Test: `crates/greytl-state/tests/recovery.rs`
- Test: `crates/greytl-state/tests/reconcile.rs`
- Test: `crates/greytl-types/tests/manifest_contract.rs`

- [ ] **Step 1: Write failing ledger-transition tests**

```rust
#[tokio::test]
async fn ambiguous_resolution_quarantines_the_batch() {
    let mut store = TestStateStore::new().await;
    let batch_id = store.register_batch(sample_manifest()).await?;
    let attempt = store.begin_commit(batch_id, sample_commit_request()).await?;

    store.resolve_attempt(attempt.id, AttemptResolution::Ambiguous).await?;

    assert_eq!(store.batch_status(batch_id).await?, BatchStatus::Quarantined);
    assert_eq!(store.attempt_status(attempt.id).await?, AttemptStatus::AmbiguousManual);
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p greytl-state ambiguous_resolution_quarantines_the_batch`
Expected: FAIL because the control-plane crate does not exist yet.

- [ ] **Step 3: Create the SQLite WAL schema and repository interface**

```rust
pub trait StateStore {
    async fn register_batch(&self, manifest: BatchManifest) -> Result<BatchId>;
    async fn record_files(&self, batch_id: BatchId, files: Vec<BatchFile>) -> Result<()>;
    async fn begin_commit(&self, batch_id: BatchId, req: CommitRequest) -> Result<CommitAttempt>;
    async fn resolve_commit(&self, attempt_id: CommitAttemptId, resolution: AttemptResolution) -> Result<()>;
    async fn link_checkpoint_pending(&self, batch_id: BatchId, cp: SourceCheckpoint, snapshot: SnapshotRef) -> Result<()>;
    async fn mark_checkpoint_durable(&self, batch_id: BatchId, ack: CheckpointAck) -> Result<()>;
    async fn mark_quarantine(&self, batch_id: BatchId, reason: QuarantineReason) -> Result<()>;
    async fn list_recovery_candidates(&self) -> Result<Vec<BatchId>>;
    async fn list_orphan_candidates(&self) -> Result<Vec<BatchFile>>;
    async fn record_cleanup(&self, file: BatchFile, action: CleanupAction) -> Result<()>;
}
```

Checkpoint durability rules to implement in this task:
- `link_checkpoint_pending` writes the batch-to-snapshot linkage before any source advancement is acknowledged
- `mark_checkpoint_durable` is the only method that records the final durable advancement after the source-specific checkpoint action succeeds
- `mark_quarantine` must persist ordering, schema, and ambiguous-resolution failures so recovery can resume deterministically

- [ ] **Step 4: Implement persisted attempt statuses including `resolving`**

```rust
pub enum AttemptStatus {
    Started,
    Resolving,
    Unknown,
    Committed,
    Rejected,
    FailedRetryable,
    FailedTerminal,
    AmbiguousManual,
}
```

- [ ] **Step 5: Implement recovery transitions, reconciler APIs, checkpoint linkage, and manifest contract tests**

Run: `cargo test -p greytl-state`
Expected: PASS

Run: `cargo test -p greytl-state --test reconcile`
Expected: PASS

Run: `cargo test -p greytl-types --test manifest_contract`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/greytl-state crates/greytl-types/tests/manifest_contract.rs
git commit -m "feat: add sqlite control plane and recovery state machine"
```

### Task 3: Build The Minimal Source SDK And Reference File Source

**Files:**
- Create: `crates/greytl-source/src/lib.rs`
- Create: `crates/greytl-source/src/adapter.rs`
- Create: `crates/greytl-source/src/file_source.rs`
- Create: `crates/greytl-source/tests/source_adapter.rs`
- Create: `fixtures/reference_workload_v0/orders_events/`
- Create: `fixtures/reference_workload_v0/customer_state/`
- Create: `fixtures/reference_workload_v0/expected/`

- [ ] **Step 1: Write failing source-contract tests**

```rust
#[tokio::test]
async fn file_source_emits_monotonic_ordering_for_customer_state() {
    let source = FileSource::from_fixture_dir("fixtures/reference_workload_v0/customer_state");
    let batch = source.snapshot_next().await?;

    assert!(is_monotonic(batch.records.iter().map(|r| r.ordering_value)));
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p greytl-source --test source_adapter -- file_source_emits_monotonic_ordering_for_customer_state`
Expected: FAIL because the source SDK and fixtures are missing.

- [ ] **Step 3: Define the narrowed v0 connector SDK**

```rust
#[async_trait::async_trait]
pub trait SourceAdapter {
    async fn spec(&self) -> SourceSpec;
    async fn check(&self) -> Result<CheckReport>;
    async fn discover(&self) -> Result<DiscoverReport>;
    async fn snapshot(&self, req: SnapshotRequest) -> Result<Option<SourceBatch>>;
    async fn checkpoint(&self, ack: CheckpointAck) -> Result<()>;
}
```

- [ ] **Step 4: Implement deterministic fixture loading for both reference workloads**

```rust
pub struct FileSource {
    fixture_dir: PathBuf,
    next_batch: usize,
}

impl FileSource {
    pub async fn snapshot_next(&mut self) -> Result<SourceBatch> {
        // Load `batch-XXXX.jsonl`, parse canonical records, preserve stable ordering.
    }
}
```

- [ ] **Step 5: Run contract tests and fixture sanity checks**

Run: `cargo test -p greytl-source --test source_adapter`
Expected: PASS

Run: `just test-fast`
Expected: PASS with source-contract tests included.

- [ ] **Step 6: Commit**

```bash
git add crates/greytl-source fixtures/reference_workload_v0
git commit -m "feat: add file source and connector contract tests"
```

### Task 4: Add The In-Process DuckDB Worker And Replayable Parquet Materialization

**Files:**
- Create: `crates/greytl-worker-duckdb/src/lib.rs`
- Create: `crates/greytl-worker-duckdb/src/normalize.rs`
- Create: `crates/greytl-worker-duckdb/src/parquet_writer.rs`
- Test: `crates/greytl-worker-duckdb/src/normalize.rs`
- Test: `crates/greytl-worker-duckdb/src/parquet_writer.rs`

- [ ] **Step 1: Write failing materialization tests**

```rust
#[tokio::test]
async fn parquet_manifest_freezes_ordering_span_and_checksum() {
    let worker = DuckDbWorker::in_memory()?;
    let batch = sample_customer_state_batch();

    let output = worker.materialize(batch).await?;

    assert_eq!(output.manifest.ordering_min, 1);
    assert_eq!(output.manifest.ordering_max, 10_000);
    assert!(!output.manifest.content_hash.is_empty());
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p greytl-worker-duckdb parquet_manifest_freezes_ordering_span_and_checksum`
Expected: FAIL because the worker crate does not exist yet.

- [ ] **Step 3: Implement Arrow normalization and latest-wins reduction**

```rust
pub async fn normalize(&self, batch: SourceBatch) -> Result<NormalizedBatch> {
    validate_ordering(&batch)?;
    let arrow = source_batch_to_arrow(batch)?;
    let reduced = if batch.table_mode == TableMode::KeyedUpsert {
        latest_wins_reduce(arrow)?
    } else {
        arrow
    };
    Ok(NormalizedBatch::new(reduced))
}
```

- [ ] **Step 4: Implement Parquet writing with file rolling and row-group budgets**

```rust
pub struct WriterConfig {
    pub target_file_bytes: u64,
    pub max_row_group_bytes: u64,
}

pub async fn write_parquet(&self, batch: NormalizedBatch) -> Result<MaterializedBatch> {
    // Roll at 256 MiB target, flush row groups at 64 MiB, emit manifest.
}
```

- [ ] **Step 5: Run worker tests**

Run: `cargo test -p greytl-worker-duckdb`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/greytl-worker-duckdb
git commit -m "feat: add duckdb normalization and parquet materialization"
```

### Task 5: Implement The Sink Protocol And Deterministic `append_only` Commit Flow

**Files:**
- Create: `crates/greytl-sink/src/lib.rs`
- Create: `crates/greytl-sink/src/commit_protocol.rs`
- Create: `crates/greytl-sink/src/test_double.rs`
- Create: `crates/greytl-sink/src/filesystem.rs`
- Create: `crates/greytl-sink/tests/sink_protocol.rs`
- Create: `crates/greytl-sink/tests/deterministic_append_only.rs`
- Create: `crates/greytl-sink/tests/deterministic_recovery.rs`

- [ ] **Step 1: Write failing sink-contract tests**

```rust
#[tokio::test]
async fn duplicate_append_only_replay_reuses_the_same_commit_identity() {
    let sink = TestDoubleSink::new();
    let request = sample_append_commit_request();

    let first = sink.commit(sink.prepare_commit(request.clone())?).await?;
    let second = sink.resolve_uncertain_commit(first.attempt()).await?;

    assert_eq!(first.snapshot_id, second.snapshot_id);
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p greytl-sink --test sink_protocol -- duplicate_append_only_replay_reuses_the_same_commit_identity`
Expected: FAIL because the sink protocol is not implemented.

- [ ] **Step 3: Implement the destination-only sink trait and failpoint-capable test double**

```rust
#[async_trait::async_trait]
pub trait Sink {
    async fn prepare_commit(&self, req: CommitRequest) -> Result<PreparedCommit>;
    async fn commit(&self, prepared: PreparedCommit) -> Result<CommitOutcome>;
    async fn lookup_commit(&self, key: &IdempotencyKey) -> Result<LookupResult>;
    async fn lookup_snapshot(&self, snapshot: &SnapshotRef) -> Result<Option<SnapshotMeta>>;
    async fn resolve_uncertain_commit(&self, attempt: &CommitAttempt) -> Result<ResolvedOutcome>;
}
```

- [ ] **Step 4: Wire `append_only` through state store, worker output, the sink test double, and the Tier 1 filesystem-backed warehouse sink**

Run: `cargo test -p greytl-sink --test deterministic_append_only`
Expected: PASS for:
- happy-path append commit
- lost-ack recovery
- duplicate replay
- checkpoint advancement after durable linkage only
- deterministic local filesystem warehouse writes and reloads

- [ ] **Step 5: Run deterministic recovery tests**

Run: `cargo test -p greytl-sink --test deterministic_recovery`
Expected: PASS for:
- commit succeeds but ack is lost
- commit fails after files are written
- orphan cleanup classification
- filesystem-backed warehouse recovery after process restart

- [ ] **Step 6: Commit**

```bash
git add crates/greytl-sink
git commit -m "feat: add deterministic sink protocol and append-only commit flow"
```

### Task 6: Implement Constrained `keyed_upsert` With Equality-Delete-Plus-Append

**Files:**
- Modify: `crates/greytl-worker-duckdb/src/normalize.rs`
- Modify: `crates/greytl-sink/src/commit_protocol.rs`
- Modify: `crates/greytl-sink/src/test_double.rs`
- Create: `crates/greytl-sink/tests/deterministic_keyed_upsert.rs`

- [ ] **Step 1: Write failing `keyed_upsert` tests**

```rust
#[tokio::test]
async fn keyed_upsert_emits_equality_deletes_and_latest_rows_only() {
    let batch = sample_keyed_upsert_batch();
    let planned = plan_keyed_upsert_commit(batch)?;

    assert_eq!(planned.equality_delete_keys.len(), 3);
    assert_eq!(planned.append_rows.len(), 2);
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p greytl-sink --test deterministic_keyed_upsert -- keyed_upsert_emits_equality_deletes_and_latest_rows_only`
Expected: FAIL because the keyed-upsert path is not implemented yet.

- [ ] **Step 3: Add latest-wins reduction and tombstone handling**

```rust
pub fn plan_keyed_upsert_commit(batch: NormalizedBatch) -> Result<KeyedUpsertPlan> {
    let latest = reduce_to_latest_by_key(batch.records)?;
    Ok(KeyedUpsertPlan {
        equality_delete_keys: latest.keys().cloned().collect(),
        append_rows: latest
            .into_values()
            .filter(|row| row.op != Operation::Delete)
            .collect(),
    })
}
```

- [ ] **Step 4: Add retry-time schema-revalidation tests for keyed-upsert**

Run: `cargo test -p greytl-sink --test deterministic_keyed_upsert`
Expected: PASS for:
- latest-wins behavior
- delete tombstones with `after=null`
- duplicate replay idempotency
- ordering violation quarantine
- schema change during retry

- [ ] **Step 5: Commit**

```bash
git add crates/greytl-worker-duckdb/src/normalize.rs crates/greytl-sink
git commit -m "feat: add constrained keyed upsert flow"
```

### Task 7: Add The Runtime Coordinator, Backpressure, And Checkpoint Sequencing

**Files:**
- Create: `crates/greytl-runtime/src/lib.rs`
- Create: `crates/greytl-runtime/src/pipeline.rs`
- Create: `crates/greytl-runtime/src/backpressure.rs`
- Create: `crates/greytl-runtime/src/failpoints.rs`
- Create: `crates/greytl-runtime/tests/backpressure.rs`

- [ ] **Step 1: Write failing backpressure tests**

```rust
#[tokio::test]
async fn full_parquet_slot_pauses_source_intake_for_the_table() {
    let runtime = RuntimeHarness::with_single_table_limits();
    runtime.fill_durable_slot("customer_state").await?;

    assert!(runtime.try_admit("customer_state").await.is_paused());
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p greytl-runtime --test backpressure -- full_parquet_slot_pauses_source_intake_for_the_table`
Expected: FAIL because the runtime crate does not exist.

- [ ] **Step 3: Implement single-writer-per-table execution and queue accounting**

```rust
pub struct TableBudget {
    pub in_memory_batches: usize,
    pub durable_pending_batches: usize,
    pub unresolved_commits: usize,
}

impl TableBudget {
    pub fn can_admit(&self) -> bool {
        self.in_memory_batches < 1
            && self.durable_pending_batches < 1
            && self.unresolved_commits < 1
    }
}
```

- [ ] **Step 4: Implement checkpoint advancement only after durable linkage**

Run: `cargo test -p greytl-runtime`
Expected: PASS for:
- paused intake on per-table saturation
- global pause on recovery-queue saturation
- checkpoint blocked during unresolved commit

- [ ] **Step 5: Commit**

```bash
git add crates/greytl-runtime
git commit -m "feat: add runtime coordination and backpressure controls"
```

### Task 8: Add The CLI, Local Stack, Real-Stack Tests, And CI Entry Points

**Files:**
- Create: `crates/greytl-cli/src/main.rs`
- Create: `crates/greytl-cli/src/commands/run.rs`
- Create: `crates/greytl-sink/src/polaris.rs`
- Create: `infra/local/docker-compose.yml`
- Create: `infra/local/object-store-init.sh`
- Create: `.github/workflows/ci.yml`
- Create: `crates/greytl-sink/tests/polaris_append_only.rs`
- Create: `crates/greytl-sink/tests/polaris_keyed_upsert.rs`
- Create: `crates/greytl-sink/tests/polaris_recovery.rs`
- Modify: `justfile`

- [ ] **Step 1: Write failing CLI and real-stack smoke tests**

```rust
#[test]
fn cli_exposes_run_and_compact_subcommands() {
    let cmd = Cli::command();
    cmd.debug_assert();
    assert!(cmd.get_subcommands().any(|s| s.get_name() == "run"));
    assert!(cmd.get_subcommands().any(|s| s.get_name() == "compact"));
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p greytl-cli cli_exposes_run_and_compact_subcommands`
Expected: FAIL because the CLI crate does not exist.

- [ ] **Step 3: Implement the CLI, Polaris sink, and local-stack scripts**

```rust
#[derive(clap::Subcommand)]
enum Commands {
    Run(commands::run::Args),
    Compact(commands::compact::Args),
}
```

```rust
pub struct PolarisSink {
    catalog: PolarisCatalog,
    warehouse: ObjectStore,
}

impl PolarisSink {
    pub async fn resolve_uncertain_commit(&self, attempt: &CommitAttempt) -> Result<ResolvedOutcome> {
        // Lookup by idempotency key and snapshot metadata against Polaris.
    }
}
```

Run: `docker compose -f infra/local/docker-compose.yml config`
Expected: VALID configuration for Polaris plus the current local S3-compatible object-store backend.

- [ ] **Step 4: Add real-stack tests and CI wiring**

Run: `just stack-up`
Expected: starts Polaris plus the current local S3-compatible object-store backend locally.

Run: `just test-real-stack`
Expected: PASS for:
- `append_only` happy path and lost-ack recovery
- constrained `keyed_upsert`
- constrained `keyed_upsert` duplicate-replay idempotency
- retry-time schema revalidation against the real stack
- Polaris uncertain-commit lookup and resolution behavior

- [ ] **Step 5: Commit**

```bash
git add crates/greytl-cli crates/greytl-sink/src/polaris.rs infra/local .github/workflows/ci.yml justfile
git commit -m "feat: add cli, local stack, and real-stack test flow"
```

### Task 8b: Audit And Generalize The Local Reference Stack Contract

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-03-22-etl-engine-v0.1-architecture-design.md`
- Modify: `docs/superpowers/plans/2026-03-22-etl-engine-v0.1-implementation-plan.md`
- Modify: `infra/local/docker-compose.yml`
- Modify: `infra/local/polaris-bootstrap.sh`
- Modify: `infra/local/object-store-init.sh`
- Modify: `justfile`

- [ ] **Step 1: Verify the current Polaris path does not use presigned URLs**

Run: `rg -n "presign|presigned" crates/greytl-sink/src/polaris.rs infra/local/polaris-bootstrap.sh`
Expected: PASS with no matches.

- [ ] **Step 2: Document RustFS as the current default local backend and record the presigned URL audit finding**

- rename bootstrap helpers and env vars so they refer to `object store` first and provider second
- replace the default local backend with RustFS behind the generic object-store contract
- require a raw S3 compatibility probe against the same endpoint, bucket, credentials, and path-style settings Polaris uses
- document the presigned URL audit finding before treating RustFS as passing the spike

- [ ] **Step 3: Add a provider-swap seam before replacing the default backend**

- define the required local-stack contract for any replacement:
  - S3-compatible API surface sufficient for Polaris catalog bootstrap
  - deterministic bucket initialization for CI
  - health checks and startup semantics compatible with `just stack-up`
  - no change to the core runtime or sink crates
- verify a candidate backend against Polaris before changing the default provider

- [ ] **Step 4: Verify the local-stack contract after abstraction**

Run: `docker compose -f infra/local/docker-compose.yml config`
Expected: VALID configuration with provider-agnostic docs and scripts.

Run: `just stack-up`
Expected: starts Polaris plus the current local S3-compatible backend.

Run: `just test-real-stack`
Expected: PASS for the existing real-stack Polaris smoke suite.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/superpowers/specs/2026-03-22-etl-engine-v0.1-architecture-design.md docs/superpowers/plans/2026-03-22-etl-engine-v0.1-implementation-plan.md infra/local justfile
git commit -m "plan: generalize local object store follow-up"
```

### Task 9: Implement The Offline Compaction Utility

**Files:**
- Create: `crates/greytl-cli/src/commands/compact.rs`
- Create: `crates/greytl-cli/tests/compact.rs`
- Modify: `justfile`

- [ ] **Step 1: Write failing compaction tests**

```rust
#[tokio::test]
async fn compact_command_rewrites_small_files_and_emits_report() {
    let report = run_compact(sample_small_file_table()).await?;

    assert!(report.rewritten_files > 0);
    assert_eq!(report.mode, "offline");
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p greytl-cli --test compact -- compact_command_rewrites_small_files_and_emits_report`
Expected: FAIL because the compact command is not implemented.

- [ ] **Step 3: Implement the minimum offline compaction contract**

Inputs:
- table identifier
- warehouse URI
- minimum small-file threshold
- maximum rewrite batch size

Outputs:
- JSON report with rewritten file count, skipped file count, output snapshot id, and elapsed time

```rust
pub struct CompactReport {
    pub mode: &'static str,
    pub rewritten_files: usize,
    pub skipped_files: usize,
    pub snapshot_id: Option<String>,
    pub elapsed_ms: u64,
}
```

- [ ] **Step 4: Run compaction tests**

Run: `cargo test -p greytl-cli --test compact`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/greytl-cli/src/commands/compact.rs crates/greytl-cli/tests/compact.rs justfile
git commit -m "feat: add offline compaction utility"
```

### Task 10: Add The Benchmark Harness And PyIceberg Baseline

**Files:**
- Create: `benchmarks/pyiceberg_baseline/pyproject.toml`
- Create: `benchmarks/pyiceberg_baseline/run.py`
- Create: `benchmarks/README.md`
- Modify: `justfile`

- [ ] **Step 1: Write failing benchmark smoke checks**

```python
def test_reference_workload_names_are_supported():
    from run import REFERENCE_WORKLOADS
    assert "append_only.orders_events" in REFERENCE_WORKLOADS
    assert "keyed_upsert.customer_state" in REFERENCE_WORKLOADS
```

- [ ] **Step 2: Run smoke check to verify failure**

Run: `python -m pytest benchmarks/pyiceberg_baseline -q`
Expected: FAIL because the baseline harness does not exist.

- [ ] **Step 3: Implement the baseline runner**

```python
REFERENCE_WORKLOADS = {
    "append_only.orders_events": "fixtures/reference_workload_v0/orders_events",
    "keyed_upsert.customer_state": "fixtures/reference_workload_v0/customer_state",
}

def main() -> int:
    # Load landed Parquet fixtures, commit with PyIceberg against Polaris + the
    # local S3-compatible object-store reference stack,
    # emit throughput and p95 commit latency metrics as JSON.
    return 0
```

- [ ] **Step 4: Wire benchmark commands into `justfile` and document `G1`**

Run: `just benchmark-baseline --dry-run`
Expected: prints the workload, stack, and output paths without performing network mutations.

- [ ] **Step 5: Commit**

```bash
git add benchmarks justfile
git commit -m "feat: add G1 baseline benchmark harness"
```

## Final Verification Checklist

- [ ] `cargo test --workspace`
- [ ] `just test-fast`
- [ ] `just test-deterministic`
- [ ] `just stack-up`
- [ ] `just test-real-stack`
- [ ] `just benchmark-baseline --dry-run`
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`

## Execution Notes

- Implement tasks in order. The later tasks assume the earlier crate boundaries already exist.
- Do not broaden source coverage or start `G4` connector sufficiency work during v0.
- Keep the DuckDB boundary in-process for v0; if a task uncovers repeated crash-containment failures, stop and surface that against `G2` rather than improvising a process boundary midstream.
- Treat every recovery-path test as mandatory. A “working” happy path without the failure-path assertions does not satisfy the spec.
