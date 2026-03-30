# Post-v0 Source Adapter v1 Design

Date: 2026-03-27
Last updated: 2026-03-28
Status: Accepted

## 1. Purpose

This document defines the post-v0 direction for widening `iceflow` from a deterministic file-backed reference source to a general source SDK that can support real database CDC and other source classes without forcing changes into the worker, state-store, runtime, or sink cores.

The immediate goal is not to implement a Postgres connector next. The goal is to pin the architectural contract that future source work should follow now that Task 10 and the narrowed v0 closeout work are complete.

## 2. Why This Is Not A Live Connector Task Yet

This design is intentionally post-v0.

The repo has now completed the gating work that originally blocked this design from becoming actionable:

- Task 10 benchmarking and baseline work from the v0 implementation plan
- final v0 verification and closeout for the narrowed write-path slice
- branch-wide cleanup needed to restore the strict reviewer and validation gates

That means steps 1 and 2 in the sequencing section are now valid to start:

- generalize the shared source contract
- port `FileSource` onto that contract without changing behavior

What is still not true is that the repo is ready for a live database connector rollout. There are still runtime-product gaps between the current repo and a real live-source execution model:

- the current CLI `run` path is workload-driven and finite, not a long-running connector service
- the current SQLite state store is created under a temp path per invocation, not a configured long-lived control-plane database
- the current CLI contract selects built-in workloads, not connector configuration
- real connector operations will require connector-specific auth, configuration, and operator-facing observability that v0 does not yet expose

This spec therefore defines the target contract and sequencing constraints for post-v0 source work. It does not claim the repo is ready to start Postgres snapshot ingestion or Postgres CDC immediately after the source-contract refactor begins.

## 3. Current State

Today the source layer is intentionally narrow:

- `iceflow-types` defines the canonical mutation model in `LogicalMutation`
- `iceflow-source` exports one concrete source: `FileSource`
- `iceflow-cli run` always uses `FileSource` and built-in fixture workloads
- the rest of the engine already reasons in replayable batches, commit attempts, and checkpoint advancement

This is the right narrowed v0 shape. The main architectural issue is not the existence of a source trait. The issue is that the current shared source types in `crates/iceflow-source/src/adapter.rs` leak file-fixture assumptions into the generic interface:

- `fixture_dir`
- `batch_count`
- `batch_files`
- `batch_index`
- `batch_file`
- `snapshot()` as the main read verb

Those fields and names are valid for `FileSource`, but they are not a durable contract for Postgres CDC, SQL Server CDC, MongoDB change streams, object-drop ingestion, or incremental APIs.

## 4. Design Goals

- preserve the narrowed v0 write-path architecture
- keep `LogicalMutation` as the near-term canonical cross-source contract
- generalize the source boundary without prematurely overfitting to one connector
- support both finite and continuous sources behind the same runtime-facing batch contract
- make source checkpoints connector-owned and opaque to the core engine
- allow deliberate replay overlap while forbidding missed changes
- keep `FileSource` as a first-class file or object-drop connector, not temporary scaffolding

## 5. Non-Goals

- implementing a Postgres connector in this task
- implementing direct MongoDB or SQL Server connectors in this task
- replacing `LogicalMutation.after` and `before` with a new typed row representation now
- changing worker, sink, or commit-ledger semantics for connector convenience
- designing a multi-writer or fully continuous runtime in this document

## 6. Baseline Architectural Decision

The canonical cross-source contract remains `LogicalMutation` in `iceflow-types`.

That choice is correct for the next phase because it already captures the source invariants the rest of the engine needs:

- operation
- key
- ordering
- source checkpoint
- source event id
- schema version
- source metadata

This is sufficient for the next source-contract refactor. It should be treated as good enough for the next step, not as an irreversible forever model.

## 7. Source Boundary v1

### 7.1 Boundary Principles

The engine-facing source API remains batch-oriented.

That means:

- `iceflow-runtime` still admits and sequences work in batches
- `iceflow-worker-duckdb` still receives `SourceBatch` values
- `iceflow-state` still links durable destination commits to source checkpoint advancement
- `iceflow-sink` remains destination-only and source-agnostic

Push-native connectors such as Postgres logical replication should hide their push mechanics behind connector-owned buffering and expose drained batches to the runtime. The core engine should not grow source-specific callbacks or push-driven orchestration.

### 7.2 Proposed Shared Source Types

The shared source SDK should move away from fixture-specific fields and toward capability- and checkpoint-oriented types.

Suggested shape:

```rust
pub struct SourceSpec {
    pub source_id: String,
    pub source_class: SourceClass,
    pub table_id: String,
    pub table_mode: TableMode,
    pub ordering_field: Option<String>,
    pub capabilities: BTreeSet<SourceCapability>,
}

pub enum SourceCapability {
    AppendOnly,
    KeyedUpsert,
    InitialSnapshot,
    ChangeFeed,
    SnapshotHandoff,
    Deletes,
    BeforeImages,
    Resume,
    DeterministicCheckpoints,
    StableLatestWinsOrdering,
}

pub struct SourceHealth {
    pub status: &'static str,
    pub detail: Option<String>,
}

// Replaces the older generic `SnapshotRef` name in the source SDK with an
// explicitly destination-oriented name to avoid confusion with source-side
// snapshot and checkpoint concepts.
pub struct DestinationSnapshotRef {
    pub uri: String,
}

pub struct CheckpointAck {
    pub source_id: String,
    pub checkpoint: CheckpointId,
    pub destination_snapshot: DestinationSnapshotRef,
}

pub struct BatchRequest {
    pub resume_from: Option<CheckpointId>,
    pub max_records: Option<usize>,
    pub max_bytes: Option<u64>,
}

pub enum BatchPoll {
    Batch(SourceBatch),
    Idle,
    Exhausted,
}

pub struct SourceBatch {
    pub batch_label: Option<String>,
    pub checkpoint_start: Option<CheckpointId>,
    pub checkpoint_end: CheckpointId,
    pub records: Vec<LogicalMutation>,
}
```

Important properties:

- `CheckpointId` remains connector-owned and opaque to the engine
- `DestinationSnapshotRef` is the destination-oriented replacement for the older ambiguous `SnapshotRef` name in the source SDK
- `CheckpointAck.destination_snapshot` remains required because this ack models post-commit checkpoint advancement after durable destination linkage, not generic source progress heartbeats
- `batch_label` is diagnostic only, not part of correctness
- `Idle` and `Exhausted` are distinct so continuous connectors and finite sources can share one contract cleanly
- `resume_from` does not imply the engine understands the internal structure of the checkpoint token

### 7.3 Proposed Trait Shape

Suggested direction:

```rust
#[allow(async_fn_in_trait)]
pub trait SourceAdapter {
    async fn spec(&self) -> Result<SourceSpec>;
    async fn check(&self) -> Result<SourceHealth>;
    async fn poll_batch(&self, req: BatchRequest) -> Result<BatchPoll>;
    async fn checkpoint(&self, ack: CheckpointAck) -> Result<()>;
}
```

`discover()` can be retained only if it becomes source-generic operator introspection rather than fixture discovery. It is not required for the core ingestion contract.

`snapshot()` should be retired as the main read verb because it incorrectly implies a one-shot snapshot model for sources that are actually incremental or continuous.

`spec()` should usually be derivable from connector configuration and local state, but returning `Result<SourceSpec>` avoids forcing a trait break later if configuration validation or lazy metadata resolution can fail.

## 8. Checkpoint And Replay Semantics

The source checkpoint token is connector-owned and opaque to the engine.

The core engine may store it, persist which checkpoint has become durable, and hand it back to the connector for resume. It must not hardcode Postgres LSN structure, MongoDB resume token semantics, or SQL Server CDC details into `iceflow-runtime`, `iceflow-state`, or `iceflow-sink`.

The required invariants are:

- no missed changes
- deterministic resume from the last durable source checkpoint
- stable ordering sufficient for the chosen table mode
- replay overlap, if present, is deliberate and safe

The engine should prefer safe overlap over risky gaps. Duplicate replay is acceptable when the connector contract makes replay deterministic and idempotent.

## 9. Capabilities And Mode Negotiation

The next source contract should expose source capabilities explicitly rather than smuggling them through connector names or documentation.

The important capabilities are:

- `AppendOnly`
- `KeyedUpsert`
- `InitialSnapshot`
- `ChangeFeed`
- `SnapshotHandoff`
- `Deletes`
- `BeforeImages`
- `Resume`
- `DeterministicCheckpoints`
- `StableLatestWinsOrdering`

This matters because not all connectors should qualify for all table modes.

Examples:

- a file-drop source may support `append_only` but not `keyed_upsert`
- a snapshot-only database source may support initial backfill but not continuous sync
- a CDC source without stable ordering should not be eligible for `keyed_upsert`

## 10. FileSource Positioning

`FileSource` should remain a first-class connector and evolve into the file or object-drop implementation of the generic source contract.

It should not be treated as legacy test scaffolding.

Reasons:

- file or object-drop ingestion is a real source class
- the repo already models that source class explicitly through `SourceClass::FileOrObjectDrop`
- keeping `FileSource` alive is the easiest way to prove the refactored source contract still supports deterministic local testing

The refactor goal is to remove file-specific details from the shared trait, not to remove the file-source implementation.

## 11. Postgres Source Acceptance Criteria

A future Postgres source should not be accepted based on “it can connect to Postgres” or “it can read rows.”

The real acceptance criteria are:

- `P1`: a consistent initial snapshot can be produced
- `P2`: the CDC start position after the snapshot is deterministic
- `P3`: no source changes are missed across the snapshot-to-CDC handoff
- `P4`: any replay overlap is deliberate and safe
- `P5`: ordering is stable enough for `keyed_upsert`, effectively log position plus intra-transaction sequence
- `P6`: restart from the last durable checkpoint is deterministic
- `P7`: checkpoint advancement only occurs after destination commit resolution is durable in `iceflow-state`
- `P8`: crash recovery can resume from control-plane state plus connector checkpoint without manual intervention

The hardest correctness milestone is the snapshot-to-CDC handoff, not the initial database read.

## 12. Sequencing Recommendation

Recommended sequence:

1. Generalize `iceflow-source` shared types and trait to remove fixture-specific assumptions.
2. Port the current `FileSource` implementation onto the new source contract without changing behavior.
3. Add a Postgres snapshot or full-refresh connector to prove the generalized source boundary against a live database.
4. Add Postgres CDC with explicit validation of snapshot-to-CDC handoff, durable checkpointing, restart behavior, and replay safety.

This order is intentionally conservative.

Steps 1 and 2 are now valid to execute against the current repo.

Step 3 proves connector mechanics and configuration boundaries.
Step 4 proves mutable-source correctness.

## 13. Post-v0 Prerequisites Before Live Connector Implementation

Before starting step 3, the project should explicitly decide how the following gaps will be handled:

- promote the SQLite control plane from per-run temp-path behavior to a configured persistent store for real connector runs
- define a connector configuration surface for CLI or service execution
- decide whether live-source execution remains a CLI-run process or becomes a long-running service
- define operator-visible health and progress reporting for live connectors

These are not arguments against the source-contract refactor. They are prerequisites for executing the first real live connector safely after the source boundary has been generalized.

## 14. Verification Expectations For The Refactor

The first post-v0 source-contract task should prove:

- `FileSource` still passes deterministic source-adapter contract tests after the trait refactor
- the runtime still receives replayable batches without source-specific logic
- checkpoint advancement rules remain unchanged
- no worker, sink, or state-store interfaces need source-brand-specific branches

The first live database source task should prove:

- durable restart from stored checkpoints
- no missed changes across handoff
- safe replay overlap
- deterministic ordering under the narrowed `keyed_upsert` contract

## 15. Deferred Questions

This design intentionally leaves these decisions for later measurement-driven work:

- whether `LogicalMutation.after` and `before` should move off `serde_json::Value`
- whether source polling should become streaming internally for performance reasons
- whether `discover()` remains part of the generic trait
- whether explicit `start()` and `stop()` lifecycle hooks belong on the shared source trait once the post-v0 execution model is defined
- whether Postgres snapshot and CDC should live in one connector or two staged implementations

Those questions should be answered after the source contract is generalized and at least one live connector exists to measure against.
