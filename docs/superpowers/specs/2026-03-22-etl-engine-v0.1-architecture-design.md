# ETL Engine v0.1 Architecture Spec

Date: 2026-03-22
Status: Draft for review

## 1. Purpose

This document defines a v0.1 architecture for an open-source, Iceberg-native ingestion engine optimized for correctness first and performance second. The immediate goal is to make the system implementable, locally testable, and falsifiable under failure, with particular emphasis on commit resolution, replay, checkpoint advancement, and recovery.

The architecture is intentionally narrow in v0:

- prove the control plane and write path under failure before broadening source coverage
- use a synthetic or file-based source for the first vertical slice
- keep compaction offline initially
- treat source-reader commoditization and DuckDB viability as hypotheses to validate, not assumptions to bake in

## 2. Product Thesis And Working Hypotheses

The system is a destination-optimized ingestion engine, not a generic ELT wrapper. The differentiation target is the normalize -> file layout -> commit -> compaction path against Iceberg-native semantics.

The architecture will explicitly test these hypotheses:

- `H1`: write-path differentiation comes primarily from normalize, write, and commit behavior, not source extraction
- `H2`: DuckDB is viable as the default embedded execution engine for the reference table modes
- `H3`: single active writer per table is sufficient as the default runtime policy for v1
- `H4`: existing connector ecosystems are sufficient for an initial useful source set without forcing core-engine changes

## 3. Non-Goals For V0

- arbitrary out-of-order CDC conflict resolution
- multi-writer per table
- continuous compaction service
- broad connector coverage
- rename or drop schema automation
- partition evolution and advanced clustering
- cloud-only recovery paths that cannot be reproduced locally

## 4. System Overview

The default pipeline is:

`source adapter -> Arrow batch -> normalize/write worker -> Parquet batch + manifest -> commit coordinator -> Iceberg catalog -> reconciliation/offline compaction`

The design uses:

- Arrow for low-latency in-memory handoff only
- Parquet plus manifest as the durable replay boundary
- a durable control plane as the source of truth for batch identity, commit attempts, checkpoint linkage, and recovery
- a sink abstraction defined in terms of deterministic Iceberg commit and lookup operations, not source progression

## 5. Control Plane And Durable State Model

Correctness lives in the durable control plane. Workers execute write-path steps, but they do not own truth about whether a batch exists, whether a commit was already attempted, whether replay is safe, or whether a source checkpoint may advance.

### 5.1 Core Invariants

- A source checkpoint advances only after a destination commit is durably resolved and linked to that checkpoint.
- Every replayable batch has a stable `batch_id`, immutable manifest, and durable ordering metadata.
- Every commit attempt has a stable idempotency key derived from durable batch identity plus table mode.
- The control plane, not workers, owns commit attempt sequencing and retry state.
- A worker crash must not make commit outcome unknowable forever; recovery must be able to resume from durable state alone.
- If a failure mode cannot be simulated locally, treat that as a design smell unless there is a strong reason otherwise.

### 5.2 State Store Responsibilities

The state store must:

- persist batch registration, manifest metadata, ordering metadata, schema fingerprint, and produced file set
- persist commit intent, commit attempts, resolution status, and linked destination snapshot metadata
- persist source checkpoint candidates and final durable checkpoint advancement
- persist quarantine records for ordering violations, schema-policy violations, malformed batches, and unresolved ambiguous commits
- persist reconciliation findings such as orphan files, duplicate replays, and unresolved recovery candidates

Required interface:

```text
register_batch(batch_manifest) -> batch_id
record_files(batch_id, fileset)
begin_commit(batch_id, commit_request) -> commit_attempt
resolve_commit(commit_attempt_id, resolution)
link_checkpoint(batch_id, source_checkpoint, destination_snapshot)
mark_quarantine(batch_id, reason)
list_recovery_candidates()
list_orphan_candidates()
```

Clarifications:

- `begin_commit` is ledger-only. It writes commit intent and allocates a new attempt record. It does not touch the catalog and must not stage remote state.
- The worker executes the actual catalog mutation only after receiving a durable `commit_attempt_id`, `attempt_no`, and `idempotency_key`.
- `attempt_no` is owned and incremented by the control plane. Workers are stateless executors.

### 5.3 Commit Ledger Schema

`batches`

- `batch_id`
- `table_id`
- `table_mode`
- `source_id`
- `source_class`
- `source_checkpoint_start`
- `source_checkpoint_end`
- `ordering_field`
- `ordering_min`
- `ordering_max`
- `schema_version`
- `schema_fingerprint`
- `manifest_uri`
- `batch_status`

Allowed `batch_status` values:

- `registered`
- `files_written`
- `commit_started`
- `commit_uncertain`
- `schema_revalidating`
- `retry_ready`
- `committed`
- `checkpoint_pending`
- `checkpointed`
- `quarantined`
- `failed_terminal`

`batch_files`

- `batch_id`
- `file_uri`
- `file_kind`
- `content_hash`
- `file_size_bytes`
- `record_count`
- `created_at`

`commit_attempts`

- `commit_attempt_id`
- `batch_id`
- `attempt_no`
- `idempotency_key`
- `catalog_target`
- `request_payload_hash`
- `attempt_status`
- `catalog_snapshot_id` nullable
- `catalog_commit_token` nullable
- `error_code` nullable
- `started_at`
- `resolved_at`

Allowed `attempt_status` values:

- `started`
- `unknown`
- `committed`
- `rejected`
- `failed_retryable`
- `failed_terminal`
- `ambiguous_manual`

`checkpoint_links`

- `batch_id`
- `source_checkpoint`
- `destination_snapshot_id`
- `linked_at`
- `ack_status`

Allowed `ack_status` values:

- `pending`
- `durable`

`ack_status=durable` means the checkpoint link is durably recorded in the state store and the source-specific checkpoint advancement action has either been durably persisted locally or acknowledged by the source system, depending on source type.

`quarantine_records`

- `batch_id`
- `reason_code`
- `details`
- `opened_at`
- `resolved_at` nullable

### 5.4 Sink And Commit Protocol

The sink abstraction is destination-only. It does not advance source state.

Required interface:

```text
prepare_commit(commit_request) -> prepared_commit
commit(prepared_commit) -> commit_result | uncertain
lookup_commit(idempotency_key) -> found | not_found | ambiguous
lookup_snapshot(snapshot_ref) -> snapshot_metadata | not_found
resolve_uncertain_commit(commit_attempt) -> committed | not_committed | ambiguous
```

Properties:

- `prepare_commit` is local and side-effect-free with respect to the catalog
- `prepare_commit` may validate request structure, canonicalize metadata, verify file sets, and compute derived commit payloads
- `prepare_commit` must not create remote catalog state, reserve transactions, or begin a two-phase commit protocol
- `commit` is the only operation allowed to attempt an Iceberg mutation
- `lookup_commit` and `resolve_uncertain_commit` are required for recovery; a target that cannot support them sufficiently fails compatibility

### 5.5 Recovery State Machine

Batch-level state machine:

```text
REGISTERED
-> FILES_WRITTEN
-> COMMIT_STARTED
-> COMMITTED | COMMIT_UNCERTAIN | SCHEMA_REVALIDATING | FAILED_TERMINAL

COMMIT_UNCERTAIN
-> RESOLVING
-> COMMITTED | AMBIGUOUS_MANUAL | SCHEMA_REVALIDATING

SCHEMA_REVALIDATING
-> RETRY_READY | QUARANTINED

RETRY_READY
-> COMMIT_STARTED

COMMITTED
-> CHECKPOINT_PENDING
-> CHECKPOINTED

ANY
-> QUARANTINED
```

Attempt-level semantics:

- `failed_retryable` is an attempt status, not a batch status
- when an attempt is `failed_retryable`, the batch transitions immediately to `schema_revalidating`
- `RESOLVING`, `failed_retryable`, and `ambiguous_manual` are tracked at the attempt layer even when they affect the next batch transition

Rules:

- schema policy must be re-evaluated before every retry after a failed or unresolved commit attempt
- if schema policy passes, the batch moves to `retry_ready`
- if schema policy fails, the batch moves to `quarantined`
- `ambiguous_manual` is terminal in v0; policy is quarantine plus manual resolution with checkpoint advancement blocked

### 5.6 Checkpoint Linkage

Checkpoint linkage is explicit and post-commit:

- a batch may contain a candidate checkpoint or watermark range
- candidate checkpoint data is not source advancement
- after a destination snapshot is durably resolved, the control plane writes the checkpoint linkage record
- only after that link is durable may the source checkpoint advance
- duplicate replay of an already committed batch must converge on the existing batch identity and checkpoint link, not create a second advancement

### 5.7 Orphan Cleanup And Reconciliation

Reconciliation is a first-class loop.

The reconciler must:

- scan ledger states such as `files_written`, `commit_uncertain`, and `ambiguous_manual`
- compare ledger state against visible catalog snapshots
- classify files as committed, staged-but-uncommitted, or orphaned
- clean up only files unreferenced by both committed ledger state and visible snapshot state
- durably record cleanup actions and findings
- verify that every durable checkpoint link maps to a visible destination snapshot

## 6. Supported Table Modes

Table behavior is defined by named modes. In v0, the runtime policy is single active writer per table. That reduces catalog conflict handling while the system proves commit, recovery, replay, and checkpoint correctness. The protocol remains compatible with future multi-writer support because commit identity and resolution are explicit rather than implied by exclusivity.

V0 must fully implement `append_only`. V0 may implement `keyed_upsert` only as a constrained first-class mode under ordered fixtures and ordered-source contracts. V1 must support both modes as first-class behaviors.

### 6.1 `append_only`

Purpose:

- insert-only ingestion for logs, snapshots materialized as append, object or file drops, and sources where mutation semantics are not required

Guarantees:

- each committed batch is applied at most once logically
- replay converges on the same committed batch identity
- row-level deduplication is not provided by this mode

Write semantics:

- normalized rows are written as data files and committed via Iceberg append semantics

Allowed logical operations:

- `insert`

Invalid operations:

- `update` is rejected before commit
- `delete` is rejected before commit

### 6.2 `keyed_upsert`

Purpose:

- maintain latest state by key for mutable source tables under ordered change input

Assumptions and semantics:

- for a given key, changes arrive in source order or are pre-ordered before entering the write path
- latest record by durable source position or watermark wins
- source position must be durable in the canonical model and batch manifest so replay is deterministic
- arbitrary out-of-order CDC conflict resolution is a non-goal for v0 and v1

Guarantees:

- under ordered input and single active writer per table, replaying the same committed batch produces the same table result
- within a batch and across batches, later source position wins for a key

Write semantics:

- the engine reduces each batch to the key's latest applicable mutation by ordering field
- the sink translates logical upserts and tombstones into the supported Iceberg mutation mechanism for the target path
- v0 requires a path that can prove equality-delete-capable semantics or a functionally equivalent mechanism in the compatibility matrix

Allowed logical operations:

- `upsert`
- `delete`

Update handling:

- logical updates are normalized to `upsert` carrying the latest after-image row

Delete handling:

- deletes are represented logically as key-scoped tombstones
- the exact delete mechanism is a compatibility-matrix requirement for the chosen engine and catalog path

Ordering violations:

- if ordering is missing, the batch is rejected before write-path execution
- if ordering is detectably violated, the batch is quarantined

Late arrivals:

- delayed earlier changes arriving after a later change has committed are out of scope for v0 and v1 and are treated as contract violations

## 7. Two-Layer Data Model

The logical mutation model defines meaning. The physical batch contract defines how that meaning moves through the hot path efficiently. These are related but not identical.

### 7.1 Logical Mutation Model

Required fields:

- `table_id`
- `source_id`
- `source_class`
- `table_mode`
- `op`
- `key`
- `after`
- `before` optional
- `ordering_field`
- `ordering_value`
- `source_checkpoint`
- `source_event_id` optional
- `schema_version`
- `ingestion_ts`
- `source_metadata`

Definitions:

- `key` is a structured composite key, not a pre-hashed surrogate
- `before` is reserved for future reconciliation, audit, and richer CDC modes; it is not required by the v0 write path
- `ordering_field` and `ordering_value` are mandatory for `keyed_upsert`
- `source_checkpoint` must be durable enough to make replay deterministic

### 7.2 Source Ordering Contract

Each connector must declare the exact field that defines ordering for its source class.

`database_cdc`

- ordering field is source log position plus intra-transaction sequence, such as LSN, SCN, or binlog coordinates
- connector must surface a total order sufficient for per-key `latest_wins`
- if ordering is missing, the source is not eligible for `keyed_upsert`
- ordering violations go to quarantine

`message_log_or_queue`

- ordering field is partition or offset plus any source-defined per-record sequence
- source key partitioning or upstream pre-ordering must guarantee per-key order before the write path
- if that guarantee is absent, only `append_only` is allowed
- ordering violations go to quarantine

`file_or_object_drop`

- ordering field is explicit business version, event timestamp plus deterministic row ordinal, or landing-assigned manifest sequence
- if no deterministic per-key ordering exists, only `append_only` is allowed
- structural absence of ordering causes validation failure
- data-derived ordering violations go to quarantine

`api_incremental`

- ordering field is cursor or watermark plus deterministic item ordinal within page or batch
- connector must prove stable replay ordering for `keyed_upsert`
- if ordering is missing, only `append_only` is allowed
- ordering violations go to quarantine

### 7.3 Physical Batch Contract

Arrow batch, in-memory only:

- used for low-latency handoff between source adapter and normalization or write stages
- not replay-safe
- metadata should be flattened into columns rather than encoded as a row-oriented envelope
- canonical metadata columns should include:
  - `_gb_op`
  - `_gb_key_*`
  - `_gb_order_field`
  - `_gb_order_value`
  - `_gb_source_checkpoint`
  - `_gb_schema_version`
  - `_gb_source_event_id` optional
  - `_gb_ingestion_ts`
- `_gb_key_*` columns are the columnar projection of the structured logical `key`
- an optional `_gb_key_hash` may be added for grouping or partition-local optimization, but it is not the canonical key
- payload columns are the target-row columns for `after`

If a worker crashes before Parquet materialization, the Arrow batch is abandoned and the pipeline restarts from the last durable source checkpoint.

Parquet batch, durable:

- this is the replayable unit for v0
- each replayable batch must be materialized to Parquet before commit attempt
- the Parquet payload mirrors the Arrow physical contract and is frozen by a manifest

Required manifest fields:

- `batch_id`
- `table_id`
- `table_mode`
- `source_id`
- `source_class`
- `source_checkpoint_start`
- `source_checkpoint_end`
- `ordering_field`
- `ordering_min`
- `ordering_max`
- `schema_version`
- `schema_fingerprint`
- `record_count`
- `op_counts`
- `file_set`
- `content_hash`
- `created_at`

### 7.4 Replay Boundary

The v0 replay boundary is `Parquet batch + manifest`.

Reasons:

- durable across process crash and worker restart
- engine-neutral and inspectable during reconciliation
- freezes file set, schema fingerprint, ordering metadata, and checkpoint span for deterministic retry
- keeps Arrow ABI and FFI details out of the recovery contract
- supports deterministic local testing and failure injection

## 8. Catalog Compatibility Matrix

The compatibility matrix is generic. The v0 reference target is DuckDB plus Polaris.

The matrix must test named table modes directly, not generic file-writing success.

Required reference cases:

- `append_only.insert`
- `append_only.replay_idempotency`
- `append_only.uncertain_commit_recovery`
- `keyed_upsert.upsert_latest_wins`
- `keyed_upsert.delete`
- `keyed_upsert.replay_idempotency`
- `keyed_upsert.ordering_violation_quarantine`
- `keyed_upsert.schema_change_during_retry`

A mode is viable only if its recovery, replay, and failure-path rules pass against the target catalog path.

## 9. Local Test Strategy

Testability is a design constraint. Core correctness paths must run locally without cloud dependencies.

### 9.1 Test Tiers

Tier 0, pure unit tests:

- control-plane ledger transitions and invariants
- checkpoint advancement rules
- idempotency key generation and duplicate replay convergence
- schema-policy evaluation
- ordering-field validation and quarantine decisions
- canonical event model validation
- batch manifest validation
- table-mode validation rules

Tier 1, deterministic integration tests:

- single-process or single-node tests with local filesystem-backed warehouse storage
- local state-store backend
- deterministic source fixtures, manifest fixtures, schema fixtures, and expected ledger transitions
- sink test double with failpoints for `commit`, `lookup_commit`, and `resolve_uncertain_commit`
- required failure-path coverage:
  - commit succeeds but ack is lost
  - commit fails after files are written
  - retry after uncertain commit
  - duplicate batch replay
  - worker crash between file write and commit
  - schema change during replay or retry
  - orphan file detection and cleanup

Tier 2, real catalog and object-store integration tests:

- single-node local stack
- local object store or filesystem-backed warehouse
- local catalog target where possible
- v0 reference stack should target Polaris plus local object store when practical
- reuse the same fixtures and expected outcomes as Tier 1
- required contract coverage:
  - sink and commit protocol behavior against the real catalog
  - `append_only` mode behavior
  - constrained `keyed_upsert` behavior
  - catalog-visible uncertain-commit resolution
  - delete or update path viability for the chosen `keyed_upsert` mechanism

Tier 3, local performance and soak tests:

- bounded-batch throughput under configured memory budgets
- backpressure propagation under catalog slowness
- file sizing and row-group policy conformance
- repeated failure and recovery cycles to expose FFI, resource-leak, or ledger-corruption issues

### 9.2 CI Policy

- Tier 0 and Tier 1 run on every pull request
- Tier 2 runs on every pull request for the reference fixture set
- Tier 3 runs nightly or by explicit opt-in

### 9.3 Developer Ergonomics

- one command for the fast local suite
- one command for the deterministic integration suite
- one command for the real local stack plus integration suite
- small default fixtures that run fast
- heavier soak and performance fixtures separated from the fast path
- CI must reuse the same fixtures, contracts, and expected ledger transitions used locally

### 9.4 Determinism Requirements

- injectable clock
- injectable id generator
- injectable failpoints
- stable fixture manifests and expected ledger snapshots
- no test may depend on wall-clock sleeps for correctness
- if a failure mode cannot be simulated locally, treat that as a design smell unless there is a strong reason otherwise

## 10. Backpressure And Queueing Model

V0 enforces pressure per table first, with a global process memory cap above that. Per-source fairness is deferred.

### 10.1 Bounded Queues

- `source_to_arrow`: at most one in-memory batch per table
- `arrow_to_parquet`: at most one normalized in-memory batch per table
- `parquet_pending`: at most one durable pending replayable batch per table
- `recovery_queue`: globally bounded for unresolved commit and reconciliation work

No unbounded channels are allowed in the hot path.

If `parquet_pending` is occupied by a batch in retry or uncertain resolution, the table is full and new source intake for that table pauses.

If `recovery_queue` is saturated, the system pauses admission of new commit work globally until recovery backlog drops below threshold. Recovery work takes priority over admitting new work.

### 10.2 Memory Budgets

- `B_global`: total process memory budget
- `B_table`: per-table active-memory budget
- `B_batch`: maximum in-memory Arrow batch size before forced materialization or source pause

A table may not admit a new in-memory batch if doing so would exceed either `B_table` or `B_global`.

### 10.3 Minimum Viable V0 Policy

- single active writer per table
- one in-memory active batch per table
- one durable pending replayable batch per table
- one unresolved commit attempt per table at a time
- if catalog slowness fills the durable slot, source intake pauses for that table
- retry uses bounded exponential backoff, but no retry is allowed while commit outcome remains unresolved
- checkpoint advancement never bypasses backpressure or recovery state

## 11. Physical Layout Policy

File shape is part of the product thesis and must be specified from the start.

### 11.1 Partitioning Default

- default hidden partition transform is ingestion day
- event-time day is allowed only when the connector can prove event-time quality and replay stability
- business-key partitioning is out of scope for v0

### 11.2 File And Row-Group Targets

- target Parquet file size is `256 MiB`
- acceptable v0 file size band is `128 MiB` to `512 MiB`
- target row-group size is `64 MiB`
- the writer is responsible for rolling files before the configured target size and flushing row groups within the configured row-group budget

### 11.3 Sort And Clustering Assumptions

- v0 provides only batch-local ordering
- default local sort is partition columns first
- for `keyed_upsert`, local sort should include key columns and ordering value when cost is acceptable
- no global clustering guarantee in v0
- no adaptive sort strategy in v0

### 11.4 Compaction Thresholds

- files smaller than `64 MiB` are compaction candidates
- partitions with more than `32` active data files in the compaction window are compaction candidates
- if `keyed_upsert` is enabled, delete-file accumulation must also be tracked as a compaction signal

### 11.5 V0 Versus Deferred

In v0:

- partition default
- file-size target
- row-group target
- file-layout validation
- offline compaction utility

Deferred:

- continuous compaction service
- adaptive partition evolution
- advanced clustering
- cost-based rewrite planning
- multi-writer layout coordination

## 12. Architecture Gates

The gates are numbered and normative. They are intended to stop architecture drift.

Two benchmark inputs are required before measurement begins:

- `reference_workload_v0`
- `R_target_v0`

`reference_workload_v0` must define at least:

- one `append_only` table and one `keyed_upsert` table
- fixed schema width and approximate row width
- fixed batch size and batch count
- fixed source-ordering characteristics
- fixed failure-injection scenarios
- fixed local catalog and object-store stack

`R_target_v0` must define the minimum acceptable steady-state throughput target for the single-writer runtime on the chosen reference workload.

### G0. Correctness Gate

Pass:

- all Tier 1 deterministic failure-path tests pass
- the same core scenarios pass in Tier 2 against the local real catalog stack

Fail:

- any unresolved ambiguity in checkpoint advancement, duplicate replay, or uncertain commit resolution

This gate must pass before claiming v0 correctness.

### G1. Write-Path Differentiation Gate

Mapped hypothesis:

- `H1`

Pass:

- on identical landed-Parquet fixtures from `reference_workload_v0`, the engine shows a material advantage over a baseline path while preserving correctness guarantees
- provisional target is either `>= 2x` committed rows per second or `>= 40%` lower p95 commit-path latency

Fail:

- improvement is marginal
- improvement disappears under recovery instrumentation
- improvement depends on skipping correctness features

This gate informs the product thesis but does not override `G0` or `G2`.

### G2. DuckDB Viability Gate

Mapped hypothesis:

- `H2`

Pass:

- the DuckDB-based path passes the catalog compatibility matrix for `append_only`
- the DuckDB-based path passes the constrained `keyed_upsert` reference path
- uncertain-commit recovery, duplicate replay, schema revalidation on retry, and local failure injection all pass

Fail:

- a required mode cannot be implemented correctly
- equality-delete-capable semantics or equivalent are unavailable for the chosen `keyed_upsert` path
- the Rust or DuckDB boundary shows unrecoverable crash, ownership, or retry-poisoning behavior

This gate must pass before proceeding beyond v0 with DuckDB as the default engine.

### G3. Single-Writer Runtime Gate

Mapped hypothesis:

- `H3`

Pass:

- one active writer per table sustains `R_target_v0` for a 30-minute run on `reference_workload_v0`
- no queue overflows, memory-budget breaches, unresolved commit overlap, or checkpoint lag beyond the configured bound occur

Fail:

- backlog grows unbounded
- queues saturate persistently
- throughput requires immediate multi-writer support

This gate must pass before declaring single-writer per table the v1 default runtime policy.

### G4. Connector Sufficiency Gate

Mapped hypothesis:

- `H4`

Pass:

- at least two reference sources from different source classes integrate through the connector SDK without modifying normalization or commit core
- each source can provide the required checkpoint and ordering contract for its chosen table mode

Fail:

- borrowed or adapted connectors require core-engine changes
- connectors cannot surface deterministic checkpoint or ordering data

This gate is not required to prove v0 correctness, but it is required before broad source claims.

## 13. Narrowed V0 Scope

In scope:

- synthetic or file-based source only
- full `append_only` implementation
- constrained first-class `keyed_upsert` implementation on ordered fixtures only
- durable control plane and commit ledger
- Parquet batch plus manifest replay boundary
- deterministic local failure injection
- local real-stack testing with a single-node catalog and object-store setup where practical
- reconciliation and orphan cleanup
- offline compaction utility

Out of scope:

- arbitrary out-of-order CDC conflict resolution
- multi-writer per table
- continuous compaction service
- broad connector coverage
- rename or drop schema automation
- partition evolution and advanced clustering

## 14. Next Planning Inputs

The next planning draft should answer these implementation-facing questions:

- what concrete state-store backend will serve as the v0 local and CI reference
- what exact local catalog and object-store stack will serve as the Tier 2 default
- what delete path will be used for constrained `keyed_upsert` in the DuckDB plus Polaris matrix
- what concrete values define `reference_workload_v0`
- what concrete target defines `R_target_v0`
- whether the Rust and DuckDB boundary should remain in-process in v0 or move to a process boundary for containment
