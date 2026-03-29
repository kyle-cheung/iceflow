# Task 9 Offline Compaction Design

## Goal

Add a catalog-shaped offline compaction command to `iceflow-cli` that rewrites small files for `append_only` tables, emits a machine-readable `CompactReport`, and stays explicitly scoped to the current iceflow-local snapshot model rather than pretending to use full Iceberg table semantics.

## Scope

In scope for Task 9:

- offline CLI compaction only
- `append_only` tables only
- generic CLI inputs for warehouse and catalog context
- a private compaction catalog seam inside `iceflow-cli`
- one implementation today: `PolarisCompactionCatalog`
- deterministic active-file reconstruction from existing iceflow-local snapshot and compaction records
- JSON compaction report output

Out of scope for Task 9:

- `keyed_upsert` compaction
- delete-file merge semantics
- real Iceberg `CreateTable`, `LoadTable`, manifest-list reads, or `CommitTransaction`
- OCC assertions against real Iceberg snapshot ids
- promoting the compaction catalog seam into `iceflow-sink` or a new shared crate

## Current Constraint

The current Polaris path is not yet a real Iceberg table integration. `PolarisSink` performs Polaris config and namespace operations and records iceflow-local commit state, but it does not create or mutate Iceberg table metadata that a real `LoadTable` call could read. Task 9 therefore compacts against the current iceflow-local snapshot model and documents that `CompactReport.snapshot_id` is a iceflow-local concept for now.

The full Polaris/Iceberg upgrade is a follow-on task:

- table creation
- `LoadTable`
- manifest-list reads
- OCC commit assertions
- mock Polaris server expansion for table APIs

## Architecture

Task 9 uses a private CLI-local abstraction rather than a shared catalog API.

- `iceflow-cli::commands::compact` owns a private `CompactionCatalog` trait
- Task 9 ships one adapter: `PolarisCompactionCatalog`
- the CLI surface stays generic even though Polaris is the only backend today
- `PolarisSink` is not refactored into a general compaction API; the compactor uses a dedicated adapter with compaction-shaped responsibilities

This avoids prematurely publishing an abstraction with only one implementation while still keeping the CLI contract provider-neutral.

## CLI Contract

The compact command should accept generic, catalog-shaped inputs:

- `--warehouse-uri`
- `--catalog-uri`
- `--catalog`
- `--namespace`
- `--table`
- `--table-mode`
- `--min-small-file-bytes`
- `--max-rewrite-files`

Task 9 executes only for `append_only`.

Any non-`append_only` request must fail at runtime with a clear operator-facing message, for example:

`compaction of keyed_upsert tables is not supported in this version`

This must be a normal error return, not a panic and not a silent no-op.

## CompactionCatalog Shape

The trait stays private to `compact.rs` for Task 9, but it should already be mode-aware so the eventual `keyed_upsert` path does not force a redesign.

Required responsibilities:

- validate the target context
- load existing iceflow-local snapshot and compaction history for the table
- build the active-file set deterministically
- persist a committed compaction record after replacement files are written

Task 9 does not add a second source of truth such as a mutable local head pointer.

## Validation And Preflight

`PolarisCompactionCatalog` preflight must be concrete and narrow:

- verify Polaris is reachable
- verify the target namespace exists
- verify the target table root already contains iceflow snapshot history in the current local model

If any preflight check fails, the command must abort before rewriting files with a clear error.

Task 9 does not require “table is registered in Polaris” because the current Polaris path does not yet create catalog table objects. Adding that check would widen this task into the deferred real-Iceberg integration task.

## Execution Flow

Task 9 compaction runs against the existing iceflow-local snapshot model:

1. Validate CLI arguments and preflight the Polaris namespace plus local table history.
2. Reject any non-`append_only` request with the explicit runtime error described above.
3. Load existing iceflow-local snapshot records for the target table.
4. Load existing compaction records for the target table.
5. Reconstruct the active data-file set deterministically:
   - start from committed files in the base iceflow snapshot history
   - replay compaction records in order
   - remove files listed in each record’s `removed_files`
   - add files listed in each record’s `added_files`
6. Select compaction candidates from the active set using the configured small-file threshold and rewrite-file limit.
7. Rewrite selected files into fewer replacement files.
8. Persist a new compaction record, which is the commit boundary for Task 9.
9. Emit a JSON `CompactReport`.

The command should be a clean no-op when no active files qualify.

## Compaction Record Format

Task 9 must pin both record location and record shape.

Interpret `warehouse_uri` as the current iceflow table root for the target adapter.

Compaction records live at:

`<table-root>/compaction/<seq>-<snapshot_id>.json`

Rules:

- `seq` is a zero-padded monotonic integer derived by scanning existing compaction records
- `snapshot_id` is a iceflow-local compaction snapshot id
- records are replayed in lexical filename order

Record payload fields:

- `snapshot_id`
- `snapshot_kind` with value `"iceflow-local"`
- `table_mode`
- `namespace`
- `table`
- `warehouse_uri`
- `min_small_file_bytes`
- `max_rewrite_files`
- `removed_files`
- `added_files`
- `created_at`

## Atomicity And Recovery

The atomicity boundary is explicit:

- replacement files are written first
- compaction record write is the commit

If the process dies after writing replacement files but before the compaction record lands, those files are treated as orphan files. Task 9 does not invent a new compaction-specific recovery path. Orphan handling stays with the existing reconciliation and orphan-cleanup mechanisms already present in iceflow-state.

## CompactReport

Task 9 should emit a JSON report with explicit temporary semantics:

```rust
pub struct CompactReport {
    pub mode: &'static str,
    pub snapshot_kind: &'static str,
    pub rewritten_files: usize,
    pub skipped_files: usize,
    pub snapshot_id: Option<String>,
    pub elapsed_ms: u64,
}
```

Task 9 values:

- `mode = "offline"`
- `snapshot_kind = "iceflow-local"`

`snapshot_id` is not an Iceberg snapshot id in Task 9. It identifies the iceflow-local compaction record that became durable.

## Verification

Task 9 must prove the compaction logic, not just the CLI parse path.

Required verification:

- a compaction test rewrites multiple small append-only files into fewer files and emits a `CompactReport` with `mode = "offline"` and `snapshot_kind = "iceflow-local"`
- a replay test reconstructs the active-file set correctly from base snapshot records plus compaction records
- a second-run stability test runs compaction twice back-to-back and asserts `rewritten_files == 0` on the second run
- a rejection test verifies `keyed_upsert` compaction fails with the explicit runtime error
- the CLI command contract is covered through `cargo test -p iceflow-cli --test compact`

The second-run no-op test is the most important guard against replay bugs that would cause runaway re-compaction.

## Deferred Follow-On

The next task after Task 9 should upgrade the Polaris path to real Iceberg semantics:

- real table creation
- `LoadTable`
- manifest-list reads for active data-file discovery
- optimistic-concurrency assertions on commit
- mock Polaris support for table endpoints

Once that lands, compaction can be upgraded from iceflow-local snapshot ids to real Iceberg snapshot semantics without changing the CLI shape introduced in Task 9.
