# iceflow-worker-duckdb

## Purpose

`iceflow-worker-duckdb` is the in-process worker crate that turns source batches into engine-shaped, replayable files. It owns normalization, Parquet materialization, and the current offline compaction helper.

## Owns

- `DuckDbWorker` construction and in-memory execution
- normalization from `SourceBatch` to `NormalizedBatch`
- Parquet writing and `MaterializedBatch` output
- append-only small-file compaction helpers

## Key Files

- `crates/iceflow-worker-duckdb/src/lib.rs`
- `crates/iceflow-worker-duckdb/src/normalize.rs`
- `crates/iceflow-worker-duckdb/src/parquet_writer.rs`
- `crates/iceflow-worker-duckdb/src/compaction.rs`
- `crates/iceflow-cli/tests/compact.rs`

## Change Here When

- source rows need different normalization into worker output
- the Parquet replay boundary changes
- materialization metadata needs to change before state-store registration
- offline compaction behavior changes

## Avoid

- embedding state-store transitions in the worker crate
- adding sink commit logic here
- making file output depend on non-deterministic ordering

## Tests To Run

- `cargo test -p iceflow-worker-duckdb`
- `cargo test -p iceflow-worker-duckdb compact_parquet_files_merges_small_inputs_into_one_output`
- `just test-compact`

## Related Crates

- `iceflow-source` supplies the input batches
- `iceflow-types` defines normalized records and manifest-facing contracts
- `iceflow-state` records the files produced here
- `iceflow-cli` uses this crate for both `run` and `compact`
