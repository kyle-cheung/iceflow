# greytl-source

## Purpose

`greytl-source` owns the source adapter boundary and the deterministic file-backed reference source used by local runs and tests. It turns checked-in workload fixtures into ordered `SourceBatch` values and accepts and validates checkpoint acknowledgements.

## Owns

- the `SourceAdapter` trait and request/response types
- source discovery, spec, snapshot, and checkpoint APIs
- the reference `FileSource` implementation
- deterministic batch indexing over the fixture workload
- JSONL fixture playback into `SourceBatch` values

## Key Files

- `crates/greytl-source/src/adapter.rs`
- `crates/greytl-source/src/file_source.rs`
- `crates/greytl-cli/src/commands/run.rs`
- `crates/greytl-source/tests/source_adapter.rs`
- `fixtures/reference_workload_v0/`

## Change Here When

- the source trait changes
- source batches need new metadata or checkpoint behavior
- supported workloads change in the CLI or source path
- the file-backed reference source needs to map fixture files differently
- a source-facing regression appears in `run` or end-to-end fixture playback
- `FileSource` should infer different `table_mode` or `source_class` values from fixture directory names

## Avoid

- pushing normalization or Parquet-writing logic into this crate
- making the reference source non-deterministic
- leaking sink or state-store concerns into the source interface

## Tests To Run

- `cargo test -p greytl-source`
- `cargo test -p greytl-source --test source_adapter`
- `just test-fast`

## Related Crates

- `greytl-types` defines the shared batch and mutation vocabulary used here
- `greytl-worker-duckdb` consumes `SourceBatch` output from this crate
- `greytl-cli` wires `FileSource` into the `run` command
