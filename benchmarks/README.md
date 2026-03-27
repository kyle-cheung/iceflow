# Benchmarks

## G1 Write-Path Differentiation

`benchmarks/pyiceberg_baseline/` provides the Python baseline for the `G1` gate in the v0 architecture spec. It measures the same landed-Parquet append-only workload against the local Polaris plus S3-compatible reference stack and emits a JSON report with:

- `generated_at`
- `git_sha`
- `workloads[].throughput_rows_per_second`
- `workloads[].p95_commit_latency_ms`

The current first pass enables `append_only.orders_events` and keeps `keyed_upsert.customer_state` registered as `skip=true` until the corresponding sink path exists in the engine.

## Prerequisites

Bring up the local reference stack first:

```sh
just stack-up
```

The `just benchmark-baseline` wrapper sources `infra/local/.env` and exports the local Polaris/object-store settings the Python harness expects.

## Dry Run

Use dry-run to verify workload selection, resolved stack settings, and output paths without generating fixtures or mutating the local stack:

```sh
just benchmark-baseline --dry-run
```

By default, the report is written to `benchmarks/pyiceberg_baseline/latest-report.json`.

## Live Run

Run the append-only baseline end to end:

```sh
just benchmark-baseline --workload append_only.orders_events
```

If `--data-dir` is omitted, the harness converts the checked-in JSONL fixtures from `fixtures/reference_workload_v0/` into Parquet under a temp directory and then uploads those Parquet files into the local S3-compatible object store before committing them through PyIceberg.

If you already have landed Parquet data, point the harness at it directly:

```sh
just benchmark-baseline --data-dir /path/to/landed-parquet
```

The `--data-dir` root should contain one subdirectory per workload, for example `orders_events/`.

## Report Notes

Each workload entry records whether it ran, was skipped, or failed. The top-level report includes the resolved stack coordinates, but secrets are intentionally omitted from the JSON output.
