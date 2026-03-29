from __future__ import annotations

import argparse
import json
import logging
import math
import os
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence
from urllib.parse import urlparse

import pyarrow.fs as pafs
import pyarrow.parquet as pq
from benchmarks.pyiceberg_baseline.generate_fixtures import generate_fixtures


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_PATH = REPO_ROOT / "benchmarks" / "pyiceberg_baseline" / "latest-report.json"
DEFAULT_DATA_DIR = Path(tempfile.gettempdir()) / "iceflow-pyiceberg-baseline" / "landed"
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkloadConfig:
    name: str
    generated_dir_name: str
    skip: bool


REFERENCE_WORKLOADS = {
    "append_only.orders_events": WorkloadConfig(
        name="append_only.orders_events",
        generated_dir_name="orders_events",
        skip=False,
    ),
    "keyed_upsert.customer_state": WorkloadConfig(
        name="keyed_upsert.customer_state",
        generated_dir_name="customer_state",
        skip=True,
    ),
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the PyIceberg baseline benchmark.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="Root directory containing landed Parquet files, one subdirectory per workload.",
    )
    parser.add_argument(
        "--workload",
        action="append",
        choices=sorted(REFERENCE_WORKLOADS),
        help="Workload to run. Repeat to select more than one workload.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path to write the JSON report.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve paths and emit the report without generating fixtures or touching the stack.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def resolve_git_sha() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return completed.stdout.strip() or "unknown"


def resolve_generated_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_data_dir(provided: Path | None) -> Path:
    return provided if provided is not None else DEFAULT_DATA_DIR


def resolve_selected_workloads(requested: Sequence[str] | None) -> list[WorkloadConfig]:
    if not requested:
        return list(REFERENCE_WORKLOADS.values())
    return [REFERENCE_WORKLOADS[name] for name in requested]


def require_env(*names: str) -> str:
    for name in names:
        if name not in os.environ:
            continue
        value = os.environ[name]
        if value == "":
            raise ValueError(f"{name} is set but empty")
        return value
    joined = " or ".join(names)
    raise ValueError(f"{joined} must be set in the environment")


def resolve_stack_config() -> dict[str, str]:
    polaris_api_port = os.environ.get("POLARIS_API_PORT", "8181")
    object_store_api_port = os.environ.get(
        "OBJECT_STORE_API_PORT",
        os.environ.get("MINIO_API_PORT", "9000"),
    )
    return {
        "catalog_uri": os.environ.get(
            "POLARIS_CATALOG_URI",
            f"http://127.0.0.1:{polaris_api_port}/api/catalog",
        ),
        "catalog_name": os.environ.get("POLARIS_CATALOG_NAME", "quickstart_catalog"),
        "namespace": os.environ.get("POLARIS_NAMESPACE", "orders_events"),
        "client_id": require_env("POLARIS_CLIENT_ID", "POLARIS_ROOT_CLIENT_ID"),
        "client_secret": require_env("POLARIS_CLIENT_SECRET", "POLARIS_ROOT_CLIENT_SECRET"),
        "object_store_endpoint": os.environ.get(
            "OBJECT_STORE_ENDPOINT",
            f"http://127.0.0.1:{object_store_api_port}",
        ),
        "object_store_bucket": os.environ.get(
            "OBJECT_STORE_BUCKET",
            os.environ.get("MINIO_BUCKET", "iceflow-warehouse"),
        ),
        "object_store_access_key": require_env("OBJECT_STORE_ACCESS_KEY", "MINIO_ROOT_USER"),
        "object_store_secret_key": require_env(
            "OBJECT_STORE_SECRET_KEY",
            "MINIO_ROOT_PASSWORD",
        ),
        "object_store_region": os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
    }


def build_workload_report(
    workload: WorkloadConfig,
    data_dir: Path,
    dry_run: bool,
) -> dict[str, object]:
    workload_data_dir = data_dir / workload.generated_dir_name
    if workload.skip:
        status = "skipped"
    elif dry_run:
        status = "dry_run"
    else:
        status = "pending"

    return {
        "workload": workload.name,
        "skip": workload.skip,
        "status": status,
        "data_dir": str(workload_data_dir),
    }


def redact_stack_config(stack: dict[str, str]) -> dict[str, str]:
    redacted = dict(stack)
    redacted.pop("client_secret", None)
    redacted.pop("object_store_access_key", None)
    redacted.pop("object_store_secret_key", None)
    return redacted


def parquet_files_for_workload(data_dir: Path) -> list[Path]:
    return sorted(data_dir.glob("*.parquet"))


def total_rows_for_files(files: Sequence[Path]) -> int:
    return sum(pq.read_metadata(path).num_rows for path in files)


def percentile_ms(samples: Sequence[float], percentile: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return round(ordered[0], 3)
    position = max(0.0, min(len(ordered) - 1, (len(ordered) - 1) * percentile))
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return round(ordered[lower_index], 3)
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    interpolated = lower_value + ((upper_value - lower_value) * (position - lower_index))
    return round(interpolated, 3)


def s3_filesystem(stack: dict[str, str]) -> pafs.S3FileSystem:
    endpoint = urlparse(stack["object_store_endpoint"])
    endpoint_override = endpoint.netloc or endpoint.path
    scheme = endpoint.scheme or "http"
    return pafs.S3FileSystem(
        access_key=stack["object_store_access_key"],
        secret_key=stack["object_store_secret_key"],
        region=stack["object_store_region"],
        scheme=scheme,
        endpoint_override=endpoint_override,
        force_virtual_addressing=False,
    )


def upload_parquet_files(
    files: Sequence[Path],
    workload: WorkloadConfig,
    stack: dict[str, str],
    run_id: str,
    uploaded_files: list[str],
) -> None:
    filesystem = s3_filesystem(stack)
    prefix = f"benchmarks/pyiceberg-baseline/{workload.generated_dir_name}/{run_id}/data"
    for path in files:
        object_key = f"{prefix}/{path.name}"
        pafs.copy_files(
            str(path),
            f"{stack['object_store_bucket']}/{object_key}",
            destination_filesystem=filesystem,
        )
        uploaded_files.append(f"s3://{stack['object_store_bucket']}/{object_key}")


def delete_uploaded_files(uploaded_files: Sequence[str], stack: dict[str, str]) -> None:
    filesystem = s3_filesystem(stack)
    for file_uri in uploaded_files:
        parsed = urlparse(file_uri)
        object_path = f"{parsed.netloc}{parsed.path}"
        filesystem.delete_file(object_path)


def namespace_for_workload(workload: WorkloadConfig, stack: dict[str, str]) -> str:
    base_namespace = stack["namespace"]
    if base_namespace == workload.generated_dir_name:
        return base_namespace
    return f"{base_namespace}_{workload.generated_dir_name}"


def table_name_for_workload(workload: WorkloadConfig, run_id: str) -> str:
    return f"pyiceberg_baseline_{workload.generated_dir_name}_{run_id.replace('-', '_')}"


def load_polaris_catalog(stack: dict[str, str]) -> "Catalog":
    from pyiceberg.catalog import Catalog, load_catalog

    return load_catalog(
        "iceflow-pyiceberg-baseline",
        type="rest",
        uri=stack["catalog_uri"],
        warehouse=stack["catalog_name"],
        credential=f"{stack['client_id']}:{stack['client_secret']}",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "header.X-Iceberg-Access-Delegation": "",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": stack["object_store_endpoint"],
            "s3.access-key-id": stack["object_store_access_key"],
            "s3.secret-access-key": stack["object_store_secret_key"],
            "s3.region": stack["object_store_region"],
            "s3.force-virtual-addressing": "false",
        },
    )


def benchmark_workload(
    workload: WorkloadConfig,
    data_dir: Path,
    stack: dict[str, str],
) -> dict[str, object]:
    parquet_files = parquet_files_for_workload(data_dir)
    if not parquet_files:
        raise FileNotFoundError(f"no parquet inputs found in {data_dir}")

    row_count = total_rows_for_files(parquet_files)
    run_id = uuid.uuid4().hex[:12]
    uploaded_files: list[str] = []
    catalog = None
    namespace = namespace_for_workload(workload, stack)
    table_name = table_name_for_workload(workload, run_id)
    table_identifier = (namespace, table_name)
    namespace_created = False
    table_created = False

    try:
        upload_parquet_files(parquet_files, workload, stack, run_id, uploaded_files)
        catalog = load_polaris_catalog(stack)
        if not catalog.namespace_exists(namespace):
            catalog.create_namespace(namespace)
            namespace_created = True

        table = catalog.create_table(
            table_identifier,
            schema=pq.read_schema(parquet_files[0]),
        )
        table_created = True

        commit_latencies_ms: list[float] = []
        started_at = time.perf_counter()
        for uploaded_file in uploaded_files:
            commit_started_at = time.perf_counter()
            table.add_files([uploaded_file])
            commit_latencies_ms.append((time.perf_counter() - commit_started_at) * 1000.0)
        elapsed_seconds = max(time.perf_counter() - started_at, 1e-9)

        return {
            "status": "completed",
            "rows": row_count,
            "batches": len(uploaded_files),
            "throughput_rows_per_second": round(row_count / elapsed_seconds, 3),
            "p95_commit_latency_ms": percentile_ms(commit_latencies_ms, 0.95),
            "table_identifier": ".".join(table_identifier),
            "table_location": table.location(),
            "uploaded_files": uploaded_files,
        }
    except Exception:
        if catalog is not None and table_created:
            try:
                catalog.drop_table(table_identifier, purge_requested=True)
            except Exception:
                LOGGER.warning("Failed to drop benchmark table %s during cleanup", ".".join(table_identifier))
        if catalog is not None and namespace_created:
            try:
                catalog.drop_namespace(namespace)
            except Exception:
                LOGGER.warning("Failed to drop benchmark namespace %s during cleanup", namespace)
        try:
            delete_uploaded_files(uploaded_files, stack)
        except Exception:
            LOGGER.warning("Failed to delete uploaded benchmark files during cleanup")
        raise


def write_report(output_path: Path, report: dict[str, object]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    data_dir = resolve_data_dir(args.data_dir)
    workloads = resolve_selected_workloads(args.workload)
    stack = resolve_stack_config()

    if not args.dry_run and args.data_dir is None:
        generate_fixtures(data_dir)

    workload_reports: list[dict[str, object]] = []
    exit_code = 0
    for workload in workloads:
        base_report = build_workload_report(workload, data_dir, args.dry_run)
        workload_data_dir = Path(base_report["data_dir"])
        if workload.skip or args.dry_run:
            workload_reports.append(base_report)
            continue

        try:
            metrics = benchmark_workload(workload, workload_data_dir, stack)
        except Exception as exc:
            LOGGER.exception("Benchmark workload failed for %s", workload.name)
            exit_code = 1
            workload_reports.append(
                {
                    **base_report,
                    "status": "failed",
                    "error": str(exc),
                }
            )
            continue

        workload_reports.append({**base_report, **metrics})

    report = {
        "generated_at": resolve_generated_at(),
        "git_sha": resolve_git_sha(),
        "data_dir": str(data_dir),
        "output_path": str(args.output),
        "stack": redact_stack_config(stack),
        "workloads": workload_reports,
    }
    write_report(args.output, report)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
