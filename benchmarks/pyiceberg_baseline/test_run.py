import logging
import json
from pathlib import Path

import pytest

from benchmarks.pyiceberg_baseline.run import (
    REFERENCE_WORKLOADS,
    benchmark_workload,
    main,
    percentile_ms,
    resolve_stack_config,
)


def set_runtime_env(monkeypatch) -> None:
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_ID", "root")
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_SECRET", "catalog-secret")
    monkeypatch.setenv("OBJECT_STORE_ACCESS_KEY", "object-access")
    monkeypatch.setenv("OBJECT_STORE_SECRET_KEY", "object-secret")
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "iceflow-warehouse")


def test_reference_workloads_are_registered_with_expected_defaults() -> None:
    assert "append_only.orders_events" in REFERENCE_WORKLOADS
    assert REFERENCE_WORKLOADS["append_only.orders_events"].skip is False
    assert "keyed_upsert.customer_state" in REFERENCE_WORKLOADS
    assert REFERENCE_WORKLOADS["keyed_upsert.customer_state"].skip is True


def test_dry_run_writes_report_with_git_sha_timestamp_and_workload_statuses(
    tmp_path: Path,
    monkeypatch,
) -> None:
    set_runtime_env(monkeypatch)
    output_path = tmp_path / "report.json"
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.resolve_git_sha",
        lambda: "abc1234",
    )

    exit_code = main(
        [
            "--dry-run",
            "--data-dir",
            str(tmp_path / "landed"),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    report = json.loads(output_path.read_text())
    assert report["git_sha"] == "abc1234"
    assert "generated_at" in report
    workload_reports = {item["workload"]: item for item in report["workloads"]}
    assert workload_reports["append_only.orders_events"]["skip"] is False
    assert workload_reports["append_only.orders_events"]["status"] == "dry_run"
    assert workload_reports["keyed_upsert.customer_state"]["skip"] is True
    assert workload_reports["keyed_upsert.customer_state"]["status"] == "skipped"


def test_main_generates_default_fixtures_and_runs_requested_workload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    set_runtime_env(monkeypatch)
    output_path = tmp_path / "report.json"
    default_data_dir = tmp_path / "auto-landed"
    benchmark_calls: list[tuple[str, Path]] = []

    monkeypatch.setattr("benchmarks.pyiceberg_baseline.run.DEFAULT_DATA_DIR", default_data_dir)
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.resolve_git_sha",
        lambda: "def5678",
    )

    def fake_generate_fixtures(data_dir: Path) -> dict[str, list[Path]]:
        (data_dir / "orders_events").mkdir(parents=True, exist_ok=True)
        (data_dir / "customer_state").mkdir(parents=True, exist_ok=True)
        return {}

    def fake_benchmark_workload(workload, data_dir: Path, stack: dict[str, str]) -> dict[str, object]:
        benchmark_calls.append((workload.name, data_dir))
        return {
            "status": "completed",
            "rows": 4,
            "batches": 2,
            "throughput_rows_per_second": 8.0,
            "p95_commit_latency_ms": 12.5,
        }

    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.generate_fixtures",
        fake_generate_fixtures,
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.benchmark_workload",
        fake_benchmark_workload,
    )

    exit_code = main(
        [
            "--workload",
            "append_only.orders_events",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert benchmark_calls == [("append_only.orders_events", default_data_dir / "orders_events")]
    report = json.loads(output_path.read_text())
    assert report["git_sha"] == "def5678"
    assert len(report["workloads"]) == 1
    workload_report = report["workloads"][0]
    assert workload_report["workload"] == "append_only.orders_events"
    assert workload_report["data_dir"] == str(default_data_dir / "orders_events")
    assert workload_report["skip"] is False
    assert workload_report["status"] == "completed"
    assert workload_report["rows"] == 4
    assert workload_report["batches"] == 2
    assert workload_report["throughput_rows_per_second"] == 8.0
    assert workload_report["p95_commit_latency_ms"] == 12.5


def test_resolve_stack_config_requires_runtime_secrets(monkeypatch) -> None:
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_ID", "root")
    monkeypatch.delenv("POLARIS_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("POLARIS_ROOT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("OBJECT_STORE_ACCESS_KEY", raising=False)
    monkeypatch.delenv("OBJECT_STORE_SECRET_KEY", raising=False)

    with pytest.raises(ValueError, match="POLARIS_CLIENT_SECRET"):
        resolve_stack_config()


def test_resolve_stack_config_requires_runtime_client_id(monkeypatch) -> None:
    monkeypatch.delenv("POLARIS_CLIENT_ID", raising=False)
    monkeypatch.delenv("POLARIS_ROOT_CLIENT_ID", raising=False)
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_SECRET", "catalog-secret")
    monkeypatch.setenv("OBJECT_STORE_ACCESS_KEY", "object-access")
    monkeypatch.setenv("OBJECT_STORE_SECRET_KEY", "object-secret")

    with pytest.raises(ValueError, match="POLARIS_CLIENT_ID"):
        resolve_stack_config()


def test_resolve_stack_config_accepts_legacy_minio_env_names(monkeypatch) -> None:
    monkeypatch.delenv("OBJECT_STORE_ACCESS_KEY", raising=False)
    monkeypatch.delenv("OBJECT_STORE_SECRET_KEY", raising=False)
    monkeypatch.delenv("OBJECT_STORE_BUCKET", raising=False)
    monkeypatch.delenv("OBJECT_STORE_API_PORT", raising=False)
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_ID", "root")
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_SECRET", "catalog-secret")
    monkeypatch.setenv("MINIO_ROOT_USER", "minio-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minio-password")
    monkeypatch.setenv("MINIO_BUCKET", "legacy-bucket")
    monkeypatch.setenv("MINIO_API_PORT", "9000")

    stack = resolve_stack_config()

    assert stack["object_store_access_key"] == "minio-user"
    assert stack["object_store_secret_key"] == "minio-password"
    assert stack["object_store_bucket"] == "legacy-bucket"
    assert stack["object_store_endpoint"] == "http://127.0.0.1:9000"


def test_resolve_stack_config_requires_object_store_access_key(monkeypatch) -> None:
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_ID", "root")
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_SECRET", "catalog-secret")
    monkeypatch.delenv("OBJECT_STORE_ACCESS_KEY", raising=False)
    monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
    monkeypatch.setenv("OBJECT_STORE_SECRET_KEY", "object-secret")

    with pytest.raises(ValueError, match="OBJECT_STORE_ACCESS_KEY"):
        resolve_stack_config()


def test_resolve_stack_config_requires_object_store_secret_key(monkeypatch) -> None:
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_ID", "root")
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_SECRET", "catalog-secret")
    monkeypatch.setenv("OBJECT_STORE_ACCESS_KEY", "object-access")
    monkeypatch.delenv("OBJECT_STORE_SECRET_KEY", raising=False)
    monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="OBJECT_STORE_SECRET_KEY"):
        resolve_stack_config()


def test_resolve_stack_config_rejects_empty_primary_client_id(monkeypatch) -> None:
    monkeypatch.setenv("POLARIS_CLIENT_ID", "")
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_ID", "fallback-root")
    monkeypatch.setenv("POLARIS_ROOT_CLIENT_SECRET", "catalog-secret")
    monkeypatch.setenv("OBJECT_STORE_ACCESS_KEY", "object-access")
    monkeypatch.setenv("OBJECT_STORE_SECRET_KEY", "object-secret")

    with pytest.raises(ValueError, match="POLARIS_CLIENT_ID is set but empty"):
        resolve_stack_config()


def test_main_logs_traceback_when_workload_fails(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    set_runtime_env(monkeypatch)
    output_path = tmp_path / "report.json"

    def fake_generate_fixtures(data_dir: Path) -> dict[str, list[Path]]:
        (data_dir / "orders_events").mkdir(parents=True, exist_ok=True)
        return {}

    def fake_benchmark_workload(workload, data_dir: Path, stack: dict[str, str]) -> dict[str, object]:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.generate_fixtures",
        fake_generate_fixtures,
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.benchmark_workload",
        fake_benchmark_workload,
    )

    caplog.set_level(logging.ERROR)
    exit_code = main(
        [
            "--workload",
            "append_only.orders_events",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 1
    assert "Benchmark workload failed for append_only.orders_events" in caplog.text
    assert "RuntimeError: boom" in caplog.text


def test_percentile_ms_interpolates_for_small_samples() -> None:
    assert percentile_ms([1.0, 10.0], 0.95) == 9.55


def test_load_polaris_catalog_disables_access_delegation_and_forces_path_style(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_load_catalog(name: str, **properties: str) -> object:
        captured["name"] = name
        captured["properties"] = properties
        return object()

    import pyiceberg.catalog

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", fake_load_catalog)

    from benchmarks.pyiceberg_baseline.run import load_polaris_catalog

    load_polaris_catalog(
        {
            "catalog_uri": "http://127.0.0.1:8181/api/catalog",
            "catalog_name": "quickstart_catalog",
            "namespace": "orders_events",
            "client_id": "root",
            "client_secret": "catalog-secret",
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "iceflow-warehouse",
            "object_store_access_key": "object-access",
            "object_store_secret_key": "object-secret",
            "object_store_region": "us-west-2",
        }
    )

    assert captured["name"] == "iceflow-pyiceberg-baseline"
    properties = captured["properties"]
    assert properties["header.X-Iceberg-Access-Delegation"] == ""
    assert properties["s3.force-virtual-addressing"] == "false"


def test_benchmark_workload_cleans_up_failed_table_and_uploaded_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    deleted_files: list[str] = []

    class FakeFilesystem:
        def delete_file(self, path: str) -> None:
            deleted_files.append(path)

    class FakeTable:
        def add_files(self, file_paths: list[str]) -> None:
            raise RuntimeError("commit failed")

    class FakeCatalog:
        def __init__(self) -> None:
            self.dropped_tables: list[tuple[tuple[str, str], bool]] = []
            self.dropped_namespaces: list[str] = []

        def namespace_exists(self, namespace: str) -> bool:
            return False

        def create_namespace(self, namespace: str) -> None:
            return None

        def create_table(self, identifier, schema, **kwargs):
            return FakeTable()

        def drop_table(self, identifier, purge_requested: bool = False) -> None:
            self.dropped_tables.append((identifier, purge_requested))

        def drop_namespace(self, namespace: str) -> None:
            self.dropped_namespaces.append(namespace)

    fake_catalog = FakeCatalog()
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.parquet_files_for_workload",
        lambda _: [tmp_path / "batch-0001.parquet"],
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.total_rows_for_files",
        lambda _: 2,
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.upload_parquet_files",
        lambda files, workload, stack, run_id, uploaded_files: uploaded_files.append(
            "s3://iceflow-warehouse/test-prefix/batch-0001.parquet"
        ),
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.load_polaris_catalog",
        lambda stack: fake_catalog,
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.s3_filesystem",
        lambda stack: FakeFilesystem(),
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.pq.read_schema",
        lambda _: object(),
    )

    with pytest.raises(RuntimeError, match="commit failed"):
        benchmark_workload(
            REFERENCE_WORKLOADS["append_only.orders_events"],
            tmp_path,
            {
                "catalog_uri": "http://127.0.0.1:8181/api/catalog",
                "catalog_name": "quickstart_catalog",
                "namespace": "orders_events",
                "client_id": "root",
                "client_secret": "catalog-secret",
                "object_store_endpoint": "http://127.0.0.1:9000",
                "object_store_bucket": "iceflow-warehouse",
                "object_store_access_key": "object-access",
                "object_store_secret_key": "object-secret",
                "object_store_region": "us-west-2",
            },
        )

    assert len(fake_catalog.dropped_tables) == 1
    dropped_identifier, purge_requested = fake_catalog.dropped_tables[0]
    assert dropped_identifier[0] == "orders_events"
    assert dropped_identifier[1].startswith("pyiceberg_baseline_orders_events_")
    assert purge_requested is True
    assert fake_catalog.dropped_namespaces == ["orders_events"]
    assert deleted_files == ["iceflow-warehouse/test-prefix/batch-0001.parquet"]


def test_benchmark_workload_cleans_up_partially_uploaded_files_when_upload_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    deleted_files: list[str] = []

    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.parquet_files_for_workload",
        lambda _: [tmp_path / "batch-0001.parquet"],
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.total_rows_for_files",
        lambda _: 2,
    )

    def fake_upload_parquet_files(files, workload, stack, run_id, uploaded_files: list[str]) -> None:
        uploaded_files.append("s3://iceflow-warehouse/test-prefix/batch-0001.parquet")
        raise RuntimeError("upload failed")

    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.upload_parquet_files",
        fake_upload_parquet_files,
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.delete_uploaded_files",
        lambda uploaded_files, stack: deleted_files.extend(uploaded_files),
    )

    with pytest.raises(RuntimeError, match="upload failed"):
        benchmark_workload(
            REFERENCE_WORKLOADS["append_only.orders_events"],
            tmp_path,
            {
                "catalog_uri": "http://127.0.0.1:8181/api/catalog",
                "catalog_name": "quickstart_catalog",
                "namespace": "orders_events",
                "client_id": "root",
                "client_secret": "catalog-secret",
                "object_store_endpoint": "http://127.0.0.1:9000",
                "object_store_bucket": "iceflow-warehouse",
                "object_store_access_key": "object-access",
                "object_store_secret_key": "object-secret",
                "object_store_region": "us-west-2",
            },
        )

    assert deleted_files == ["s3://iceflow-warehouse/test-prefix/batch-0001.parquet"]


def test_benchmark_workload_preserves_original_exception_when_cleanup_fails(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    class FakeTable:
        def add_files(self, file_paths: list[str]) -> None:
            raise RuntimeError("commit failed")

    class FakeCatalog:
        def namespace_exists(self, namespace: str) -> bool:
            return False

        def create_namespace(self, namespace: str) -> None:
            return None

        def create_table(self, identifier, schema, **kwargs):
            return FakeTable()

        def drop_table(self, identifier, purge_requested: bool = False) -> None:
            raise RuntimeError("drop table failed")

        def drop_namespace(self, namespace: str) -> None:
            raise RuntimeError("drop namespace failed")

    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.parquet_files_for_workload",
        lambda _: [tmp_path / "batch-0001.parquet"],
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.total_rows_for_files",
        lambda _: 2,
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.upload_parquet_files",
        lambda files, workload, stack, run_id, uploaded_files: uploaded_files.append(
            "s3://iceflow-warehouse/test-prefix/batch-0001.parquet"
        ),
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.load_polaris_catalog",
        lambda stack: FakeCatalog(),
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.pq.read_schema",
        lambda _: object(),
    )

    def fake_delete_uploaded_files(uploaded_files, stack) -> None:
        raise RuntimeError("delete files failed")

    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.delete_uploaded_files",
        fake_delete_uploaded_files,
    )

    caplog.set_level(logging.WARNING)
    with pytest.raises(RuntimeError, match="commit failed"):
        benchmark_workload(
            REFERENCE_WORKLOADS["append_only.orders_events"],
            tmp_path,
            {
                "catalog_uri": "http://127.0.0.1:8181/api/catalog",
                "catalog_name": "quickstart_catalog",
                "namespace": "orders_events",
                "client_id": "root",
                "client_secret": "catalog-secret",
                "object_store_endpoint": "http://127.0.0.1:9000",
                "object_store_bucket": "iceflow-warehouse",
                "object_store_access_key": "object-access",
                "object_store_secret_key": "object-secret",
                "object_store_region": "us-west-2",
            },
        )

    assert "Failed to drop benchmark table" in caplog.text
    assert "Failed to drop benchmark namespace" in caplog.text
    assert "Failed to delete uploaded benchmark files during cleanup" in caplog.text


def test_benchmark_workload_uses_catalog_assigned_table_location(
    tmp_path: Path,
    monkeypatch,
) -> None:
    create_table_calls: list[tuple[tuple[str, str], object, dict[str, object]]] = []

    class FakeTable:
        def add_files(self, file_paths: list[str]) -> None:
            return None

        def location(self) -> str:
            return "s3://iceflow-warehouse/orders_events/managed-table"

    class FakeCatalog:
        def namespace_exists(self, namespace: str) -> bool:
            return True

        def create_table(self, identifier, schema, **kwargs):
            create_table_calls.append((identifier, schema, kwargs))
            return FakeTable()

    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.parquet_files_for_workload",
        lambda _: [tmp_path / "batch-0001.parquet"],
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.total_rows_for_files",
        lambda _: 2,
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.upload_parquet_files",
        lambda files, workload, stack, run_id, uploaded_files: uploaded_files.append(
            "s3://iceflow-warehouse/test-prefix/batch-0001.parquet"
        ),
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.load_polaris_catalog",
        lambda stack: FakeCatalog(),
    )
    monkeypatch.setattr(
        "benchmarks.pyiceberg_baseline.run.pq.read_schema",
        lambda _: object(),
    )

    metrics = benchmark_workload(
        REFERENCE_WORKLOADS["append_only.orders_events"],
        tmp_path,
        {
            "catalog_uri": "http://127.0.0.1:8181/api/catalog",
            "catalog_name": "quickstart_catalog",
            "namespace": "orders_events",
            "client_id": "root",
            "client_secret": "catalog-secret",
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "iceflow-warehouse",
            "object_store_access_key": "object-access",
            "object_store_secret_key": "object-secret",
            "object_store_region": "us-west-2",
        },
    )

    assert len(create_table_calls) == 1
    identifier, _schema, kwargs = create_table_calls[0]
    assert identifier[0] == "orders_events"
    assert identifier[1].startswith("pyiceberg_baseline_orders_events_")
    assert kwargs == {}
    assert metrics["table_location"] == "s3://iceflow-warehouse/orders_events/managed-table"
