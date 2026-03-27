import json
from pathlib import Path

from benchmarks.pyiceberg_baseline.run import REFERENCE_WORKLOADS, main


def test_reference_workloads_are_registered_with_expected_defaults() -> None:
    assert "append_only.orders_events" in REFERENCE_WORKLOADS
    assert REFERENCE_WORKLOADS["append_only.orders_events"].skip is False
    assert "keyed_upsert.customer_state" in REFERENCE_WORKLOADS
    assert REFERENCE_WORKLOADS["keyed_upsert.customer_state"].skip is True


def test_dry_run_writes_report_with_git_sha_timestamp_and_workload_statuses(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
        raising=False,
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
