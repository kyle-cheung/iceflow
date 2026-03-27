from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_FIXTURE_ROOT = REPO_ROOT / "fixtures" / "reference_workload_v0"
SOURCE_FIXTURE_DIRS = {
    "append_only.orders_events": REFERENCE_FIXTURE_ROOT / "orders_events",
    "keyed_upsert.customer_state": REFERENCE_FIXTURE_ROOT / "customer_state",
}


def load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            records.append(normalize_value(json.loads(stripped)))
    return records


def normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        if not value:
            # PyArrow cannot write empty struct values to Parquet, so the
            # append-only fixture's `{}` sentinel must be normalized first.
            return None
        return {key: normalize_value(child) for key, child in value.items()}
    if isinstance(value, list):
        return [normalize_value(child) for child in value]
    return value


def parquet_path_for_jsonl(output_dir: Path, jsonl_path: Path) -> Path:
    return output_dir / f"{jsonl_path.stem}.parquet"


def generate_fixtures(output_root: Path) -> dict[str, list[Path]]:
    output_root.mkdir(parents=True, exist_ok=True)
    generated: dict[str, list[Path]] = {}

    for workload, source_dir in SOURCE_FIXTURE_DIRS.items():
        workload_dir = output_root / source_dir.name
        workload_dir.mkdir(parents=True, exist_ok=True)
        for existing in workload_dir.glob("*.parquet"):
            existing.unlink()

        generated_paths: list[Path] = []
        for jsonl_path in sorted(source_dir.glob("*.jsonl")):
            table = pa.Table.from_pylist(load_jsonl_records(jsonl_path))
            parquet_path = parquet_path_for_jsonl(workload_dir, jsonl_path)
            pq.write_table(table, parquet_path)
            generated_paths.append(parquet_path)

        generated[workload] = generated_paths

    return generated


def main() -> int:
    generate_fixtures(Path.cwd())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
