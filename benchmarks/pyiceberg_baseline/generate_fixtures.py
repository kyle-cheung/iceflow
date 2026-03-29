from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, UTC
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


@dataclass(frozen=True)
class WorkloadShape:
    source_id: str
    source_class: str
    table_mode: str
    ordering_field: str


WORKLOAD_SHAPES = {
    "append_only.orders_events": WorkloadShape(
        source_id="file.orders_events",
        source_class="file_or_object_drop",
        table_mode="append_only",
        ordering_field="source_position",
    ),
    "keyed_upsert.customer_state": WorkloadShape(
        source_id="file.customer_state",
        source_class="database_cdc",
        table_mode="keyed_upsert",
        ordering_field="source_position",
    ),
}

LANDED_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("table_id", pa.string(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("source_class", pa.string(), nullable=False),
        pa.field("table_mode", pa.string(), nullable=False),
        pa.field("op", pa.string(), nullable=False),
        pa.field("key_json", pa.string(), nullable=False),
        pa.field("after_json", pa.string()),
        pa.field("before_json", pa.string()),
        pa.field("ordering_field", pa.string(), nullable=False),
        pa.field("ordering_value", pa.int64(), nullable=False),
        pa.field("source_checkpoint", pa.string(), nullable=False),
        pa.field("source_event_id", pa.string()),
        pa.field("schema_version", pa.int32(), nullable=False),
        pa.field("ingestion_ts", pa.timestamp("us"), nullable=False),
        pa.field("source_metadata_json", pa.string(), nullable=False),
    ]
)


def load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            records.append(json.loads(stripped))
    return records


def json_text(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=False)


def timestamp_value(value: Any) -> datetime:
    seconds = int(value)
    return datetime.fromtimestamp(seconds, UTC).replace(tzinfo=None)


def landed_record(workload: str, table_id: str, record: dict[str, Any]) -> dict[str, Any]:
    shape = WORKLOAD_SHAPES[workload]
    return {
        "table_id": table_id,
        "source_id": shape.source_id,
        "source_class": shape.source_class,
        "table_mode": shape.table_mode,
        "op": record["op"],
        "key_json": json_text(record.get("key", {})),
        "after_json": None if record.get("after") is None else json_text(record["after"]),
        "before_json": None if record.get("before") is None else json_text(record["before"]),
        "ordering_field": shape.ordering_field,
        "ordering_value": record["ordering_value"],
        "source_checkpoint": record["source_checkpoint"],
        "source_event_id": record.get("source_event_id"),
        "schema_version": record["schema_version"],
        "ingestion_ts": timestamp_value(record["ingestion_ts"]),
        "source_metadata_json": json_text(record.get("source_metadata", {})),
    }


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
            records = [
                landed_record(workload, source_dir.name, record)
                for record in load_jsonl_records(jsonl_path)
            ]
            table = pa.Table.from_pylist(records, schema=LANDED_PARQUET_SCHEMA)
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
