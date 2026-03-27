from pathlib import Path

import pyarrow.parquet as pq

from benchmarks.pyiceberg_baseline.generate_fixtures import generate_fixtures


def test_generate_fixtures_materializes_parquet_for_both_reference_workloads(
    tmp_path: Path,
) -> None:
    generated = generate_fixtures(tmp_path)

    append_only_files = generated["append_only.orders_events"]
    keyed_upsert_files = generated["keyed_upsert.customer_state"]

    assert len(append_only_files) == 2
    assert len(keyed_upsert_files) == 2

    append_only_table = pq.read_table(append_only_files[0])
    keyed_upsert_table = pq.read_table(keyed_upsert_files[0])

    assert append_only_table.num_rows == 2
    assert "ordering_value" in append_only_table.column_names
    assert keyed_upsert_table.num_rows == 2
    assert "source_event_id" in keyed_upsert_table.column_names
