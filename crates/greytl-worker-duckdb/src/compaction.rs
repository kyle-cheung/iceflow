use crate::parquet_writer::{manifest_file, rows_for_target, sanitize_identifier, DuckDb};
use crate::WriterConfig;
use anyhow::{Error, Result};
use chrono::Utc;
use greytl_types::ManifestFile;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedFiles {
    pub files: Vec<ManifestFile>,
    pub record_count: u64,
}

pub fn compact_parquet_files(
    config: &WriterConfig,
    input_files: &[PathBuf],
    output_dir: &Path,
    output_prefix: &str,
) -> Result<CompactedFiles> {
    if input_files.is_empty() {
        anyhow::bail!("input_files cannot be empty");
    }

    fs::create_dir_all(output_dir)
        .map_err(|err| Error::msg(format!("{}: {err}", output_dir.display())))?;

    let db = DuckDb::open_in_memory()?;
    let safe_output_prefix = sanitize_output_component(output_prefix);
    let view_name = format!(
        "compaction_{}_{}",
        sanitize_identifier(safe_output_prefix.as_str()),
        unique_suffix()
    );
    let table_name = format!(
        "compaction_rows_{}_{}",
        sanitize_identifier(safe_output_prefix.as_str()),
        unique_suffix()
    );
    db.create_parquet_view(&view_name, input_files)?;
    db.create_temp_table_from_source(&table_name, &view_name)?;
    db.drop_view(&view_name)?;

    let total_rows = db.count_rows(&table_name)?;
    let total_input_bytes = total_input_bytes(input_files)?;
    let average_record_bytes = if total_rows == 0 {
        1
    } else {
        ((total_input_bytes / total_rows).max(1)) as usize
    };
    let rows_per_file = rows_for_target(config.target_file_bytes, average_record_bytes);
    let row_group_rows = rows_for_target(config.max_row_group_bytes, average_record_bytes);

    let created_at = Utc::now();
    let mut files = Vec::new();
    let mut offset = 0u64;
    let mut chunk_index = 0usize;
    while offset < total_rows {
        let file_path = output_dir.join(format!("{safe_output_prefix}-{chunk_index:04}.parquet"));
        let chunk_rows = rows_per_file.min((total_rows - offset) as usize);

        db.copy_table_slice_to_parquet(
            &table_name,
            &file_path,
            row_group_rows,
            chunk_rows,
            offset,
        )?;
        files.push(manifest_file(
            &file_path,
            chunk_rows as u64,
            created_at.clone(),
        )?);

        offset += chunk_rows as u64;
        chunk_index += 1;
    }

    db.drop_table(&table_name)?;
    Ok(CompactedFiles {
        files,
        record_count: total_rows,
    })
}

fn sanitize_output_component(output_prefix: &str) -> String {
    let leaf = Path::new(output_prefix)
        .components()
        .rev()
        .find_map(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .unwrap_or("");
    let sanitized: String = leaf
        .chars()
        .map(|ch| match ch {
            '-' | '_' | '.' => ch,
            ch if ch.is_ascii_alphanumeric() => ch,
            _ => '_',
        })
        .collect();
    if sanitized.is_empty() {
        "batch".to_string()
    } else {
        sanitized
    }
}

fn total_input_bytes(input_files: &[PathBuf]) -> Result<u64> {
    let mut total = 0u64;
    for path in input_files {
        let metadata =
            fs::metadata(path).map_err(|err| Error::msg(format!("{}: {err}", path.display())))?;
        total += metadata.len();
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::compact_parquet_files;
    use crate::parquet_writer::DuckDb;
    use crate::test_support::run_ready;
    use crate::{DuckDbWorker, WriterConfig};
    use greytl_source::SourceBatch;
    use greytl_types::{
        checkpoint, ordering, table_id, LogicalMutation, SourceClass, StructuredKey, TableMode,
    };
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn compact_parquet_files_merges_small_inputs_into_one_output() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let first = run_ready(worker.materialize(sample_append_only_batch("batch-0001", 1)))
            .expect("first materialized batch");
        let second = run_ready(worker.materialize(sample_append_only_batch("batch-0002", 4)))
            .expect("second materialized batch");
        assert_eq!(first.manifest.file_set.len(), 1);
        assert_eq!(second.manifest.file_set.len(), 1);

        let input_files = vec![
            path_from_file_uri(&first.manifest.file_set[0].file_uri),
            path_from_file_uri(&second.manifest.file_set[0].file_uri),
        ];
        let output_dir = std::env::temp_dir()
            .join("greytl-worker-duckdb-compaction-tests")
            .join(unique_suffix());

        let result = compact_parquet_files(
            &WriterConfig::default(),
            &input_files,
            &output_dir,
            "compacted",
        )
        .expect("compaction succeeds");

        assert_eq!(result.record_count, 6);
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0].record_count, 6);
        assert!(path_from_file_uri(&result.files[0].file_uri).exists());
    }

    #[test]
    fn compact_parquet_files_multi_output_preserves_all_rows() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let mut input_files = Vec::new();

        for batch_index in 0..10 {
            let ordering_start = batch_index * 3 + 1;
            let materialized = run_ready(worker.materialize(sample_append_only_batch(
                &format!("batch-{batch_index:04}"),
                ordering_start,
            )))
            .expect("materialized batch");
            assert_eq!(materialized.manifest.file_set.len(), 1);
            input_files.push(path_from_file_uri(
                &materialized.manifest.file_set[0].file_uri,
            ));
        }

        let output_dir = std::env::temp_dir()
            .join("greytl-worker-duckdb-compaction-tests")
            .join(unique_suffix());
        let result = compact_parquet_files(
            &WriterConfig {
                target_file_bytes: 1,
                max_row_group_bytes: 1,
            },
            &input_files,
            &output_dir,
            "compacted",
        )
        .expect("compaction succeeds");

        assert!(result.files.len() > 1);
        assert_eq!(result.record_count, 30);

        let output_paths = result
            .files
            .iter()
            .map(|file| path_from_file_uri(&file.file_uri))
            .collect::<Vec<_>>();
        let db = DuckDb::open_in_memory().expect("duckdb");
        db.create_parquet_view("compacted_output", &output_paths)
            .expect("create view");

        let total_rows = db
            .query_i64("SELECT COUNT(*)::BIGINT FROM compacted_output")
            .expect("count rows");
        let distinct_ordering_values = db
            .query_i64("SELECT COUNT(DISTINCT ordering_value)::BIGINT FROM compacted_output")
            .expect("count distinct ordering");

        assert_eq!(total_rows, 30);
        assert_eq!(distinct_ordering_values, 30);
    }

    #[test]
    fn compact_parquet_files_sanitizes_output_prefix_for_paths() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let materialized = run_ready(worker.materialize(sample_append_only_batch("batch-0001", 1)))
            .expect("materialized batch");
        let input_files = vec![path_from_file_uri(
            &materialized.manifest.file_set[0].file_uri,
        )];

        let output_dir = std::env::temp_dir()
            .join("greytl-worker-duckdb-compaction-tests")
            .join(unique_suffix());
        let result = compact_parquet_files(
            &WriterConfig::default(),
            &input_files,
            &output_dir,
            "../outside/subdir",
        )
        .expect("compaction succeeds");

        assert_eq!(result.files.len(), 1);
        let output_path = path_from_file_uri(&result.files[0].file_uri);
        assert!(output_path.starts_with(&output_dir));
        let file_name = output_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("file name");
        assert!(file_name.ends_with(".parquet"));
        assert!(!file_name.contains('/'));
        assert!(!file_name.contains(".."));
    }

    #[test]
    fn compact_parquet_files_preserves_safe_output_prefix_characters() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let materialized = run_ready(worker.materialize(sample_append_only_batch("batch-0001", 1)))
            .expect("materialized batch");
        let input_files = vec![path_from_file_uri(
            &materialized.manifest.file_set[0].file_uri,
        )];

        let output_dir = std::env::temp_dir()
            .join("greytl-worker-duckdb-compaction-tests")
            .join(unique_suffix());
        let result = compact_parquet_files(
            &WriterConfig::default(),
            &input_files,
            &output_dir,
            "compact-000001.v1",
        )
        .expect("compaction succeeds");

        assert_eq!(result.files.len(), 1);
        let output_path = path_from_file_uri(&result.files[0].file_uri);
        let file_name = output_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("file name");
        assert_eq!(file_name, "compact-000001.v1-0000.parquet");
    }

    fn sample_append_only_batch(batch_stem: &str, order_start: i64) -> SourceBatch {
        SourceBatch {
            batch_file: format!("fixtures/orders/{batch_stem}.jsonl"),
            records: vec![
                append_insert(order_start, order_start),
                append_insert(order_start + 1, order_start + 1),
                append_insert(order_start + 2, order_start + 2),
            ],
        }
    }

    fn append_insert(order_id: i64, ordering_value: i64) -> LogicalMutation {
        LogicalMutation::insert(
            table_id("orders"),
            "source-a",
            SourceClass::FileOrObjectDrop,
            TableMode::AppendOnly,
            StructuredKey::new(vec![]),
            ordering("line_number", ordering_value),
            checkpoint(format!("cp-{ordering_value}")),
            1,
            sample_time(),
            BTreeMap::new(),
        )
        .with_after(json!({ "order_id": order_id, "amount_cents": 100 }))
        .build()
        .expect("valid insert")
    }

    fn sample_time() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(1, 0).expect("valid timestamp")
    }

    fn path_from_file_uri(file_uri: &str) -> PathBuf {
        let path = file_uri.strip_prefix("file://").expect("file URI");
        PathBuf::from(path)
    }

    fn unique_suffix() -> String {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos().to_string())
            .unwrap_or_else(|_| "0".to_string())
    }
}

fn unique_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().to_string())
        .unwrap_or_else(|_| "0".to_string())
}
