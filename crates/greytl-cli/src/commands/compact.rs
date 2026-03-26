use anyhow::{Error, Result};
use greytl_types::TableMode;
use greytl_worker_duckdb::{compact_parquet_files, WriterConfig};
use serde::Serialize;
use std::path::PathBuf;
use std::time::Instant;

use super::compact_catalog::{CompactionCatalog, PolarisCompactionCatalog};
use super::compact_history::{
    build_created_at_string, load_base_snapshot_files, load_compaction_records,
    next_compaction_sequence, rebuild_active_files, write_compaction_record, ActiveFile,
    CompactionRecord,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub warehouse_uri: String,
    pub catalog_uri: String,
    pub catalog_name: String,
    pub namespace: String,
    pub table: String,
    pub table_mode: TableMode,
    pub min_small_file_bytes: u64,
    pub max_rewrite_files: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompactReport {
    pub mode: &'static str,
    pub snapshot_kind: &'static str,
    pub rewritten_files: usize,
    pub skipped_files: usize,
    pub snapshot_id: Option<String>,
    pub elapsed_ms: u64,
}

impl CompactReport {
    pub fn to_json(&self) -> String {
        serde_json_ext::to_string(self).expect("compact report serialization should not fail")
    }
}

impl Args {
    pub fn parse(args: Vec<String>) -> Result<Self> {
        let mut warehouse_uri = None;
        let mut catalog_uri = None;
        let mut catalog_name = None;
        let mut namespace = None;
        let mut table = None;
        let mut table_mode = None;
        let mut min_small_file_bytes = None;
        let mut max_rewrite_files = None;
        let mut index = 0;

        while index < args.len() {
            match args[index].as_str() {
                "--warehouse-uri" => {
                    index += 1;
                    warehouse_uri = args.get(index).cloned();
                }
                "--catalog-uri" => {
                    index += 1;
                    catalog_uri = args.get(index).cloned();
                }
                "--catalog" => {
                    index += 1;
                    catalog_name = args.get(index).cloned();
                }
                "--namespace" => {
                    index += 1;
                    namespace = args.get(index).cloned();
                }
                "--table" => {
                    index += 1;
                    table = args.get(index).cloned();
                }
                "--table-mode" => {
                    index += 1;
                    table_mode =
                        Some(parse_table_mode(args.get(index).ok_or_else(|| {
                            Error::msg("--table-mode requires a value")
                        })?)?);
                }
                "--min-small-file-bytes" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| Error::msg("--min-small-file-bytes requires a value"))?;
                    min_small_file_bytes = Some(
                        value
                            .parse::<u64>()
                            .map_err(|_| Error::msg("--min-small-file-bytes must be a u64"))?,
                    );
                }
                "--max-rewrite-files" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| Error::msg("--max-rewrite-files requires a value"))?;
                    let parsed = value.parse::<usize>().map_err(|_| {
                        Error::msg("--max-rewrite-files must be a positive integer")
                    })?;
                    if parsed == 0 {
                        anyhow::bail!("--max-rewrite-files must be greater than zero");
                    }
                    max_rewrite_files = Some(parsed);
                }
                other => anyhow::bail!("unknown compact argument: {}", other),
            }
            index += 1;
        }

        Ok(Self {
            warehouse_uri: warehouse_uri
                .ok_or_else(|| Error::msg("--warehouse-uri is required"))?,
            catalog_uri: catalog_uri.ok_or_else(|| Error::msg("--catalog-uri is required"))?,
            catalog_name: catalog_name.ok_or_else(|| Error::msg("--catalog is required"))?,
            namespace: namespace.ok_or_else(|| Error::msg("--namespace is required"))?,
            table: table.ok_or_else(|| Error::msg("--table is required"))?,
            table_mode: table_mode.ok_or_else(|| Error::msg("--table-mode is required"))?,
            min_small_file_bytes: min_small_file_bytes
                .ok_or_else(|| Error::msg("--min-small-file-bytes is required"))?,
            max_rewrite_files: max_rewrite_files
                .ok_or_else(|| Error::msg("--max-rewrite-files is required"))?,
        })
    }
}

pub fn execute_blocking(args: Args) -> Result<CompactReport> {
    crate::block_on(execute(args))
}

pub async fn execute(args: Args) -> Result<CompactReport> {
    let started = Instant::now();
    if args.table_mode != TableMode::AppendOnly {
        anyhow::bail!("compaction of keyed_upsert tables is not supported in this version");
    }

    let catalog = PolarisCompactionCatalog::new(
        args.catalog_uri.clone(),
        args.catalog_name.clone(),
        args.namespace.clone(),
    );
    catalog.validate_target()?;

    let table_root = file_uri_path(&args.warehouse_uri)?;
    let snapshot_dir = catalog.snapshot_dir(&table_root);
    let compaction_dir = table_root.join("compaction");
    let base_files = load_base_snapshot_files(&snapshot_dir)?;
    let records = load_compaction_records(&compaction_dir)?;
    let active_files = rebuild_active_files(base_files, &records)?;

    let mut candidates = active_files
        .into_iter()
        .filter(|file| file.file_size_bytes < args.min_small_file_bytes)
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.file_uri.cmp(&right.file_uri));
    let skipped_files = candidates.len().saturating_sub(args.max_rewrite_files);
    candidates.truncate(args.max_rewrite_files);

    if candidates.len() < 2 {
        return Ok(CompactReport {
            mode: "offline",
            snapshot_kind: "greytl-local",
            rewritten_files: 0,
            skipped_files: skipped_files + candidates.len(),
            snapshot_id: None,
            elapsed_ms: started.elapsed().as_millis() as u64,
        });
    }

    let sequence = next_compaction_sequence(&compaction_dir)?;
    let snapshot_id = format!("compact-{sequence:06}");
    let compacted = compact_parquet_files(
        &WriterConfig::default(),
        &candidate_paths(&candidates)?,
        &table_root.join("data"),
        &snapshot_id,
    )?;

    let record = CompactionRecord {
        sequence,
        snapshot_id: snapshot_id.clone(),
        snapshot_kind: "greytl-local".to_string(),
        table_mode: args.table_mode.stable_tag().to_string(),
        namespace: args.namespace.clone(),
        table: args.table.clone(),
        warehouse_uri: args.warehouse_uri.clone(),
        min_small_file_bytes: args.min_small_file_bytes,
        max_rewrite_files: args.max_rewrite_files,
        removed_files: candidates
            .iter()
            .map(|file| file.file_uri.clone())
            .collect(),
        added_files: compacted
            .files
            .iter()
            .map(|file| file.file_uri.clone())
            .collect(),
        created_at: build_created_at_string(),
    };
    write_compaction_record(&compaction_dir, &record)?;

    Ok(CompactReport {
        mode: "offline",
        snapshot_kind: "greytl-local",
        rewritten_files: record.removed_files.len(),
        skipped_files,
        snapshot_id: Some(snapshot_id),
        elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

fn parse_table_mode(value: &str) -> Result<TableMode> {
    match value {
        "append_only" => Ok(TableMode::AppendOnly),
        "keyed_upsert" => Ok(TableMode::KeyedUpsert),
        other => anyhow::bail!("unsupported table mode: {}", other),
    }
}

fn candidate_paths(candidates: &[ActiveFile]) -> Result<Vec<PathBuf>> {
    candidates
        .iter()
        .map(|file| file_uri_path(&file.file_uri))
        .collect()
}

fn file_uri_path(uri: &str) -> Result<PathBuf> {
    uri.strip_prefix("file://")
        .map(PathBuf::from)
        .ok_or_else(|| Error::msg(format!("compact requires file:// uri, got {uri}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_args_require_catalog_context_and_table_mode() {
        let missing_catalog = Args::parse(vec![
            "--warehouse-uri".to_string(),
            "file:///tmp/orders".to_string(),
            "--table".to_string(),
            "orders".to_string(),
        ])
        .expect_err("compact args should reject missing catalog inputs");

        assert!(missing_catalog.to_string().contains("--catalog-uri"));

        let missing_table_mode = Args::parse(vec![
            "--warehouse-uri".to_string(),
            "file:///tmp/orders".to_string(),
            "--catalog-uri".to_string(),
            "http://127.0.0.1:8181/api/catalog".to_string(),
            "--catalog".to_string(),
            "quickstart_catalog".to_string(),
            "--namespace".to_string(),
            "orders_events".to_string(),
            "--table".to_string(),
            "orders".to_string(),
            "--min-small-file-bytes".to_string(),
            "1024".to_string(),
            "--max-rewrite-files".to_string(),
            "8".to_string(),
        ])
        .expect_err("compact args should reject missing table mode");

        assert!(missing_table_mode.to_string().contains("--table-mode"));
    }

    #[test]
    fn compact_report_renders_machine_readable_json() {
        let report = CompactReport {
            mode: "offline",
            snapshot_kind: "greytl-local",
            rewritten_files: 2,
            skipped_files: 1,
            snapshot_id: Some("compact-000001".to_string()),
            elapsed_ms: 12,
        };

        let json = report.to_json();
        assert!(json.contains("\"mode\":\"offline\""));
        assert!(json.contains("\"snapshot_kind\":\"greytl-local\""));
        assert!(json.contains("\"snapshot_id\":\"compact-000001\""));
    }

    #[test]
    fn compact_command_rejects_keyed_upsert_tables() {
        let err = execute_blocking(Args {
            warehouse_uri: "file:///tmp/orders".to_string(),
            catalog_uri: "http://127.0.0.1:8181/api/catalog".to_string(),
            catalog_name: "quickstart_catalog".to_string(),
            namespace: "orders_events".to_string(),
            table: "orders".to_string(),
            table_mode: TableMode::KeyedUpsert,
            min_small_file_bytes: 1024,
            max_rewrite_files: 8,
        })
        .expect_err("keyed_upsert compaction should be rejected");

        assert_eq!(
            err.to_string(),
            "compaction of keyed_upsert tables is not supported in this version"
        );
    }
}
