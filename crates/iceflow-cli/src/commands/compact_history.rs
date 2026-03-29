use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::file_uri_path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveFile {
    pub file_uri: String,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CompactionRecord {
    pub sequence: u64,
    pub snapshot_id: String,
    pub snapshot_kind: String,
    pub table_mode: String,
    pub namespace: String,
    pub table: String,
    pub warehouse_uri: String,
    pub min_small_file_bytes: u64,
    pub max_rewrite_files: usize,
    pub removed_files: Vec<String>,
    pub added_files: Vec<String>,
    pub created_at: String,
}

pub(crate) fn load_base_snapshot_files(snapshot_dir: &Path) -> Result<Vec<ActiveFile>> {
    let mut files = Vec::new();
    if !snapshot_dir.exists() {
        anyhow::bail!(
            "missing iceflow snapshot history at {}",
            snapshot_dir.display()
        );
    }

    let mut snapshot_paths = fs::read_dir(snapshot_dir)
        .map_err(|err| Error::msg(format!("{}: {err}", snapshot_dir.display())))?
        .map(|entry| entry.map(|value| value.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|err| Error::msg(err.to_string()))?;
    snapshot_paths.sort();

    for path in snapshot_paths {
        let content = fs::read_to_string(&path)
            .map_err(|err| Error::msg(format!("{}: {err}", path.display())))?;
        for line in content.lines() {
            if let Some(value) = line.strip_prefix("file=") {
                files.retain(|file: &ActiveFile| file.file_uri != value);
                files.push(ActiveFile {
                    file_uri: value.to_string(),
                    file_size_bytes: 0,
                });
            }
        }
    }

    Ok(files)
}

pub(crate) fn load_compaction_records(compaction_dir: &Path) -> Result<Vec<CompactionRecord>> {
    if !compaction_dir.exists() {
        return Ok(Vec::new());
    }

    let mut paths = fs::read_dir(compaction_dir)
        .map_err(|err| Error::msg(format!("{}: {err}", compaction_dir.display())))?
        .map(|entry| entry.map(|value| value.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|err| Error::msg(err.to_string()))?;
    paths.sort();

    let mut records = Vec::new();
    for path in paths {
        let content = fs::read_to_string(&path)
            .map_err(|err| Error::msg(format!("{}: {err}", path.display())))?;
        records.push(
            serde_json::from_str(&content)
                .map_err(|err| Error::msg(format!("{}: {err}", path.display())))?,
        );
    }
    records.sort_by_key(|record: &CompactionRecord| record.sequence);
    Ok(records)
}

pub(crate) fn rebuild_active_files(
    base: Vec<ActiveFile>,
    records: &[CompactionRecord],
) -> Result<Vec<ActiveFile>> {
    let mut ordered = base
        .into_iter()
        .map(|file| file.file_uri)
        .collect::<Vec<_>>();

    for record in records {
        for removed in &record.removed_files {
            ordered.retain(|file_uri| file_uri != removed);
        }
        for added in &record.added_files {
            ordered.retain(|file_uri| file_uri != added);
            ordered.push(added.clone());
        }
    }

    ordered
        .into_iter()
        .map(|file_uri| {
            let path = file_uri_path(&file_uri)?;
            let file_size_bytes = fs::metadata(&path)
                .map_err(|err| Error::msg(format!("{}: {err}", path.display())))?
                .len();
            Ok(ActiveFile {
                file_uri,
                file_size_bytes,
            })
        })
        .collect()
}

pub(crate) fn next_compaction_sequence(compaction_dir: &Path) -> Result<u64> {
    let mut max_sequence = 0u64;
    if !compaction_dir.exists() {
        return Ok(1);
    }
    for entry in fs::read_dir(compaction_dir)
        .map_err(|err| Error::msg(format!("{}: {err}", compaction_dir.display())))?
    {
        let path = entry.map_err(|err| Error::msg(err.to_string()))?.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(prefix) = name.split('-').next() else {
            continue;
        };
        if let Ok(sequence) = prefix.parse::<u64>() {
            max_sequence = max_sequence.max(sequence);
        }
    }
    Ok(max_sequence + 1)
}

pub(crate) fn write_compaction_record(
    compaction_dir: &Path,
    record: &CompactionRecord,
) -> Result<PathBuf> {
    fs::create_dir_all(compaction_dir)
        .map_err(|err| Error::msg(format!("{}: {err}", compaction_dir.display())))?;
    let path = compaction_dir.join(format!(
        "{:06}-{}.json",
        record.sequence, record.snapshot_id
    ));
    let json = serde_json::to_string_pretty(record).map_err(|err| Error::msg(err.to_string()))?;
    fs::write(&path, json).map_err(|err| Error::msg(format!("{}: {err}", path.display())))?;
    Ok(path)
}

pub(crate) fn build_created_at_string() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    format!("{}:{:09}", timestamp.as_secs(), timestamp.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn rebuild_active_files_replays_compaction_records_in_order() -> anyhow::Result<()> {
        let root = std::env::temp_dir()
            .join("iceflow-cli-compact-history-tests")
            .join("replay");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("_iceflow_snapshots"))
            .map_err(|err| Error::msg(err.to_string()))?;
        fs::create_dir_all(root.join("data")).map_err(|err| Error::msg(err.to_string()))?;
        fs::create_dir_all(root.join("compaction")).map_err(|err| Error::msg(err.to_string()))?;

        let file_a = root.join("data/source-a.parquet");
        let file_b = root.join("data/source-b.parquet");
        let file_c = root.join("data/rewrite-c.parquet");
        fs::write(&file_a, b"aaa").map_err(|err| Error::msg(err.to_string()))?;
        fs::write(&file_b, b"bbb").map_err(|err| Error::msg(err.to_string()))?;
        fs::write(&file_c, b"cccccc").map_err(|err| Error::msg(err.to_string()))?;

        fs::write(
            root.join("_iceflow_snapshots/snapshot-0001.txt"),
            format!(
                "snapshot_id=snapshot-0001\nsnapshot_uri=file://{}/_iceflow_snapshots/snapshot-0001.txt\nbatch_id=batch-0001\ndestination_uri=file://{}\nidempotency_key=batch-0001:append\nrecord_count=2\nfile=file://{}\nfile=file://{}",
                root.display(),
                root.display(),
                file_a.display(),
                file_b.display(),
            ),
        )
        .map_err(|err| Error::msg(err.to_string()))?;

        let record = CompactionRecord {
            sequence: 1,
            snapshot_id: "compact-000001".to_string(),
            snapshot_kind: "iceflow-local".to_string(),
            table_mode: "append_only".to_string(),
            namespace: "orders_events".to_string(),
            table: "orders".to_string(),
            warehouse_uri: format!("file://{}", root.display()),
            min_small_file_bytes: 8,
            max_rewrite_files: 8,
            removed_files: vec![format!("file://{}", file_a.display())],
            added_files: vec![format!("file://{}", file_c.display())],
            created_at: "1:000000000".to_string(),
        };
        write_compaction_record(&root.join("compaction"), &record)?;
        fs::remove_file(&file_a).map_err(|err| Error::msg(err.to_string()))?;

        let active = rebuild_active_files(
            load_base_snapshot_files(&root.join("_iceflow_snapshots"))?,
            &load_compaction_records(&root.join("compaction"))?,
        )?;

        assert_eq!(
            active
                .iter()
                .map(|file| file.file_uri.clone())
                .collect::<Vec<_>>(),
            vec![
                format!("file://{}", file_b.display()),
                format!("file://{}", file_c.display()),
            ]
        );
        Ok(())
    }

    #[test]
    fn next_compaction_sequence_skips_existing_records() -> anyhow::Result<()> {
        let dir = std::env::temp_dir()
            .join("iceflow-cli-compact-history-tests")
            .join("sequence");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).map_err(|err| Error::msg(err.to_string()))?;
        fs::write(dir.join("000001-compact-000001.json"), b"{}")
            .map_err(|err| Error::msg(err.to_string()))?;
        fs::write(dir.join("000002-compact-000002.json"), b"{}")
            .map_err(|err| Error::msg(err.to_string()))?;

        assert_eq!(next_compaction_sequence(&dir)?, 3);
        Ok(())
    }

    #[test]
    fn load_compaction_records_orders_by_numeric_sequence() -> anyhow::Result<()> {
        let dir = std::env::temp_dir()
            .join("iceflow-cli-compact-history-tests")
            .join("record-order");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).map_err(|err| Error::msg(err.to_string()))?;

        write_compaction_record(
            &dir,
            &CompactionRecord {
                sequence: 999_999,
                snapshot_id: "compact-999999".to_string(),
                snapshot_kind: "iceflow-local".to_string(),
                table_mode: "append_only".to_string(),
                namespace: "orders_events".to_string(),
                table: "orders".to_string(),
                warehouse_uri: "file:///tmp/orders".to_string(),
                min_small_file_bytes: 8,
                max_rewrite_files: 8,
                removed_files: vec![],
                added_files: vec![],
                created_at: "1:000000000".to_string(),
            },
        )?;
        write_compaction_record(
            &dir,
            &CompactionRecord {
                sequence: 1_000_000,
                snapshot_id: "compact-1000000".to_string(),
                snapshot_kind: "iceflow-local".to_string(),
                table_mode: "append_only".to_string(),
                namespace: "orders_events".to_string(),
                table: "orders".to_string(),
                warehouse_uri: "file:///tmp/orders".to_string(),
                min_small_file_bytes: 8,
                max_rewrite_files: 8,
                removed_files: vec![],
                added_files: vec![],
                created_at: "1:000000001".to_string(),
            },
        )?;

        let records = load_compaction_records(&dir)?;

        assert_eq!(
            records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![999_999, 1_000_000]
        );
        Ok(())
    }
}
