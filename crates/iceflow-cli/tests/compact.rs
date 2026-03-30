mod support;

use anyhow::Result;
use iceflow_cli::commands::compact::{self, Args};
use iceflow_cli::commands::run::{self, Args as RunArgs, SinkKind};
use iceflow_types::TableMode;
use std::fs;
use std::path::{Path, PathBuf};

use support::polaris_mock::MockPolarisServer;

#[test]
fn compact_command_rewrites_small_files_and_emits_report() -> Result<()> {
    let server = MockPolarisServer::start("quickstart_catalog");
    let fixture = compactable_table("compact-rewrite", &server)?;

    let report = compact::execute_blocking(Args {
        warehouse_uri: format!("file://{}", fixture.table_root.display()),
        catalog_uri: server.catalog_uri(),
        catalog_name: server.warehouse().to_string(),
        namespace: "orders_events".to_string(),
        table: "orders".to_string(),
        table_mode: TableMode::AppendOnly,
        min_small_file_bytes: u64::MAX,
        max_rewrite_files: 8,
    })?;

    assert_eq!(report.mode, "offline");
    assert_eq!(report.snapshot_kind, "iceflow-local");
    assert!(report.rewritten_files >= 2);
    assert!(report.snapshot_id.is_some());
    assert_eq!(compaction_record_count(&fixture.table_root)?, 1);
    Ok(())
}

#[test]
fn compact_command_second_run_is_a_noop_after_record_replay() -> Result<()> {
    let server = MockPolarisServer::start("quickstart_catalog");
    let fixture = compactable_table("compact-second-run", &server)?;
    let args = Args {
        warehouse_uri: format!("file://{}", fixture.table_root.display()),
        catalog_uri: server.catalog_uri(),
        catalog_name: server.warehouse().to_string(),
        namespace: "orders_events".to_string(),
        table: "orders".to_string(),
        table_mode: TableMode::AppendOnly,
        min_small_file_bytes: u64::MAX,
        max_rewrite_files: 8,
    };

    let _first = compact::execute_blocking(args.clone())?;
    let second = compact::execute_blocking(args)?;

    assert_eq!(second.rewritten_files, 0);
    assert_eq!(compaction_record_count(&fixture.table_root)?, 1);
    Ok(())
}

#[test]
fn compact_command_rejects_keyed_upsert_tables_with_clear_error() -> Result<()> {
    let server = MockPolarisServer::start("quickstart_catalog");
    let fixture = compactable_table("compact-keyed-reject", &server)?;

    let err = compact::execute_blocking(Args {
        warehouse_uri: format!("file://{}", fixture.table_root.display()),
        catalog_uri: server.catalog_uri(),
        catalog_name: server.warehouse().to_string(),
        namespace: "orders_events".to_string(),
        table: "orders".to_string(),
        table_mode: TableMode::KeyedUpsert,
        min_small_file_bytes: u64::MAX,
        max_rewrite_files: 8,
    })
    .expect_err("keyed_upsert should be rejected");

    assert_eq!(
        err.to_string(),
        "compaction of keyed_upsert tables is not supported in this version"
    );
    Ok(())
}

struct CompactFixture {
    table_root: PathBuf,
}

fn compactable_table(test_name: &str, server: &MockPolarisServer) -> Result<CompactFixture> {
    let table_root = std::env::temp_dir()
        .join("iceflow-cli-compact-tests")
        .join(test_name);
    let _ = fs::remove_dir_all(&table_root);

    let _ = run::execute_blocking(RunArgs {
        workload: "append_only.orders_events".to_string(),
        destination_uri: format!("file://{}", table_root.display()),
        sink: SinkKind::Polaris,
        catalog_uri: Some(server.catalog_uri()),
        catalog_name: Some(server.warehouse().to_string()),
        namespace: Some("orders_events".to_string()),
        batch_limit: None,
    })?;

    Ok(CompactFixture { table_root })
}

fn compaction_record_count(table_root: &Path) -> Result<usize> {
    let dir = table_root.join("compaction");
    if !dir.exists() {
        return Ok(0);
    }

    Ok(fs::read_dir(&dir)
        .map_err(|err| anyhow::Error::msg(format!("{}: {err}", dir.display())))?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|value| value.to_str()) == Some("json"))
        .count())
}
