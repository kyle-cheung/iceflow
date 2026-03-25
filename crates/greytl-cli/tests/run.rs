use anyhow::Result;
use greytl_cli::commands::run::{self, Args, SinkKind};
use std::fs;
use std::path::PathBuf;

#[test]
fn run_command_processes_reference_workload_to_filesystem_sink() -> Result<()> {
    let destination_root = temp_root("run-append-only");
    let report = run::execute_blocking(Args {
        workload: "append_only.orders_events".to_string(),
        destination_uri: format!("file://{}", destination_root.display()),
        sink: SinkKind::Filesystem,
        catalog_uri: None,
        catalog_name: None,
        namespace: None,
        batch_limit: None,
    })?;

    let data_dir = destination_root.join("data");
    let file_count = fs::read_dir(&data_dir)
        .map_err(|err| anyhow::Error::msg(err.to_string()))?
        .count();

    assert_eq!(report.workload, "append_only.orders_events");
    assert_eq!(report.committed_batches, 2);
    assert!(file_count >= 2);
    assert!(report.last_snapshot_uri.is_some());
    assert_eq!(report.durable_checkpoint.as_deref(), Some("cp-0013"));
    Ok(())
}

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join("greytl-cli-tests").join(name);
    let _ = fs::remove_dir_all(&root);
    root
}
