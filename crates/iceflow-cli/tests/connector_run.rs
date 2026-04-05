use anyhow::{Error, Result};
use iceflow_cli::commands::connector_cmd::{self, RunArgs};
use iceflow_state::{StateStore, TestStateStore};
use iceflow_types::TableId;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn connector_run_processes_reference_connector_to_filesystem_sink() -> Result<()> {
    block_on(async {
        let config_root = TempConfigRoot::new("connector-run-orders-append")?;
        let destination_root = temp_output_root("connector-run-orders-append-output");
        config_root.write_orders_append_connector(&destination_root)?;

        let state = TestStateStore::new().await?;
        let report = connector_cmd::run_with_state(
            RunArgs {
                connector_config: config_root.path().join("connectors/orders_append.toml"),
                config_root: config_root.path().to_path_buf(),
                batch_limit: None,
            },
            &state,
        )
        .await?;

        let data_dir = destination_root.join("data");
        let file_count = fs::read_dir(&data_dir)
            .map_err(|err| Error::msg(format!("failed to read {}: {err}", data_dir.display())))?
            .count();

        assert_eq!(report.tables_processed, 1);
        assert_eq!(report.total_committed_batches, 2);
        assert!(file_count >= 2);
        assert_eq!(
            state
                .last_durable_checkpoint_for_table(&canonical_orders_table_id())
                .await?
                .map(|checkpoint| checkpoint.checkpoint.to_string()),
            Some("cp-0013".to_string())
        );
        Ok(())
    })
}

#[test]
fn connector_run_resumes_from_last_durable_checkpoint() -> Result<()> {
    block_on(async {
        let config_root = TempConfigRoot::new("connector-run-resume-orders-append")?;
        let destination_root = temp_output_root("connector-run-resume-orders-append-output");
        config_root.write_orders_append_connector(&destination_root)?;

        let state = TestStateStore::new().await?;
        let first_report = connector_cmd::run_with_state(
            RunArgs {
                connector_config: config_root.path().join("connectors/orders_append.toml"),
                config_root: config_root.path().to_path_buf(),
                batch_limit: Some(1),
            },
            &state,
        )
        .await?;

        assert_eq!(first_report.total_committed_batches, 1);
        assert_eq!(
            state
                .last_durable_checkpoint_for_table(&canonical_orders_table_id())
                .await?
                .map(|checkpoint| checkpoint.checkpoint.to_string()),
            Some("cp-0011".to_string())
        );

        let second_report = connector_cmd::run_with_state(
            RunArgs {
                connector_config: config_root.path().join("connectors/orders_append.toml"),
                config_root: config_root.path().to_path_buf(),
                batch_limit: None,
            },
            &state,
        )
        .await?;

        assert_eq!(second_report.total_committed_batches, 1);
        assert_eq!(
            state
                .last_durable_checkpoint_for_table(&canonical_orders_table_id())
                .await?
                .map(|checkpoint| checkpoint.checkpoint.to_string()),
            Some("cp-0013".to_string())
        );
        Ok(())
    })
}

fn canonical_orders_table_id() -> TableId {
    TableId::from("orders_events.orders_events")
}

fn reference_workload_root() -> PathBuf {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/reference_workload_v0"
    ))
    .to_path_buf()
}

fn next_temp_path(label: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    std::env::temp_dir().join(format!(
        "iceflow-cli-{label}-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn temp_output_root(label: &str) -> PathBuf {
    let root = next_temp_path(label);
    let _ = fs::remove_dir_all(&root);
    root
}

struct TempConfigRoot {
    root: PathBuf,
}

impl TempConfigRoot {
    fn new(label: &str) -> Result<Self> {
        let root = next_temp_path(label);
        if root.exists() {
            fs::remove_dir_all(&root)
                .map_err(|err| Error::msg(format!("failed to reset {}: {err}", root.display())))?;
        }

        fs::create_dir_all(root.join("sources"))
            .map_err(|err| Error::msg(format!("failed to create sources dir: {err}")))?;
        fs::create_dir_all(root.join("destinations"))
            .map_err(|err| Error::msg(format!("failed to create destinations dir: {err}")))?;
        fs::create_dir_all(root.join("connectors"))
            .map_err(|err| Error::msg(format!("failed to create connectors dir: {err}")))?;

        Ok(Self { root })
    }

    fn path(&self) -> &Path {
        &self.root
    }

    fn write(&self, relative_path: &str, contents: &str) -> Result<()> {
        let path = self.root.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                Error::msg(format!("failed to create {}: {err}", parent.display()))
            })?;
        }
        fs::write(&path, contents)
            .map_err(|err| Error::msg(format!("failed to write {}: {err}", path.display())))?;
        Ok(())
    }

    fn write_orders_append_connector(&self, destination_root: &Path) -> Result<()> {
        self.write(
            "sources/local_file.toml",
            &format!(
                "version = 1\nkind = \"file\"\n\n[properties]\nfixture_root = \"{}\"\n",
                reference_workload_root().display()
            ),
        )?;
        self.write(
            "destinations/local_fs.toml",
            &format!(
                "version = 1\nkind = \"filesystem\"\n\n[properties]\nroot_uri = \"{}\"\n",
                destination_root.display()
            ),
        )?;
        self.write(
            "connectors/orders_append.toml",
            r#"version = 1
source = "local_file"
destination = "local_fs"

[[tables]]
source_schema = ""
source_table = "orders_events"
destination_namespace = "orders_events"
destination_table = "orders_events"
table_mode = "append_only"
"#,
        )
    }
}

impl Drop for TempConfigRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime should build")
        .block_on(future)
}
