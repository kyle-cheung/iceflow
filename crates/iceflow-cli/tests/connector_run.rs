use anyhow::{Error, Result};
use iceflow_cli::commands::connector_cmd::{self, RunArgs};
use iceflow_state::{SqliteStateStore, StateStore, TestStateStore};
use iceflow_types::TableId;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn connector_run_processes_reference_connector_to_filesystem_sink() -> Result<()> {
    block_on(async {
        let config_root = TempConfigRoot::new("connector-run-orders-append")?;
        let destination_root = TempOutputRoot::new("connector-run-orders-append-output")?;
        config_root.write_orders_append_connector(destination_root.path())?;

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

        let data_dir = destination_root.path().join("data");
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
fn connector_run_blocking_persists_state_under_config_root() -> Result<()> {
    block_on(async {
        let config_root = TempConfigRoot::new("connector-run-persistent-state")?;
        let destination_root = TempOutputRoot::new("connector-run-persistent-state-output")?;
        config_root.write_orders_append_connector(destination_root.path())?;

        let connector_config = config_root.path().join("connectors/orders_append.toml");
        let state_path = config_root
            .path()
            .join(".iceflow/state/orders_append.sqlite3");

        assert!(!state_path.exists());

        let first_report = connector_cmd::run_blocking(RunArgs {
            connector_config: connector_config.clone(),
            config_root: config_root.path().to_path_buf(),
            batch_limit: Some(1),
        })?;

        assert_eq!(first_report.total_committed_batches, 1);
        assert!(state_path.exists());
        assert_eq!(
            SqliteStateStore::open_persistent(&state_path)
                .await?
                .last_durable_checkpoint_for_table(&canonical_orders_table_id())
                .await?
                .map(|checkpoint| checkpoint.checkpoint.to_string()),
            Some("cp-0011".to_string())
        );

        let second_report = connector_cmd::run_blocking(RunArgs {
            connector_config,
            config_root: config_root.path().to_path_buf(),
            batch_limit: None,
        })?;

        assert_eq!(second_report.total_committed_batches, 1);
        assert_eq!(
            SqliteStateStore::open_persistent(&state_path)
                .await?
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
        let destination_root = TempOutputRoot::new("connector-run-resume-orders-append-output")?;
        config_root.write_orders_append_connector(destination_root.path())?;

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

#[test]
fn connector_run_batch_limit_is_global_across_tables() -> Result<()> {
    block_on(async {
        let config_root = TempConfigRoot::new("connector-run-multi-table-limit")?;
        let destination_root = TempOutputRoot::new("connector-run-multi-table-limit-output")?;
        config_root.write_multi_table_orders_connector(destination_root.path())?;

        let state = TestStateStore::new().await?;
        let report = connector_cmd::run_with_state(
            RunArgs {
                connector_config: config_root
                    .path()
                    .join("connectors/orders_append_multi.toml"),
                config_root: config_root.path().to_path_buf(),
                batch_limit: Some(3),
            },
            &state,
        )
        .await?;

        let data_dir = destination_root.path().join("data");
        let file_count = fs::read_dir(&data_dir)
            .map_err(|err| Error::msg(format!("failed to read {}: {err}", data_dir.display())))?
            .count();

        assert_eq!(report.tables_processed, 2);
        assert_eq!(report.total_committed_batches, 3);
        assert_eq!(file_count, 3);
        assert_eq!(
            state
                .last_durable_checkpoint_for_table(&canonical_orders_left_table_id())
                .await?
                .map(|checkpoint| checkpoint.checkpoint.to_string()),
            Some("cp-0013".to_string())
        );
        assert_eq!(
            state
                .last_durable_checkpoint_for_table(&canonical_orders_right_table_id())
                .await?
                .map(|checkpoint| checkpoint.checkpoint.to_string()),
            Some("cp-0011".to_string())
        );
        Ok(())
    })
}

fn canonical_orders_table_id() -> TableId {
    TableId::from("orders_events.orders_events")
}

fn canonical_orders_left_table_id() -> TableId {
    TableId::from("orders_left.orders_left")
}

fn canonical_orders_right_table_id() -> TableId {
    TableId::from("orders_right.orders_right")
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

struct TempOutputRoot {
    root: PathBuf,
}

impl TempOutputRoot {
    fn new(label: &str) -> Result<Self> {
        let root = next_temp_path(label);
        if root.exists() {
            fs::remove_dir_all(&root)
                .map_err(|err| Error::msg(format!("failed to reset {}: {err}", root.display())))?;
        }
        Ok(Self { root })
    }

    fn path(&self) -> &Path {
        &self.root
    }
}

impl Drop for TempOutputRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
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

    fn write_multi_table_orders_connector(&self, destination_root: &Path) -> Result<()> {
        let fixture_root = self.root.join("fixture_root");
        self.copy_orders_events_fixture(
            reference_workload_root().join("orders_events"),
            fixture_root.join("orders_left"),
            &["batch-0001.jsonl", "batch-0002.jsonl"],
        )?;
        self.copy_orders_events_fixture(
            reference_workload_root().join("orders_events"),
            fixture_root.join("orders_right"),
            &["batch-right-0001.jsonl", "batch-right-0002.jsonl"],
        )?;
        self.write(
            "sources/local_file.toml",
            &format!(
                "version = 1\nkind = \"file\"\n\n[properties]\nfixture_root = \"{}\"\n",
                fixture_root.display()
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
            "connectors/orders_append_multi.toml",
            r#"version = 1
source = "local_file"
destination = "local_fs"

[[tables]]
source_schema = ""
source_table = "orders_left"
destination_namespace = "orders_left"
destination_table = "orders_left"
table_mode = "append_only"

[[tables]]
source_schema = ""
source_table = "orders_right"
destination_namespace = "orders_right"
destination_table = "orders_right"
table_mode = "append_only"
"#,
        )
    }

    fn copy_orders_events_fixture(
        &self,
        source_dir: PathBuf,
        destination_dir: PathBuf,
        destination_filenames: &[&str],
    ) -> Result<()> {
        fs::create_dir_all(&destination_dir).map_err(|err| {
            Error::msg(format!(
                "failed to create fixture dir {}: {err}",
                destination_dir.display()
            ))
        })?;

        for (index, destination_filename) in destination_filenames.iter().enumerate() {
            let source_path = source_dir.join(format!("batch-{:04}.jsonl", index + 1));
            let destination_path = destination_dir.join(destination_filename);
            let contents = fs::read_to_string(&source_path).map_err(|err| {
                Error::msg(format!("failed to read {}: {err}", source_path.display()))
            })?;
            fs::write(&destination_path, contents).map_err(|err| {
                Error::msg(format!(
                    "failed to write fixture copy {}: {err}",
                    destination_path.display()
                ))
            })?;
        }

        Ok(())
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
