mod support;

use anyhow::{Error, Result};
use iceflow_state::{
    checkpoint_ack, checkpoint_ref, AttemptResolution, CommitRequest, SnapshotRef,
    SqliteStateStore, StateStore,
};
use iceflow_types::{
    BatchId, BatchManifest, CheckpointId, IceflowDateTime, IceflowUtc, ManifestFile, Operation,
    SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use support::snowflake_live::LiveSnowflakeHarness;

const SNOWFLAKE_MUTABLE_SINK_WARNING_FRAGMENT: &str = "Snowflake updates and deletes";

#[test]
fn connector_check_validates_file_source_connector() -> Result<()> {
    let report = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: fixtures().join("connectors/orders_append.toml"),
            config_root: fixtures().to_path_buf(),
        },
    )?;

    assert!(report.valid);
    assert!(report.errors.is_empty());
    assert_eq!(report.table_count, 1);
    Ok(())
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn connector_check_live_snowflake_connector() -> Result<()> {
    let harness = LiveSnowflakeHarness::new()?;
    let config_root = harness.config_root();
    let state_dir = config_root.join(".iceflow").join("state");

    harness.reset_table()?; // Reset Snowflake table state before running the live check.

    let report = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: harness.connector_config(),
            config_root,
        },
    )?;

    assert!(
        report.valid,
        "expected valid Snowflake connector check, got errors={:?}, warnings={:?}",
        report.errors, report.warnings
    );
    assert!(
        report.errors.is_empty(),
        "unexpected errors from Snowflake connector check: {:?}",
        report.errors
    );
    assert_eq!(
        report.table_count, 1,
        "expected exactly one table in check report, got {:?}",
        report
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains(SNOWFLAKE_MUTABLE_SINK_WARNING_FRAGMENT)),
        "expected Snowflake mutable-sink warning in {:?}",
        report.warnings
    );
    let state_exists = state_dir
        .try_exists()
        .map_err(|err| Error::msg(format!("failed to stat {}: {err}", state_dir.display())))?;
    assert!(
        !state_exists,
        "connector check should not create state directory, found {:?}",
        state_dir
    );
    Ok(())
}

#[test]
fn connector_check_does_not_create_persistent_state_directory() -> Result<()> {
    let config_root = TempConfigRoot::new("connector-check-stateless")?;
    config_root.write(
        "sources/local_file.toml",
        &format!(
            "version = 1\nkind = \"file\"\n\n[properties]\nfixture_root = \"{}\"\n",
            orders_events_fixture_root().display()
        ),
    )?;
    config_root.write(
        "destinations/local_fs.toml",
        "version = 1\nkind = \"filesystem\"\n\n[properties]\nroot_uri = \"/tmp/iceflow-output\"\n",
    )?;
    config_root.write(
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
    )?;

    let state_dir = config_root.path().join(".iceflow/state");
    assert!(!state_dir.exists());

    let report = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: config_root.path().join("connectors/orders_append.toml"),
            config_root: config_root.path().to_path_buf(),
        },
    )?;

    assert!(report.valid);
    assert!(!state_dir.exists());
    Ok(())
}

#[test]
fn connector_check_rejects_invalid_persisted_snowflake_checkpoint_before_source_build() -> Result<()>
{
    let config_root = TempConfigRoot::new("connector-check-invalid-snowflake-checkpoint")?;
    config_root.write(
        "sources/local_snowflake.toml",
        r#"version = 1
kind = "snowflake"

[properties]
account = "xy12345.us-east-1"
user = "ICEFLOW_DEMO"
password = "secret"
warehouse = "ICEFLOW_WH"
role = "ICEFLOW_ROLE"
database = "SOURCE_DB"
auth_method = "password"
"#,
    )?;
    config_root.write(
        "destinations/local_fs.toml",
        "version = 1\nkind = \"filesystem\"\n\n[properties]\nroot_uri = \"/tmp/iceflow-output\"\n",
    )?;
    config_root.write(
        "connectors/snowflake_customer_state_append.toml",
        r#"version = 1
source = "local_snowflake"
destination = "local_fs"

[[tables]]
source_schema = "PUBLIC"
source_table = "CUSTOMER_STATE"
destination_namespace = "customer_state"
destination_table = "customer_state"
table_mode = "append_only"
"#,
    )?;

    block_on(seed_persisted_snowflake_checkpoint(
        config_root.path(),
        CheckpointId::from("snowflake:v1:stream:".to_string()),
    ))?;

    let err = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: config_root
                .path()
                .join("connectors/snowflake_customer_state_append.toml"),
            config_root: config_root.path().to_path_buf(),
        },
    )
    .expect_err("invalid persisted snowflake checkpoint should fail");

    assert!(err.to_string().contains("invalid durable checkpoint"));
    assert!(err.to_string().contains("snowflake:v1:stream:"));
    assert!(
        !err.to_string().to_ascii_lowercase().contains("adbc"),
        "invalid checkpoint should fail before Snowflake source build/connect: {err}"
    );
    Ok(())
}

#[test]
fn connector_check_resolves_optional_catalog() -> Result<()> {
    let report = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: fixtures().join("connectors/orders_append_polaris.toml"),
            config_root: fixtures().to_path_buf(),
        },
    )?;

    assert!(report.valid);
    assert!(report.errors.is_empty());
    assert_eq!(report.table_count, 1);
    Ok(())
}

#[test]
fn connector_check_rejects_missing_source() {
    let err = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: fixtures().join("connectors/orders_append.toml"),
            config_root: std::path::PathBuf::from("/nonexistent"),
        },
    )
    .expect_err("missing source should fail");

    assert!(err.to_string().contains("source"));
}

#[test]
fn connector_check_validates_keyed_upsert_connector() -> Result<()> {
    let config_root = TempConfigRoot::new("connector-check-keyed-upsert-valid")?;
    config_root.write(
        "sources/customer_state.toml",
        &format!(
            "version = 1\nkind = \"file\"\n\n[properties]\nfixture_root = \"{}\"\n",
            customer_state_fixture_root().display()
        ),
    )?;
    config_root.write(
        "destinations/local_fs.toml",
        "version = 1\nkind = \"filesystem\"\n\n[properties]\nroot_uri = \"/tmp/iceflow-output\"\n",
    )?;
    config_root.write(
        "connectors/customer_state_upsert.toml",
        r#"version = 1
source = "customer_state"
destination = "local_fs"

[[tables]]
source_schema = ""
source_table = "customer_state"
destination_namespace = "customer_state"
destination_table = "customer_state"
table_mode = "keyed_upsert"
key_columns = ["customer_id"]
ordering_field = "source_position"
"#,
    )?;

    let report = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: config_root
                .path()
                .join("connectors/customer_state_upsert.toml"),
            config_root: config_root.path().to_path_buf(),
        },
    )?;

    assert!(report.valid);
    assert!(report.errors.is_empty());
    assert_eq!(report.table_count, 1);
    Ok(())
}

#[test]
fn connector_check_rejects_keyed_upsert_without_key_columns() -> Result<()> {
    let config_root = TempConfigRoot::new("connector-check-keyed-upsert-missing-key")?;
    config_root.write(
        "sources/customer_state.toml",
        &format!(
            "version = 1\nkind = \"file\"\n\n[properties]\nfixture_root = \"{}\"\n",
            customer_state_fixture_root().display()
        ),
    )?;
    config_root.write(
        "destinations/local_fs.toml",
        "version = 1\nkind = \"filesystem\"\n\n[properties]\nroot_uri = \"/tmp/iceflow-output\"\n",
    )?;
    config_root.write(
        "connectors/customer_state_upsert.toml",
        r#"version = 1
source = "customer_state"
destination = "local_fs"

[[tables]]
source_schema = ""
source_table = "customer_state"
destination_namespace = "customer_state"
destination_table = "customer_state"
table_mode = "keyed_upsert"
ordering_field = "source_position"
"#,
    )?;

    let report = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: config_root
                .path()
                .join("connectors/customer_state_upsert.toml"),
            config_root: config_root.path().to_path_buf(),
        },
    )?;

    assert!(!report.valid);
    assert!(report
        .errors
        .iter()
        .any(|error| error.contains("keyed_upsert requires key_columns")));
    Ok(())
}

#[test]
fn connector_check_rejects_polaris_destination_without_catalog_reference() -> Result<()> {
    let config_root = TempConfigRoot::new("connector-check-polaris-missing-catalog")?;
    config_root.write(
        "sources/local_file.toml",
        &format!(
            "version = 1\nkind = \"file\"\n\n[properties]\nfixture_root = \"{}\"\n",
            orders_events_fixture_root().display()
        ),
    )?;
    config_root.write(
        "destinations/local_polaris.toml",
        "version = 1\nkind = \"polaris\"\n\n[properties]\nwarehouse_uri = \"file:///tmp/iceflow-output\"\n",
    )?;
    config_root.write(
        "connectors/orders_append_polaris.toml",
        r#"version = 1
source = "local_file"
destination = "local_polaris"

[[tables]]
source_schema = ""
source_table = "orders_events"
destination_namespace = "orders_events"
destination_table = "orders_events"
table_mode = "append_only"
"#,
    )?;

    let report = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: config_root
                .path()
                .join("connectors/orders_append_polaris.toml"),
            config_root: config_root.path().to_path_buf(),
        },
    )?;

    assert!(!report.valid);
    assert!(report
        .errors
        .iter()
        .any(|error| error.contains("destination 'local_polaris' requires a catalog reference")));
    Ok(())
}

#[test]
fn connector_check_catalog_conflict_does_not_report_missing_catalog_reference() -> Result<()> {
    let config_root = TempConfigRoot::new("connector-check-polaris-conflict")?;
    config_root.write(
        "sources/local_file.toml",
        &format!(
            "version = 1\nkind = \"file\"\n\n[properties]\nfixture_root = \"{}\"\n",
            orders_events_fixture_root().display()
        ),
    )?;
    config_root.write(
        "destinations/local_polaris.toml",
        "version = 1\nkind = \"polaris\"\ncatalog = \"catalog_a\"\n\n[properties]\nwarehouse_uri = \"file:///tmp/iceflow-output\"\n",
    )?;
    config_root.write(
        "catalogs/catalog_a.toml",
        "version = 1\nkind = \"polaris\"\nendpoint = \"http://127.0.0.1:8181/api/catalog\"\nwarehouse = \"quickstart_catalog\"\n",
    )?;
    config_root.write(
        "catalogs/catalog_b.toml",
        "version = 1\nkind = \"polaris\"\nendpoint = \"http://127.0.0.1:8181/api/catalog\"\nwarehouse = \"quickstart_catalog\"\n",
    )?;
    config_root.write(
        "connectors/orders_append_polaris.toml",
        r#"version = 1
source = "local_file"
destination = "local_polaris"
catalog = "catalog_b"

[[tables]]
source_schema = ""
source_table = "orders_events"
destination_namespace = "orders_events"
destination_table = "orders_events"
table_mode = "append_only"
"#,
    )?;

    let report = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: config_root
                .path()
                .join("connectors/orders_append_polaris.toml"),
            config_root: config_root.path().to_path_buf(),
        },
    )?;

    assert!(!report.valid);
    assert!(report
        .errors
        .iter()
        .any(|error| error.contains("does not match destination catalog")));
    assert!(!report
        .errors
        .iter()
        .any(|error| error.contains("requires a catalog reference")));
    Ok(())
}

#[test]
fn connector_check_reports_invalid_resolved_catalog_file_without_aborting() -> Result<()> {
    let config_root = TempConfigRoot::new("connector-check-invalid-resolved-catalog")?;
    config_root.write(
        "sources/local_file.toml",
        &format!(
            "version = 1\nkind = \"file\"\n\n[properties]\nfixture_root = \"{}\"\n",
            orders_events_fixture_root().display()
        ),
    )?;
    config_root.write(
        "destinations/local_polaris.toml",
        "version = 1\nkind = \"polaris\"\ncatalog = \"catalog_a\"\n\n[properties]\nwarehouse_uri = \"file:///tmp/iceflow-output\"\n",
    )?;
    config_root.write(
        "catalogs/catalog_a.toml",
        "version = 1\nkind = \"polaris\"\nendpoint = [\n",
    )?;
    config_root.write(
        "connectors/orders_append_polaris.toml",
        r#"version = 1
source = "local_file"
destination = "local_polaris"

[[tables]]
source_schema = ""
source_table = "orders_events"
destination_namespace = "orders_events"
destination_table = "orders_events"
table_mode = "append_only"
"#,
    )?;

    let report = iceflow_cli::commands::connector_cmd::check_blocking(
        iceflow_cli::commands::connector_cmd::CheckArgs {
            connector_config: config_root
                .path()
                .join("connectors/orders_append_polaris.toml"),
            config_root: config_root.path().to_path_buf(),
        },
    )?;

    assert!(!report.valid);
    assert!(report
        .errors
        .iter()
        .any(|error| error.contains("catalog 'catalog_a': failed to parse catalog config")));
    Ok(())
}

fn fixtures() -> std::path::PathBuf {
    std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/config_samples"
    ))
    .to_path_buf()
}

fn reference_workload_root() -> PathBuf {
    fixtures()
        .parent()
        .expect("config_samples parent")
        .join("reference_workload_v0")
}

fn orders_events_fixture_root() -> PathBuf {
    reference_workload_root().join("orders_events")
}

fn customer_state_fixture_root() -> PathBuf {
    reference_workload_root().join("customer_state")
}

fn next_temp_config_root(name: &str) -> PathBuf {
    static NEXT_TEMP_CONFIG_ROOT_ID: AtomicU64 = AtomicU64::new(0);

    std::env::temp_dir().join(format!(
        "iceflow-cli-{name}-{}-{}",
        std::process::id(),
        NEXT_TEMP_CONFIG_ROOT_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

struct TempConfigRoot {
    root: PathBuf,
}

impl TempConfigRoot {
    fn new(name: &str) -> Result<Self> {
        let root = next_temp_config_root(name);
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
        fs::create_dir_all(root.join("catalogs"))
            .map_err(|err| Error::msg(format!("failed to create catalogs dir: {err}")))?;
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
}

impl Drop for TempConfigRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

async fn seed_persisted_snowflake_checkpoint(
    config_root: &Path,
    checkpoint_id: CheckpointId,
) -> Result<()> {
    let state_path = config_root
        .join(".iceflow")
        .join("state")
        .join("snowflake_customer_state_append.sqlite3");
    let store = SqliteStateStore::open_persistent(&state_path).await?;
    let batch_id = store.register_batch(sample_snowflake_manifest()).await?;
    durable_checkpoint_batch(
        &store,
        &batch_id,
        checkpoint_id,
        "file:///tmp/snowflake-checkpoint",
    )
    .await
}

fn sample_snowflake_manifest() -> BatchManifest {
    let created_at = IceflowDateTime::<IceflowUtc>::from_timestamp(5, 0).expect("valid timestamp");

    BatchManifest {
        batch_id: BatchId::from("batch-0001"),
        table_id: TableId::from("customer_state.customer_state"),
        table_mode: TableMode::AppendOnly,
        source_id: "snowflake.config.local_snowflake".to_string(),
        source_class: SourceClass::DatabaseCdc,
        source_checkpoint_start: CheckpointId::from(
            "snowflake:v1:snapshot:01b12345-0600-1234-0000-000000000000".to_string(),
        ),
        source_checkpoint_end: CheckpointId::from(
            "snowflake:v1:stream:01b12345-0601-1234-0000-000000000000".to_string(),
        ),
        ordering_field: "snowflake_ordinal".to_string(),
        ordering_min: 1,
        ordering_max: 1,
        schema_version: 1,
        schema_fingerprint: "customer-state-v1".to_string(),
        record_count: 1,
        op_counts: BTreeMap::from([(Operation::Insert, 1)]),
        file_set: vec![ManifestFile {
            file_uri: "file:///tmp/customer_state.parquet".to_string(),
            file_kind: "parquet".to_string(),
            content_hash: "content-hash-1".to_string(),
            file_size_bytes: 128,
            record_count: 1,
            created_at: created_at.clone(),
        }],
        content_hash: "batch-content-hash".to_string(),
        created_at,
    }
}

async fn durable_checkpoint_batch(
    store: &SqliteStateStore,
    batch_id: &BatchId,
    checkpoint_id: CheckpointId,
    snapshot_uri: &str,
) -> Result<()> {
    let attempt = store
        .begin_commit(
            batch_id.clone(),
            CommitRequest {
                destination_uri: "file:///tmp/warehouse".to_string(),
                snapshot: SnapshotRef {
                    uri: snapshot_uri.to_string(),
                },
                actor: "connector-check-test".to_string(),
            },
        )
        .await?;

    let snapshot = SnapshotRef {
        uri: snapshot_uri.to_string(),
    };

    store
        .resolve_commit(attempt.id, AttemptResolution::Committed)
        .await?;
    store
        .link_checkpoint_pending(
            batch_id.clone(),
            checkpoint_ref("snowflake.config.local_snowflake", checkpoint_id.clone()),
            snapshot.clone(),
        )
        .await?;
    store
        .mark_checkpoint_durable(
            batch_id.clone(),
            checkpoint_ack("snowflake.config.local_snowflake", checkpoint_id, snapshot),
        )
        .await
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
