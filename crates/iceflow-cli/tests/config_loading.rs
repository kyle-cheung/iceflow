use anyhow::{Error, Result};
use iceflow_cli::config::{
    build_bound_source_from_config, build_sink_from_config, build_source_from_config,
    connector_table_id, load_catalog_config, load_connector_config, load_destination_config,
    load_optional_catalog_config, load_source_config, resolve_catalog_name, BoundSourceContext,
    CatalogConfig, ConfiguredSink, ConnectorConfig, SourceConfig,
};
use iceflow_source::{
    BatchPoll, BatchRequest, CheckpointAck, OpenCaptureRequest, SourceTableSelection,
};
use iceflow_types::{TableId, TableMode};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

fn fixtures() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/config_samples"
    ))
}

const SNOWFLAKE_SAMPLE_ENV_VARS: &[(&str, &str)] = &[
    ("SNOWFLAKE_ACCOUNT", "snowflake-account"),
    ("SNOWFLAKE_USER", "snowflake-user"),
    ("SNOWFLAKE_WAREHOUSE", "snowflake-warehouse"),
    ("SNOWFLAKE_ROLE", "snowflake-role"),
    ("SNOWFLAKE_DATABASE", "snowflake-database"),
];

#[must_use]
struct SnowflakeSampleEnvGuard {
    _lock: MutexGuard<'static, ()>,
    prior_values: Vec<(&'static str, Option<OsString>)>,
}

impl SnowflakeSampleEnvGuard {
    fn install() -> Self {
        let lock = sample_env_lock().lock().expect("env lock");
        let prior_values = SNOWFLAKE_SAMPLE_ENV_VARS
            .iter()
            .map(|(name, _)| (*name, std::env::var_os(name)))
            .collect();

        for (name, value) in SNOWFLAKE_SAMPLE_ENV_VARS {
            std::env::set_var(name, value);
        }

        Self {
            _lock: lock,
            prior_values,
        }
    }
}

impl Drop for SnowflakeSampleEnvGuard {
    fn drop(&mut self) {
        for (name, value) in std::mem::take(&mut self.prior_values) {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

// This lock is intentionally independent because this helper lives in a
// different test binary than the shared live-env helper; a shared
// test-support crate would be the right consolidation point later.
fn sample_env_lock() -> &'static Mutex<()> {
    static ENV_LOCK: Mutex<()> = Mutex::new(());
    &ENV_LOCK
}

fn next_temp_config_path(name: &str) -> PathBuf {
    static NEXT_TEMP_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    std::env::temp_dir().join(format!(
        "iceflow-cli-{name}-{}-{}.toml",
        std::process::id(),
        NEXT_TEMP_CONFIG_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

struct TempConfig {
    path: PathBuf,
}

impl TempConfig {
    fn new(name: &str, contents: &str) -> Result<Self> {
        let path = next_temp_config_path(name);
        std::fs::write(&path, contents)
            .map_err(|err| Error::msg(format!("failed to write {}: {err}", path.display())))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn fixture_root() -> PathBuf {
    fixtures()
        .parent()
        .expect("config_samples parent")
        .join("reference_workload_v0")
}

fn table_entry() -> Result<(ConnectorConfig, usize)> {
    let connector = load_connector_config(&fixtures().join("connectors/orders_append.toml"))?;
    Ok((connector, 0))
}

fn source_config_path() -> PathBuf {
    fixtures().join("sources/local_file.toml")
}

fn write_temp_config(name: &str, contents: &str) -> Result<TempConfig> {
    TempConfig::new(name, contents)
}

fn build_test_catalog(properties: BTreeMap<String, String>) -> CatalogConfig {
    CatalogConfig {
        version: 1,
        kind: "polaris".to_string(),
        endpoint: "http://127.0.0.1:8181/api/catalog".to_string(),
        warehouse: "quickstart_catalog".to_string(),
        properties,
    }
}

fn load_orders_append_destination() -> Result<iceflow_cli::config::DestinationConfig> {
    load_destination_config(&fixtures().join("destinations/local_fs.toml"))
}

fn load_orders_polaris_destination() -> Result<iceflow_cli::config::DestinationConfig> {
    load_destination_config(&fixtures().join("destinations/local_polaris.toml"))
}

#[test]
fn load_source_config_parses_file_source() -> Result<()> {
    let config = load_source_config(&fixtures().join("sources/local_file.toml"))?;

    assert_eq!(config.kind, "file");
    assert_eq!(
        config.properties.get("fixture_root").map(String::as_str),
        Some("../../reference_workload_v0")
    );
    Ok(())
}

#[test]
fn load_source_config_parses_local_snowflake_sample() -> Result<()> {
    let _env_guard = SnowflakeSampleEnvGuard::install();

    let config = load_source_config(&fixtures().join("sources/local_snowflake.toml"))?;

    assert_eq!(config.kind, "snowflake");
    assert_eq!(
        config.properties.get("account").map(String::as_str),
        Some("snowflake-account")
    );
    assert_eq!(
        config.properties.get("user").map(String::as_str),
        Some("snowflake-user")
    );
    assert_eq!(
        config.properties.get("warehouse").map(String::as_str),
        Some("snowflake-warehouse")
    );
    assert_eq!(
        config.properties.get("role").map(String::as_str),
        Some("snowflake-role")
    );
    assert_eq!(
        config.properties.get("database").map(String::as_str),
        Some("snowflake-database")
    );
    assert_eq!(
        config.properties.get("auth_method").map(String::as_str),
        Some("password")
    );

    Ok(())
}

#[test]
fn load_connector_config_parses_orders_append() -> Result<()> {
    let config = load_connector_config(&fixtures().join("connectors/orders_append.toml"))?;

    assert_eq!(config.source, "local_file");
    assert_eq!(config.destination, "local_fs");
    assert_eq!(config.tables.len(), 1);
    assert_eq!(config.tables[0].source_table, "orders_events");
    assert_eq!(config.tables[0].table_mode, "append_only");
    Ok(())
}

#[test]
fn load_connector_config_parses_snowflake_customer_state_append() -> Result<()> {
    let config =
        load_connector_config(&fixtures().join("connectors/snowflake_customer_state_append.toml"))?;

    assert_eq!(config.source, "local_snowflake");
    assert_eq!(config.destination, "local_fs");
    assert_eq!(config.tables.len(), 1);

    let table = &config.tables[0];
    assert_eq!(table.source_schema, "PUBLIC");
    assert_eq!(table.source_table, "CUSTOMER_STATE");
    assert_eq!(table.destination_namespace, "customer_state");
    assert_eq!(table.destination_table, "customer_state");
    assert_eq!(table.table_mode, "append_only");

    Ok(())
}

#[test]
fn load_catalog_config_parses_local_polaris() -> Result<()> {
    let config = load_catalog_config(&fixtures().join("catalogs/local_polaris.toml"))?;

    assert_eq!(config.kind, "polaris");
    assert_eq!(config.warehouse, "quickstart_catalog");
    Ok(())
}

#[test]
fn load_source_config_preserves_escaped_dollar_prefix() -> Result<()> {
    let temp = write_temp_config(
        "test_source_literal_dollar",
        r#"
version = 1
kind = "postgres"

[properties]
password = "$$literal"
"#,
    )?;

    let config = load_source_config(temp.path())?;

    assert_eq!(
        config.properties.get("password").map(String::as_str),
        Some("$literal")
    );
    Ok(())
}

#[test]
fn load_source_config_reports_parse_errors() -> Result<()> {
    let temp = write_temp_config(
        "test_source_parse_error",
        r#"
version = 1
kind = "postgres

[properties]
password = "secret"
"#,
    )?;

    let err = load_source_config(temp.path()).expect_err("invalid TOML should fail");

    assert!(err.to_string().contains("failed to parse source config"));
    Ok(())
}

#[test]
fn resolve_catalog_name_uses_destination_catalog() -> Result<()> {
    let connector =
        load_connector_config(&fixtures().join("connectors/orders_append_polaris.toml"))?;
    let destination = load_orders_polaris_destination()?;

    let catalog_name = resolve_catalog_name(&connector, &destination)?;

    assert_eq!(catalog_name.as_deref(), Some("local_polaris"));
    Ok(())
}

#[test]
fn resolve_catalog_name_rejects_conflicting_catalogs() -> Result<()> {
    let mut connector =
        load_connector_config(&fixtures().join("connectors/orders_append_polaris.toml"))?;
    let destination = load_orders_polaris_destination()?;
    connector.catalog = Some("other_catalog".to_string());

    let err = resolve_catalog_name(&connector, &destination).expect_err("conflicting catalogs");

    assert!(err.to_string().contains("does not match"));
    Ok(())
}

#[test]
fn load_optional_catalog_config_loads_destination_catalog() -> Result<()> {
    let connector =
        load_connector_config(&fixtures().join("connectors/orders_append_polaris.toml"))?;
    let destination = load_orders_polaris_destination()?;

    let catalog = load_optional_catalog_config(fixtures(), &connector, &destination)?
        .expect("catalog config should load");

    assert_eq!(catalog.kind, "polaris");
    assert_eq!(catalog.warehouse, "quickstart_catalog");
    Ok(())
}

#[test]
fn connector_table_id_uses_destination_namespace_and_table() -> Result<()> {
    let connector = load_connector_config(&fixtures().join("connectors/orders_append.toml"))?;

    let table_id = connector_table_id(&connector.tables[0]);

    assert_eq!(table_id.as_str(), "orders_events.orders_events");
    Ok(())
}

#[test]
fn build_sink_from_config_builds_filesystem_sink() -> Result<()> {
    let destination = load_orders_append_destination()?;
    let (connector, index) = table_entry()?;
    let table_entry = &connector.tables[index];
    let table_id = connector_table_id(table_entry);

    let sink = build_sink_from_config(&destination, None, table_entry, &table_id)?;

    assert!(matches!(sink, ConfiguredSink::Filesystem { .. }));
    assert_eq!(sink.destination_uri(), "file:///tmp/iceflow-output");
    Ok(())
}

#[test]
fn build_sink_from_config_builds_polaris_sink() -> Result<()> {
    let destination = load_orders_polaris_destination()?;
    let catalog = load_catalog_config(&fixtures().join("catalogs/local_polaris.toml"))?;
    let (connector, index) = table_entry()?;
    let table_entry = &connector.tables[index];
    let table_id = connector_table_id(table_entry);

    let sink = build_sink_from_config(&destination, Some(&catalog), table_entry, &table_id)?;

    assert!(matches!(sink, ConfiguredSink::Polaris { .. }));
    assert_eq!(sink.destination_uri(), "file:///tmp/iceflow-output");
    Ok(())
}

#[test]
fn build_sink_from_config_rejects_partial_polaris_credentials() -> Result<()> {
    let destination = load_orders_polaris_destination()?;
    let catalog = build_test_catalog(BTreeMap::from([(
        "client_id".to_string(),
        "client-id".to_string(),
    )]));
    let (connector, index) = table_entry()?;
    let table_entry = &connector.tables[index];
    let table_id = connector_table_id(table_entry);

    let err = build_sink_from_config(&destination, Some(&catalog), table_entry, &table_id)
        .expect_err("partial credentials should fail");

    assert!(err
        .to_string()
        .contains("must set both client_id and client_secret"));
    Ok(())
}

#[test]
fn build_sink_from_config_accepts_bearer_token_catalog_auth() -> Result<()> {
    let destination = load_orders_polaris_destination()?;
    let catalog = build_test_catalog(BTreeMap::from([(
        "bearer_token".to_string(),
        "token".to_string(),
    )]));
    let (connector, index) = table_entry()?;
    let table_entry = &connector.tables[index];
    let table_id = connector_table_id(table_entry);

    let sink = build_sink_from_config(&destination, Some(&catalog), table_entry, &table_id)?;

    assert!(matches!(sink, ConfiguredSink::Polaris { .. }));
    Ok(())
}

#[test]
fn build_source_from_config_routes_file_tables_through_fixture_root() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime")
        .block_on(async {
            let source_path = source_config_path();
            let source_config = load_source_config(&source_path)?;
            let connector =
                load_connector_config(&fixtures().join("connectors/orders_append.toml"))?;
            let table_entry = &connector.tables[0];
            let table_id = connector_table_id(table_entry);
            let source = build_source_from_config(
                &source_config,
                source_path.parent().unwrap_or_else(|| Path::new(".")),
            )?;
            let spec = source.spec().await?;

            let mut session = source
                .open_capture(OpenCaptureRequest {
                    table: SourceTableSelection {
                        table_id: table_id.clone(),
                        source_schema: table_entry.source_schema.clone(),
                        source_table: table_entry.source_table.clone(),
                        table_mode: TableMode::AppendOnly,
                    },
                    resume_from: None,
                })
                .await?;

            let batch = match session.poll_batch(BatchRequest::default()).await? {
                BatchPoll::Batch(batch) => batch,
                other => panic!("expected Batch, got {other:?}"),
            };
            assert_eq!(batch.batch_label.as_deref(), Some("batch-0001.jsonl"));
            assert_eq!(batch.records.len(), 2);

            session
                .checkpoint(CheckpointAck {
                    source_id: spec.source_id,
                    checkpoint: batch.checkpoint_end.clone(),
                    snapshot_uri: "file:///tmp/test-snapshot".to_string(),
                })
                .await?;
            session.close().await?;

            assert_ne!(table_id, TableId::new(table_entry.source_table.clone()));
            Ok(())
        })
}

#[test]
fn build_bound_source_from_config_routes_file_tables_through_fixture_root() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime")
        .block_on(async {
            let source_path = source_config_path();
            let source_config = load_source_config(&source_path)?;
            let connector =
                load_connector_config(&fixtures().join("connectors/orders_append.toml"))?;
            let table_entry = &connector.tables[0];
            let table_id = connector_table_id(table_entry);
            let source = build_bound_source_from_config(
                &source_config,
                source_path.parent().unwrap_or_else(|| Path::new(".")),
                &BoundSourceContext {
                    connector_name: "orders_append".to_string(),
                    connector: connector.clone(),
                    durable_checkpoint: None,
                },
            )?;

            let mut session = source
                .open_capture(OpenCaptureRequest {
                    table: SourceTableSelection {
                        table_id: table_id.clone(),
                        source_schema: table_entry.source_schema.clone(),
                        source_table: table_entry.source_table.clone(),
                        table_mode: TableMode::AppendOnly,
                    },
                    resume_from: None,
                })
                .await?;

            let batch = match session.poll_batch(BatchRequest::default()).await? {
                BatchPoll::Batch(batch) => batch,
                other => panic!("expected Batch, got {other:?}"),
            };

            assert_eq!(batch.batch_label.as_deref(), Some("batch-0001.jsonl"));
            assert_eq!(batch.records.len(), 2);
            session.close().await?;
            Ok(())
        })
}

#[test]
fn build_bound_source_from_config_routes_snowflake_to_binding_validation() {
    let source_config = SourceConfig {
        version: 1,
        kind: "snowflake".to_string(),
        properties: BTreeMap::from([
            ("account".to_string(), "xy12345.us-east-1".to_string()),
            ("user".to_string(), "ICEFLOW_DEMO".to_string()),
            ("password".to_string(), "secret".to_string()),
            ("warehouse".to_string(), "ICEFLOW_WH".to_string()),
            ("role".to_string(), "ICEFLOW_ROLE".to_string()),
            ("database".to_string(), "SOURCE_DB".to_string()),
            ("auth_method".to_string(), "password".to_string()),
        ]),
    };
    let connector = ConnectorConfig {
        version: 1,
        source: "local_snowflake".to_string(),
        destination: "local_fs".to_string(),
        catalog: None,
        capture: Default::default(),
        tables: Vec::new(),
    };

    let result = build_bound_source_from_config(
        &source_config,
        Path::new("/tmp/config/sources"),
        &BoundSourceContext {
            connector_name: "snowflake_customer_state_append".to_string(),
            connector,
            durable_checkpoint: None,
        },
    );
    let err = match result {
        Ok(_) => panic!("snowflake source should reject invalid binding"),
        Err(err) => err,
    };

    assert!(err.to_string().contains("exactly one selected table"));
}

#[test]
fn build_source_from_config_accepts_absolute_fixture_root() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime")
        .block_on(async {
            let absolute_fixture_root = fixture_root();
            let expected_fixture_root = absolute_fixture_root.display().to_string();
            let source_config = SourceConfig {
                version: 1,
                kind: "file".to_string(),
                properties: BTreeMap::from([(
                    "fixture_root".to_string(),
                    expected_fixture_root.clone(),
                )]),
            };

            let source = build_source_from_config(&source_config, Path::new("/tmp/unused"))?;
            let report = source.check().await?;

            assert_eq!(
                report.details.get("fixture_root").map(String::as_str),
                Some(expected_fixture_root.as_str())
            );
            Ok(())
        })
}
