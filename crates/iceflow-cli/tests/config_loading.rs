use anyhow::{Error, Result};
use iceflow_cli::config::{
    build_source_from_config, connector_table_id, load_catalog_config, load_connector_config,
    load_optional_catalog_config, load_source_config, resolve_catalog_name,
};
use iceflow_source::{
    BatchPoll, BatchRequest, CheckpointAck, OpenCaptureRequest, SourceTableSelection,
};
use iceflow_types::{TableId, TableMode};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

fn fixtures() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/config_samples"
    ))
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);
    let mut future = Pin::from(Box::new(future));
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("future unexpectedly pending"),
    }
}

fn write_temp_config(name: &str, contents: &str) -> Result<PathBuf> {
    let path = std::env::temp_dir().join(name);
    std::fs::write(&path, contents)
        .map_err(|err| Error::msg(format!("failed to write {}: {err}", path.display())))?;
    Ok(path)
}

fn remove_temp_config(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(Error::msg(format!(
            "failed to remove {}: {err}",
            path.display()
        ))),
    }
}

#[test]
fn load_source_config_parses_file_source() -> Result<()> {
    let config = load_source_config(&fixtures().join("sources/local_file.toml"))?;

    assert_eq!(config.kind, "file");
    assert_eq!(
        config.properties.get("fixture_root").map(String::as_str),
        Some("fixtures/reference_workload_v0")
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
fn load_catalog_config_parses_local_polaris() -> Result<()> {
    let config = load_catalog_config(&fixtures().join("catalogs/local_polaris.toml"))?;

    assert_eq!(config.kind, "polaris");
    assert_eq!(config.warehouse, "quickstart_catalog");
    Ok(())
}

#[test]
fn load_source_config_resolves_env_var_reference() -> Result<()> {
    let temp = write_temp_config(
        "test_source_env.toml",
        r#"
version = 1
kind = "postgres"

[properties]
host = "$TEST_PG_HOST"
port = "5432"
database = "testdb"
"#,
    )?;
    std::env::set_var("TEST_PG_HOST", "localhost");

    let config = load_source_config(&temp)?;

    assert_eq!(
        config.properties.get("host").map(String::as_str),
        Some("localhost")
    );

    std::env::remove_var("TEST_PG_HOST");
    remove_temp_config(&temp)?;
    Ok(())
}

#[test]
fn load_source_config_fails_on_missing_env_var() -> Result<()> {
    let temp = write_temp_config(
        "test_source_missing_env.toml",
        r#"
version = 1
kind = "postgres"

[properties]
password = "$MISSING_SECRET_VAR"
"#,
    )?;

    let err = load_source_config(&temp).expect_err("missing env var should fail");

    assert!(err.to_string().contains("MISSING_SECRET_VAR"));
    remove_temp_config(&temp)?;
    Ok(())
}

#[test]
fn resolve_catalog_name_uses_destination_catalog() -> Result<()> {
    let connector =
        load_connector_config(&fixtures().join("connectors/orders_append_polaris.toml"))?;
    let destination = iceflow_cli::config::load_destination_config(
        &fixtures().join("destinations/local_polaris.toml"),
    )?;

    let catalog_name = resolve_catalog_name(&connector, &destination)?;

    assert_eq!(catalog_name.as_deref(), Some("local_polaris"));
    Ok(())
}

#[test]
fn load_optional_catalog_config_loads_destination_catalog() -> Result<()> {
    let connector =
        load_connector_config(&fixtures().join("connectors/orders_append_polaris.toml"))?;
    let destination = iceflow_cli::config::load_destination_config(
        &fixtures().join("destinations/local_polaris.toml"),
    )?;

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
fn build_source_from_config_routes_file_tables_through_fixture_root() -> Result<()> {
    let source_path = fixtures().join("sources/local_file.toml");
    let source_config = load_source_config(&source_path)?;
    let connector = load_connector_config(&fixtures().join("connectors/orders_append.toml"))?;
    let table_entry = &connector.tables[0];
    let table_id = connector_table_id(table_entry);
    let source = build_source_from_config(
        &source_config,
        source_path.parent().unwrap_or_else(|| Path::new(".")),
    )?;
    let spec = block_on(source.spec())?;

    let mut session = block_on(source.open_capture(OpenCaptureRequest {
        table: SourceTableSelection {
            table_id: table_id.clone(),
            source_schema: table_entry.source_schema.clone(),
            source_table: table_entry.source_table.clone(),
            table_mode: TableMode::AppendOnly,
        },
        resume_from: None,
    }))?;

    let batch = match block_on(session.poll_batch(BatchRequest::default()))? {
        BatchPoll::Batch(batch) => batch,
        other => panic!("expected Batch, got {other:?}"),
    };
    assert_eq!(batch.batch_label.as_deref(), Some("batch-0001.jsonl"));
    assert_eq!(batch.records.len(), 2);

    block_on(session.checkpoint(CheckpointAck {
        source_id: spec.source_id,
        checkpoint: batch.checkpoint_end.clone(),
        snapshot_uri: "file:///tmp/test-snapshot".to_string(),
    }))?;
    block_on(session.close())?;

    assert_ne!(table_id, TableId::new(table_entry.source_table.clone()));
    Ok(())
}
