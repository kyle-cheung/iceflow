use anyhow::{Error, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

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
