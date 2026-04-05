use anyhow::Result;

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

fn fixtures() -> std::path::PathBuf {
    std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/config_samples"
    ))
    .to_path_buf()
}
