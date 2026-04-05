use anyhow::Result;

#[test]
fn source_check_reports_file_source_capabilities() -> Result<()> {
    use iceflow_source::SourceCapability;

    let report = iceflow_cli::commands::source_cmd::execute_blocking(
        iceflow_cli::commands::source_cmd::Args {
            source_config: fixtures().join("sources/local_file.toml"),
        },
    )?;

    assert!(report
        .capabilities
        .contains(&SourceCapability::InitialSnapshot));
    assert!(report.capabilities.contains(&SourceCapability::Resume));
    assert!(!report.capabilities.contains(&SourceCapability::ChangeFeed));
    assert!(report.warnings.is_empty());
    Ok(())
}

fn fixtures() -> std::path::PathBuf {
    std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/config_samples"
    ))
    .to_path_buf()
}
