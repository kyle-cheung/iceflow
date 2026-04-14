use anyhow::Result;
use iceflow_source::SourceCapability;

#[path = "../../iceflow-source-snowflake/tests/support/live_env.rs"]
mod live_env;
use live_env::{apply_live_auth_env_overrides, load_repo_dotenv};

#[test]
fn source_check_reports_file_source_capabilities() -> Result<()> {
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

#[test]
#[ignore = "requires live Snowflake credentials"]
fn source_check_reports_snowflake_capabilities() -> Result<()> {
    load_repo_dotenv()?;
    let _auth_env = apply_live_auth_env_overrides();

    let report = iceflow_cli::commands::source_cmd::execute_blocking(
        iceflow_cli::commands::source_cmd::Args {
            source_config: fixtures().join("sources/local_snowflake.toml"),
        },
    )?;

    let expected_capabilities = [
        SourceCapability::AppendOnly,
        SourceCapability::InitialSnapshot,
        SourceCapability::ChangeFeed,
        SourceCapability::SnapshotHandoff,
        SourceCapability::Deletes,
        SourceCapability::Resume,
        SourceCapability::DeterministicCheckpoints,
    ];

    for capability in expected_capabilities {
        assert!(report.capabilities.contains(&capability));
    }
    assert!(!report.capabilities.contains(&SourceCapability::KeyedUpsert));

    Ok(())
}

fn fixtures() -> std::path::PathBuf {
    std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/config_samples"
    ))
    .to_path_buf()
}
