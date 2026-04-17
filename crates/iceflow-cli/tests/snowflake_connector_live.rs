mod support;

use anyhow::{Error, Result};
use iceflow_cli::commands::connector_cmd::{self, CheckArgs, RunArgs};
use iceflow_state::{SqliteStateStore, StateStore};
use iceflow_types::TableId;
use std::path::{Path, PathBuf};
use tokio::runtime::{Builder, Runtime};

#[test]
#[ignore = "requires live Snowflake credentials"]
fn connector_run_bootstraps_and_resumes_snowflake_source() -> Result<()> {
    let harness = support::snowflake_live::LiveSnowflakeHarness::new()?;
    harness.reset_table()?;

    let connector_config = harness.connector_config();
    let config_root = harness.config_root();
    let report = connector_cmd::check_blocking(CheckArgs {
        connector_config: connector_config.clone(),
        config_root: config_root.clone(),
    })?;
    assert!(report.valid);

    let first = connector_cmd::run_blocking(RunArgs {
        connector_config: connector_config.clone(),
        config_root: config_root.clone(),
        batch_limit: Some(1),
    })?;
    assert_eq!(first.total_committed_batches, 1);

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Error::msg(format!("failed to build Tokio runtime: {err}")))?;
    let state_path = state_db_path(&config_root, &connector_config)?;
    let first_checkpoint = require_checkpoint(
        durable_checkpoint_token(&runtime, &state_path)?,
        &state_path,
        "first",
    )?;
    assert!(
        first_checkpoint.starts_with(SNAPSHOT_TOKEN_PREFIX),
        "expected Snowflake snapshot token (prefix {SNAPSHOT_TOKEN_PREFIX}) but got {first_checkpoint} from {state_path:?}"
    );

    harness.insert_customer("customer-003", "trial")?;

    let second = connector_cmd::run_blocking(RunArgs {
        connector_config: connector_config.clone(),
        config_root: config_root.clone(),
        batch_limit: Some(1),
    })?;
    assert_eq!(second.total_committed_batches, 1);

    let second_checkpoint = require_checkpoint(
        durable_checkpoint_token(&runtime, &state_path)?,
        &state_path,
        "second",
    )?;
    assert!(
        second_checkpoint.starts_with(STREAM_TOKEN_PREFIX),
        "expected Snowflake stream token (prefix {STREAM_TOKEN_PREFIX}) but got {second_checkpoint} from {state_path:?}"
    );
    Ok(())
}

const CUSTOMER_STATE_TABLE_ID: &str = "customer_state.customer_state";
const SNAPSHOT_TOKEN_PREFIX: &str = "snowflake:v1:snapshot:";
const STREAM_TOKEN_PREFIX: &str = "snowflake:v1:stream:";

fn state_db_path(config_root: &Path, connector_config: &Path) -> Result<PathBuf> {
    let stem = connector_config
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| {
            Error::msg(format!(
                "connector config path {} has invalid or missing file stem",
                connector_config.display()
            ))
        })?;
    Ok(config_root
        .join(".iceflow")
        .join("state")
        .join(format!("{stem}.sqlite3")))
}

fn require_checkpoint(
    checkpoint: Option<String>,
    state_path: &Path,
    stage: &str,
) -> Result<String> {
    checkpoint.ok_or_else(|| {
        Error::msg(format!(
            "no durable checkpoint recorded in {} after {stage} run",
            state_path.display()
        ))
    })
}

fn durable_checkpoint_token(runtime: &Runtime, state_path: &Path) -> Result<Option<String>> {
    let state_path = state_path.to_path_buf();
    let table_id = TableId::from(CUSTOMER_STATE_TABLE_ID);
    let checkpoint = runtime.block_on(async move {
        let store = SqliteStateStore::open_persistent(&state_path).await?;
        store.last_durable_checkpoint_for_table(&table_id).await
    })?;
    Ok(checkpoint.map(|checkpoint| checkpoint.checkpoint.to_string()))
}
