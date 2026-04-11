mod support;

use anyhow::Result;
use iceflow_cli::commands::connector_cmd::{self, CheckArgs, RunArgs};

#[test]
#[ignore = "requires live Snowflake credentials"]
fn connector_run_bootstraps_and_resumes_snowflake_source() -> Result<()> {
    let harness = support::snowflake_live::LiveSnowflakeHarness::new()?;
    harness.reset_table()?;

    let report = connector_cmd::check_blocking(CheckArgs {
        connector_config: harness.connector_config(),
        config_root: harness.config_root(),
    })?;
    assert!(report.valid);

    let first = connector_cmd::run_blocking(RunArgs {
        connector_config: harness.connector_config(),
        config_root: harness.config_root(),
        batch_limit: Some(1),
    })?;
    assert_eq!(first.total_committed_batches, 1);

    harness.insert_customer("customer-003", "trial")?;

    let second = connector_cmd::run_blocking(RunArgs {
        connector_config: harness.connector_config(),
        config_root: harness.config_root(),
        batch_limit: Some(1),
    })?;
    assert_eq!(second.total_committed_batches, 1);
    Ok(())
}
