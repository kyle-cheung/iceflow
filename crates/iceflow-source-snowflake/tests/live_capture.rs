mod support;

use anyhow::{Error, Result};
use iceflow_source::{
    BatchPoll, BatchRequest, CheckpointAck, OpenCaptureRequest, SourceAdapter, SourceBatch,
    SourceCaptureSession, SourceTableSelection,
};
use iceflow_source_snowflake::{
    client::{qualified_table_name, AdbcSnowflakeClient},
    SnowflakeAuthMethod, SnowflakeBindingRequest, SnowflakeConnectorBinding, SnowflakeSource,
    SnowflakeSourceConfig, SnowflakeTableBinding,
};
use iceflow_types::{TableId, TableMode};
use support::live_env::{
    apply_live_auth_env_overrides, load_repo_dotenv, optional_env, quote_string_literal,
    required_env,
};
use tokio::runtime::Builder;

const LIVE_CAPTURE_TABLE: &str = "ICEFLOW_TEST_SOURCE_CUSTOMER_STATE";

#[test]
#[ignore = "requires live Snowflake credentials"]
fn live_capture_reads_snapshot_then_change_batches() -> Result<()> {
    let harness = LiveSnowflakeHarness::new()?;
    harness.reset_customer_state()?;

    let mut session = harness.open_bound_session(None)?;
    let snapshot = harness.expect_batch(&mut session)?;
    assert!(!snapshot.records.is_empty());

    Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(session.checkpoint(harness.ack_for(&snapshot)))?;

    harness.insert_customer("customer-003", "trial")?;
    let changes = harness.expect_batch(&mut session)?;
    assert!(changes.records.iter().any(|record| record.after.is_some()));
    Ok(())
}

struct LiveSnowflakeHarness {
    config: SnowflakeSourceConfig,
    schema: String,
}

impl LiveSnowflakeHarness {
    fn new() -> Result<Self> {
        load_repo_dotenv()?;
        apply_live_auth_env_overrides();

        Ok(Self {
            config: SnowflakeSourceConfig {
                source_label: "live".to_string(),
                account: required_env("SNOWFLAKE_ACCOUNT")?,
                user: required_env("SNOWFLAKE_USER")?,
                password: optional_env("SNOWFLAKE_PASSWORD").unwrap_or_default(),
                warehouse: required_env("SNOWFLAKE_WAREHOUSE")?,
                role: required_env("SNOWFLAKE_ROLE")?,
                database: required_env("SNOWFLAKE_DATABASE")?,
                auth_method: SnowflakeAuthMethod::Password,
            },
            schema: optional_env("SNOWFLAKE_SCHEMA").unwrap_or_else(|| "PUBLIC".to_string()),
        })
    }

    fn client(&self) -> Result<AdbcSnowflakeClient> {
        AdbcSnowflakeClient::connect(self.config.clone())
    }

    fn table_name(&self) -> String {
        qualified_table_name(&self.schema, LIVE_CAPTURE_TABLE)
    }

    fn reset_customer_state(&self) -> Result<()> {
        let client = self.client()?;
        let table = self.table_name();
        client.exec(&format!(
            "CREATE OR REPLACE TABLE {table} (customer_id STRING NOT NULL, status STRING, updated_at TIMESTAMP_NTZ, CONSTRAINT CUSTOMER_STATE_PK PRIMARY KEY (customer_id))",
        ))?;
        client.exec(&format!("ALTER TABLE {table} SET CHANGE_TRACKING = TRUE"))?;
        client.exec(&format!(
            "INSERT INTO {table} VALUES ('customer-001', 'active', CURRENT_TIMESTAMP()), ('customer-002', 'trial', CURRENT_TIMESTAMP())",
        ))?;
        Ok(())
    }

    fn insert_customer(&self, customer_id: &str, status: &str) -> Result<()> {
        let client = self.client()?;
        client.exec(&format!(
            "INSERT INTO {} VALUES ('{}', '{}', CURRENT_TIMESTAMP())",
            self.table_name(),
            quote_string_literal(customer_id),
            quote_string_literal(status),
        ))?;
        Ok(())
    }

    fn open_bound_session(
        &self,
        resume_from: Option<iceflow_types::CheckpointId>,
    ) -> Result<Box<dyn SourceCaptureSession + Send>> {
        let binding = self.binding(resume_from.clone())?;
        let source = SnowflakeSource::new(
            self.config.clone(),
            Some(binding.clone()),
            Box::new(self.client()?),
        );

        Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(source.open_capture(OpenCaptureRequest {
                table: SourceTableSelection {
                    table_id: TableId::from("customer_state.customer_state"),
                    source_schema: binding.source_schema.clone(),
                    source_table: binding.source_table.clone(),
                    table_mode: TableMode::AppendOnly,
                },
                resume_from,
            }))
            .map_err(|err| Error::msg(err.to_string()))
    }

    fn binding(
        &self,
        durable_checkpoint: Option<iceflow_types::CheckpointId>,
    ) -> Result<SnowflakeConnectorBinding> {
        SnowflakeConnectorBinding::from_request(SnowflakeBindingRequest {
            connector_name: "snowflake_customer_state_append".to_string(),
            tables: vec![SnowflakeTableBinding {
                source_schema: self.schema.clone(),
                source_table: LIVE_CAPTURE_TABLE.to_string(),
                destination_namespace: "customer_state".to_string(),
                destination_table: "customer_state".to_string(),
                table_mode: "append_only".to_string(),
            }],
            durable_checkpoint,
        })
    }

    fn expect_batch(
        &self,
        session: &mut Box<dyn SourceCaptureSession + Send>,
    ) -> Result<SourceBatch> {
        let runtime = Builder::new_current_thread().build().expect("runtime");

        for _ in 0..20 {
            match runtime
                .block_on(session.poll_batch(BatchRequest::default()))
                .map_err(|err| Error::msg(err.to_string()))?
            {
                BatchPoll::Batch(batch) => return Ok(batch),
                BatchPoll::Idle => std::thread::sleep(std::time::Duration::from_millis(250)),
                BatchPoll::Exhausted => {
                    return Err(Error::msg(
                        "Snowflake session exhausted before emitting a batch",
                    ))
                }
            }
        }

        Err(Error::msg(
            "Snowflake session stayed idle without emitting a batch",
        ))
    }

    fn ack_for(&self, batch: &SourceBatch) -> CheckpointAck {
        CheckpointAck {
            source_id: "snowflake.config.live".to_string(),
            checkpoint: batch.checkpoint_end.clone(),
            snapshot_uri: "file:///tmp/live-snowflake".to_string(),
        }
    }
}
