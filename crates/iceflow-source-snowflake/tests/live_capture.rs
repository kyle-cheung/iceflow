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
use iceflow_types::{IceflowJsonValue, LogicalMutation, TableId, TableMode};
use std::sync::{Mutex, MutexGuard};
use support::live_env::{
    apply_live_auth_env_overrides, load_repo_dotenv, optional_env, quote_string_literal,
    required_env, LiveAuthEnvGuard,
};
use tokio::runtime::{Builder, Runtime};

const LIVE_CAPTURE_TABLE: &str = "ICEFLOW_TEST_SOURCE_CUSTOMER_STATE";

#[test]
#[ignore = "requires live Snowflake credentials"]
fn live_capture_reads_snapshot_then_change_batches() -> Result<()> {
    let _guard = live_capture_lock();
    let harness = LiveSnowflakeHarness::new()?;
    harness.reset_customer_state()?;

    let mut session = harness.open_bound_session(None)?;
    let snapshot = harness.expect_batch(&mut session)?;
    assert!(!snapshot.records.is_empty());

    harness
        .runtime()
        .block_on(session.checkpoint(harness.ack_for(&snapshot)))?;

    harness.insert_customer("customer-003", "trial")?;
    let changes = harness.expect_batch(&mut session)?;
    assert!(changes.records.iter().any(|record| record.after.is_some()));
    Ok(())
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn live_capture_resumes_from_snapshot_checkpoint() -> Result<()> {
    let _guard = live_capture_lock();
    let harness = LiveSnowflakeHarness::new()?;
    harness.reset_customer_state()?;

    let mut session = harness.open_bound_session(None)?;
    let snapshot = harness.expect_batch(&mut session)?;
    assert!(!snapshot.records.is_empty());

    harness
        .runtime()
        .block_on(session.checkpoint(harness.ack_for(&snapshot)))?;

    drop(session);
    harness.insert_customer("customer-003", "trial")?;

    let mut resumed = harness.open_bound_session(Some(snapshot.checkpoint_end.clone()))?;
    let resumed_batch = harness.expect_batch(&mut resumed)?;
    assert_eq!(
        resumed_batch.checkpoint_start.as_ref(),
        Some(&snapshot.checkpoint_end),
        "resumed snapshot batch did not start from the snapshot checkpoint"
    );
    assert_batch_ids(&resumed_batch, &["customer-003"]);
    Ok(())
}

#[test]
#[ignore = "requires live Snowflake credentials"]
fn live_capture_resumes_from_stream_checkpoint() -> Result<()> {
    let _guard = live_capture_lock();
    let harness = LiveSnowflakeHarness::new()?;
    harness.reset_customer_state()?;

    let mut session = harness.open_bound_session(None)?;
    let snapshot = harness.expect_batch(&mut session)?;
    assert!(!snapshot.records.is_empty());

    harness
        .runtime()
        .block_on(session.checkpoint(harness.ack_for(&snapshot)))?;

    harness.insert_customer("customer-003", "trial")?;
    let stream_batch = harness.expect_batch(&mut session)?;
    assert_batch_ids(&stream_batch, &["customer-003"]);
    let stream_checkpoint = stream_batch.checkpoint_end.clone();

    harness
        .runtime()
        .block_on(session.checkpoint(harness.ack_for(&stream_batch)))?;

    drop(session);
    harness.insert_customer("customer-004", "trial")?;

    let mut resumed = harness.open_bound_session(Some(stream_checkpoint.clone()))?;
    let resumed_batch = harness.expect_batch(&mut resumed)?;
    assert_eq!(
        resumed_batch.checkpoint_start.as_ref(),
        Some(&stream_checkpoint),
        "resumed stream batch did not start from the stream checkpoint"
    );
    assert_batch_ids(&resumed_batch, &["customer-004"]);
    let runtime = harness.runtime();
    runtime.block_on(resumed.checkpoint(harness.ack_for(&resumed_batch)))?;
    assert_no_more_batches(&mut resumed, runtime)?;
    Ok(())
}

struct LiveSnowflakeHarness {
    _auth_env: LiveAuthEnvGuard,
    config: SnowflakeSourceConfig,
    schema: String,
    runtime: Runtime,
}

impl LiveSnowflakeHarness {
    fn new() -> Result<Self> {
        load_repo_dotenv()?;
        let auth_env = apply_live_auth_env_overrides();

        let runtime = Builder::new_current_thread().build().expect("runtime");

        Ok(Self {
            _auth_env: auth_env,
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
            runtime,
        })
    }

    fn runtime(&self) -> &Runtime {
        &self.runtime
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

        // `?` converts `iceflow_source`'s error type into this test crate's `anyhow::Error`.
        Ok(self
            .runtime
            .block_on(source.open_capture(OpenCaptureRequest {
                table: SourceTableSelection {
                    table_id: TableId::from("customer_state.customer_state"),
                    source_schema: binding.source_schema.clone(),
                    source_table: binding.source_table.clone(),
                    table_mode: TableMode::AppendOnly,
                },
                resume_from,
            }))?)
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
        for _ in 0..20 {
            match self
                .runtime
                .block_on(session.poll_batch(BatchRequest::default()))?
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

fn collect_customer_ids(batch: &SourceBatch) -> Vec<String> {
    batch
        .records
        .iter()
        .filter_map(record_customer_id)
        .collect()
}

fn record_customer_id(record: &LogicalMutation) -> Option<String> {
    record
        .key
        .parts
        .iter()
        .find(|part| part.name.eq_ignore_ascii_case("customer_id"))
        .and_then(|part| match &part.value {
            IceflowJsonValue::String(value) => Some(value.clone()),
            _ => None,
        })
}

fn assert_batch_ids(batch: &SourceBatch, expected_ids: &[&str]) {
    let mut ids = collect_customer_ids(batch);
    assert_eq!(
        ids.len(),
        batch.records.len(),
        "some records were missing customer_id keys"
    );
    ids.sort();
    let mut expected: Vec<String> = expected_ids.iter().map(|id| id.to_string()).collect();
    expected.sort();
    assert_eq!(ids, expected, "batch customer IDs do not match expected")
}

fn assert_no_more_batches(
    session: &mut Box<dyn SourceCaptureSession + Send>,
    runtime: &Runtime,
) -> Result<()> {
    // This helper polls a bounded number of times because the live session is non-async
    // and we need to ensure there is no delayed replay without hanging forever.
    for _ in 0..20 {
        match runtime.block_on(session.poll_batch(BatchRequest::default()))? {
            BatchPoll::Idle => std::thread::sleep(std::time::Duration::from_millis(250)),
            BatchPoll::Exhausted => return Ok(()),
            BatchPoll::Batch(batch) => {
                let ids = collect_customer_ids(&batch).join(",");
                return Err(Error::msg(format!(
                    "unexpected batch after resumed checkpoint (checkpoint={} ids=[{}])",
                    batch.checkpoint_end, ids,
                )));
            }
        }
    }

    // Snowflake incremental sessions stay open and report `Idle` when they are caught up rather
    // than naturally transitioning to `Exhausted`. Clearing this bounded idle window (~5s across
    // 20 polls) without another batch is the strongest no-replay signal this live test can assert
    // without hanging.
    Ok(())
}

fn live_capture_lock() -> MutexGuard<'static, ()> {
    static LIVE_CAPTURE_LOCK: Mutex<()> = Mutex::new(());
    // This mutex only serializes the live capture tests within this binary.
    LIVE_CAPTURE_LOCK
        .lock()
        .expect("failed to lock live capture mutex")
}
