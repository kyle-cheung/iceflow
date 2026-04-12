use crate::binding::SnowflakeConnectorBinding;
use crate::checkpoint::{decode_checkpoint, encode_checkpoint, Checkpoint};
use crate::client::SnowflakeClient;
#[cfg(test)]
use crate::client::{RowSet, StatementOutcome};
use crate::metadata::TableMetadata;
use anyhow::{Error, Result};
use async_trait::async_trait;
use iceflow_source::{
    BatchPoll, BatchRequest, CheckpointAck, SourceBatch, SourceCaptureSession, SourceResult,
};
#[cfg(test)]
use iceflow_types::CheckpointId;
use iceflow_types::{TableId, TableMode};
use std::sync::Arc;

pub(crate) struct SnowflakeCaptureSession {
    source_id: String,
    table_id: TableId,
    table_mode: TableMode,
    binding: SnowflakeConnectorBinding,
    client: Arc<dyn SnowflakeClient + Send + Sync>,
    metadata: TableMetadata,
    phase: SessionPhase,
    pending: Option<SourceBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SessionPhase {
    Snapshot,
    Incremental { last_durable_boundary: Checkpoint },
}

impl SnowflakeCaptureSession {
    pub(crate) fn new_with_snapshot(
        source_id: String,
        table_id: TableId,
        table_mode: TableMode,
        binding: SnowflakeConnectorBinding,
        client: Arc<dyn SnowflakeClient + Send + Sync>,
        metadata: TableMetadata,
        batch: SourceBatch,
    ) -> Self {
        Self {
            source_id,
            table_id,
            table_mode,
            binding,
            client,
            metadata,
            phase: SessionPhase::Snapshot,
            pending: Some(batch),
        }
    }

    pub(crate) fn new_incremental(
        source_id: String,
        table_id: TableId,
        table_mode: TableMode,
        binding: SnowflakeConnectorBinding,
        client: Arc<dyn SnowflakeClient + Send + Sync>,
        metadata: TableMetadata,
        last_durable_boundary: Checkpoint,
    ) -> Self {
        Self {
            source_id,
            table_id,
            table_mode,
            binding,
            client,
            metadata,
            phase: SessionPhase::Incremental {
                last_durable_boundary,
            },
            pending: None,
        }
    }

    #[cfg(test)]
    fn new_from_snapshot(records: Vec<iceflow_types::LogicalMutation>) -> Self {
        let source_id = "snowflake.config.local_snowflake".to_string();
        let checkpoint = encode_checkpoint(Checkpoint::Snapshot {
            anchor_query_id: "01b12345-0600-1234-0000-000000000000".to_string(),
        });
        let batch = SourceBatch {
            batch_label: Some("snowflake-snapshot".to_string()),
            checkpoint_start: None,
            checkpoint_end: checkpoint,
            records,
        };

        Self::new_with_snapshot(
            source_id,
            TableId::from("customer_state.customer_state"),
            TableMode::AppendOnly,
            test_binding(None),
            Arc::new(TestSnowflakeClient),
            test_metadata(),
            batch,
        )
    }

    #[cfg(test)]
    fn new_for_resume_test(checkpoint: CheckpointId) -> Self {
        let decoded = decode_checkpoint(&checkpoint).expect("checkpoint");
        Self::new_incremental(
            "snowflake.config.local_snowflake".to_string(),
            TableId::from("customer_state.customer_state"),
            TableMode::AppendOnly,
            test_binding(Some(checkpoint)),
            Arc::new(TestSnowflakeClient),
            test_metadata(),
            decoded,
        )
    }

    fn poll_incremental(&mut self, last_durable_boundary: Checkpoint) -> Result<BatchPoll> {
        let actual_schema_fingerprint =
            crate::metadata::load_schema_fingerprint(self.client.as_ref(), &self.binding)?;
        crate::value::verify_schema_fingerprint(
            &self.metadata.schema_fingerprint,
            &actual_schema_fingerprint,
        )?;

        let boundary = self.client.exec("SELECT CURRENT_TIMESTAMP()")?;
        let checkpoint_end = encode_checkpoint(Checkpoint::Stream {
            boundary_query_id: boundary.query_id.clone(),
        });
        let rows = self.client.query_rows(&changes_query_from_checkpoint(
            &self.binding,
            &self.metadata,
            &last_durable_boundary,
            &boundary.query_id,
        ))?;
        let records = crate::value::collapse_change_rows(
            crate::value::MutationContext::new(
                self.table_id.clone(),
                self.source_id.clone(),
                self.table_mode,
                &self.metadata,
                checkpoint_end.clone(),
                rows.query_id.clone(),
            ),
            crate::value::parse_change_rows(rows)?,
        )?;

        if records.is_empty() {
            return Ok(BatchPoll::Idle);
        }

        let batch = SourceBatch {
            batch_label: Some(format!("snowflake-change-{}", boundary.query_id)),
            checkpoint_start: Some(encode_checkpoint(last_durable_boundary)),
            checkpoint_end,
            records,
        };
        self.pending = Some(batch.clone());
        Ok(BatchPoll::Batch(batch))
    }
}

#[async_trait]
impl SourceCaptureSession for SnowflakeCaptureSession {
    async fn poll_batch(&mut self, _req: BatchRequest) -> SourceResult<BatchPoll> {
        if let Some(batch) = self.pending.as_ref() {
            return Ok(BatchPoll::Batch(batch.clone()));
        }

        match &self.phase {
            SessionPhase::Snapshot => Ok(BatchPoll::Idle),
            SessionPhase::Incremental {
                last_durable_boundary,
            } => self
                .poll_incremental(last_durable_boundary.clone())
                .map_err(crate::source_error),
        }
    }

    async fn checkpoint(&mut self, ack: CheckpointAck) -> SourceResult<()> {
        self.checkpoint_inner(ack).map_err(crate::source_error)
    }

    async fn close(&mut self) -> SourceResult<()> {
        Ok(())
    }
}

impl SnowflakeCaptureSession {
    fn checkpoint_inner(&mut self, ack: CheckpointAck) -> Result<()> {
        if ack.source_id != self.source_id {
            return Err(Error::msg("checkpoint ack source_id does not match source"));
        }
        if ack.snapshot_uri.trim().is_empty() {
            return Err(Error::msg("checkpoint ack snapshot uri is required"));
        }

        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| Error::msg("no pending Snowflake batch to checkpoint"))?;
        if pending.checkpoint_end != ack.checkpoint {
            return Err(Error::msg("checkpoint ack does not match pending batch"));
        }

        self.phase = SessionPhase::Incremental {
            last_durable_boundary: decode_checkpoint(&ack.checkpoint)?,
        };
        self.pending = None;
        Ok(())
    }
}

fn changes_query_from_checkpoint(
    binding: &SnowflakeConnectorBinding,
    metadata: &TableMetadata,
    start: &Checkpoint,
    end_query_id: &str,
) -> String {
    let start_clause = match start {
        Checkpoint::Snapshot { .. } => format!(
            "AT (STREAM => '{}')",
            crate::client::quote_literal_value(&crate::client::qualified_table_name(
                &binding.source_schema,
                &binding.managed_stream_name,
            )),
        ),
        Checkpoint::Stream { boundary_query_id } => format!(
            "AT (STATEMENT => '{}')",
            crate::client::quote_literal_value(boundary_query_id),
        ),
    };

    changes_query_with_start_clause(binding, metadata, &start_clause, end_query_id)
}

fn changes_query_with_start_clause(
    binding: &SnowflakeConnectorBinding,
    metadata: &TableMetadata,
    start_clause: &str,
    end_query_id: &str,
) -> String {
    let select_list =
        std::iter::once("TO_VARCHAR(METADATA$ROW_ID) AS \"METADATA$ROW_ID\"".to_string())
            .chain(std::iter::once(
                "TO_VARCHAR(METADATA$ACTION) AS \"METADATA$ACTION\"".to_string(),
            ))
            .chain(std::iter::once(
                "TO_VARCHAR(METADATA$ISUPDATE) AS \"METADATA$ISUPDATE\"".to_string(),
            ))
            .chain(metadata.columns.iter().map(|column| {
                format!(
                    "TO_VARCHAR({}) AS {}",
                    crate::client::quote_identifier(column),
                    crate::client::quote_identifier(column),
                )
            }))
            .collect::<Vec<_>>()
            .join(", ");

    format!(
        "SELECT {select_list} FROM {} CHANGES(INFORMATION => DEFAULT) {start_clause} END(STATEMENT => '{}')",
        crate::client::qualified_table_name(&binding.source_schema, &binding.source_table),
        crate::client::quote_literal_value(end_query_id),
    )
}

#[cfg(test)]
fn test_binding(durable_checkpoint: Option<CheckpointId>) -> SnowflakeConnectorBinding {
    crate::SnowflakeConnectorBinding::from_request(crate::SnowflakeBindingRequest {
        connector_name: "snowflake_customer_state_append".to_string(),
        tables: vec![crate::SnowflakeTableBinding {
            source_schema: "PUBLIC".to_string(),
            source_table: "CUSTOMER_STATE".to_string(),
            destination_namespace: "customer_state".to_string(),
            destination_table: "customer_state".to_string(),
            table_mode: "append_only".to_string(),
        }],
        durable_checkpoint,
    })
    .expect("binding")
}

#[cfg(test)]
fn test_metadata() -> TableMetadata {
    TableMetadata {
        primary_keys: vec!["ID".to_string()],
        columns: vec!["ID".to_string(), "NAME".to_string()],
        schema_fingerprint: "PUBLIC:CUSTOMER_STATE:ID|VARCHAR|COLUMN;NAME|VARCHAR|COLUMN"
            .to_string(),
    }
}

#[cfg(test)]
struct TestSnowflakeClient;

#[cfg(test)]
impl SnowflakeClient for TestSnowflakeClient {
    fn exec(&self, _sql: &str) -> Result<StatementOutcome> {
        Ok(StatementOutcome {
            query_id: "01b12345-0602-1234-0000-000000000000".to_string(),
        })
    }

    fn query_rows(&self, sql: &str) -> Result<RowSet> {
        if sql.contains("RESULT_SCAN('01b12345-0602-1234-0000-000000000000')") {
            return Ok(RowSet {
                query_id: "pk-query".to_string(),
                columns: vec!["column_name".to_string()],
                rows: vec![vec!["ID".to_string()]],
            });
        }

        if sql.starts_with("DESCRIBE TABLE") {
            return Ok(RowSet {
                query_id: "describe-query".to_string(),
                columns: Vec::new(),
                rows: vec![
                    vec![
                        "ID".to_string(),
                        "VARCHAR".to_string(),
                        "COLUMN".to_string(),
                    ],
                    vec![
                        "NAME".to_string(),
                        "VARCHAR".to_string(),
                        "COLUMN".to_string(),
                    ],
                ],
            });
        }

        Ok(RowSet {
            query_id: "changes-query".to_string(),
            columns: vec![
                "METADATA$ROW_ID".to_string(),
                "METADATA$ACTION".to_string(),
                "METADATA$ISUPDATE".to_string(),
                "ID".to_string(),
                "NAME".to_string(),
            ],
            rows: vec![vec![
                "row-1".to_string(),
                "INSERT".to_string(),
                "false".to_string(),
                "1".to_string(),
                "after".to_string(),
            ]],
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::checkpoint::{decode_checkpoint, Checkpoint};
    use crate::client::{RowSet, SnowflakeClient, StatementOutcome};
    use iceflow_source::{BatchPoll, BatchRequest, SourceCaptureSession};
    use std::sync::{Arc, Mutex};
    use tokio::runtime::Builder;

    #[test]
    fn fresh_session_returns_one_snapshot_batch() {
        let mut session = super::SnowflakeCaptureSession::new_from_snapshot(fake_snapshot_rows());

        let first = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(session.poll_batch(BatchRequest::default()))
            .expect("poll");
        let BatchPoll::Batch(batch) = first else {
            panic!("expected snapshot batch");
        };

        assert_eq!(batch.records.len(), 2);
        assert_eq!(
            decode_checkpoint(&batch.checkpoint_end).expect("decode"),
            Checkpoint::Snapshot {
                anchor_query_id: "01b12345-0600-1234-0000-000000000000".to_string(),
            }
        );
    }

    #[test]
    fn snapshot_batch_replays_until_checkpoint_ack() {
        let mut session = super::SnowflakeCaptureSession::new_from_snapshot(fake_snapshot_rows());

        let first = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(session.poll_batch(BatchRequest::default()))
            .expect("poll");
        let second = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(session.poll_batch(BatchRequest::default()))
            .expect("poll");

        assert_eq!(first, second);
    }

    #[test]
    fn incremental_batch_replays_until_checkpoint_ack() {
        let mut session =
            super::SnowflakeCaptureSession::new_for_resume_test(iceflow_types::CheckpointId::from(
                "snowflake:v1:stream:01b12345-0601-1234-0000-000000000000",
            ));

        let first = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(session.poll_batch(BatchRequest::default()))
            .expect("poll");
        let second = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(session.poll_batch(BatchRequest::default()))
            .expect("poll");

        assert_eq!(first, second);
    }

    #[test]
    fn changes_query_casts_metadata_and_data_columns_to_varchar() {
        let binding = super::test_binding(None);
        let metadata = super::test_metadata();

        let query = super::changes_query_from_checkpoint(
            &binding,
            &metadata,
            &Checkpoint::Stream {
                boundary_query_id: "01b12345-0601-1234-0000-000000000000".to_string(),
            },
            "01b12345-0602-1234-0000-000000000000",
        );

        assert!(query.contains("TO_VARCHAR(METADATA$ROW_ID) AS \"METADATA$ROW_ID\""));
        assert!(query.contains("TO_VARCHAR(METADATA$ISUPDATE) AS \"METADATA$ISUPDATE\""));
        assert!(query.contains("TO_VARCHAR(\"NAME\") AS \"NAME\""));
    }

    #[test]
    fn first_incremental_after_snapshot_checkpoint_uses_stream_anchor() {
        let client = RecordingIncrementalClient::default();
        let binding = super::test_binding(None);
        let mut session = super::SnowflakeCaptureSession::new_incremental(
            "snowflake.config.local_snowflake".to_string(),
            iceflow_types::TableId::from("customer_state.customer_state"),
            iceflow_types::TableMode::AppendOnly,
            binding.clone(),
            Arc::new(client.clone()),
            super::test_metadata(),
            Checkpoint::Snapshot {
                anchor_query_id: "01b12345-0600-1234-0000-000000000000".to_string(),
            },
        );

        let poll = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(session.poll_batch(BatchRequest::default()))
            .expect("poll");
        assert!(matches!(poll, BatchPoll::Batch(_)));

        let queries = client.queries.lock().expect("queries");
        let changes_query = queries
            .iter()
            .find(|sql| sql.contains("CHANGES(INFORMATION => DEFAULT)"))
            .expect("changes query");

        assert!(changes_query.contains("AT (STREAM =>"));
        assert!(changes_query.contains(&binding.managed_stream_name));
        assert!(changes_query.contains("END(STATEMENT => '01b12345-0602-1234-0000-000000000000')"));
        assert!(!changes_query.contains("AT (STATEMENT => '01b12345-0600-1234-0000-000000000000')"));
    }

    fn fake_snapshot_rows() -> Vec<iceflow_types::LogicalMutation> {
        crate::value::test_mutations(
            2,
            "snowflake:v1:snapshot:01b12345-0600-1234-0000-000000000000",
        )
        .expect("test mutations")
    }

    #[derive(Clone, Default)]
    struct RecordingIncrementalClient {
        queries: Arc<Mutex<Vec<String>>>,
    }

    impl SnowflakeClient for RecordingIncrementalClient {
        fn exec(&self, _sql: &str) -> anyhow::Result<StatementOutcome> {
            Ok(StatementOutcome {
                query_id: "01b12345-0602-1234-0000-000000000000".to_string(),
            })
        }

        fn query_rows(&self, sql: &str) -> anyhow::Result<RowSet> {
            self.queries.lock().expect("queries").push(sql.to_string());
            if sql.starts_with("DESCRIBE TABLE") {
                return Ok(RowSet {
                    query_id: "describe-query".to_string(),
                    columns: Vec::new(),
                    rows: vec![
                        vec![
                            "ID".to_string(),
                            "VARCHAR".to_string(),
                            "COLUMN".to_string(),
                        ],
                        vec![
                            "NAME".to_string(),
                            "VARCHAR".to_string(),
                            "COLUMN".to_string(),
                        ],
                    ],
                });
            }

            Ok(RowSet {
                query_id: "changes-query".to_string(),
                columns: vec![
                    "METADATA$ROW_ID".to_string(),
                    "METADATA$ACTION".to_string(),
                    "METADATA$ISUPDATE".to_string(),
                    "ID".to_string(),
                    "NAME".to_string(),
                ],
                rows: vec![vec![
                    "row-1".to_string(),
                    "INSERT".to_string(),
                    "false".to_string(),
                    "1".to_string(),
                    "after".to_string(),
                ]],
            })
        }
    }
}
