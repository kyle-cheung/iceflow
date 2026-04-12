mod binding;
mod checkpoint;
pub mod client;
pub mod config;
#[cfg(test)]
mod env_test_support;
mod metadata;
mod session;
mod value;

use anyhow::Result;
use async_trait::async_trait;
use iceflow_source::{
    validate_source_spec, OpenCaptureRequest, SourceAdapter, SourceCapability, SourceCheckReport,
    SourceError, SourceResult, SourceSpec,
};
use iceflow_types::SourceClass;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub use binding::{SnowflakeBindingRequest, SnowflakeConnectorBinding, SnowflakeTableBinding};
pub use checkpoint::{decode_checkpoint, encode_checkpoint, Checkpoint};
pub use config::{SnowflakeAuthMethod, SnowflakeSourceConfig};

const ADBC_AUTH_TYPE_ENV: &str = "ADBC_SNOWFLAKE_SQL_AUTH_TYPE";
const ADBC_JWT_KEY_ENV_VARS: &[&str] = &[
    "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY",
    "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE",
];
#[cfg(test)]
const EXTERNAL_ADBC_AUTH_ENV_VARS: &[&str] = &[
    ADBC_AUTH_TYPE_ENV,
    "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY",
    "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE",
];

pub struct SnowflakeSource {
    config: SnowflakeSourceConfig,
    source_id: String,
    binding: Option<SnowflakeConnectorBinding>,
    client: Arc<dyn client::SnowflakeClient + Send + Sync>,
}

impl SnowflakeSource {
    pub fn new(
        config: SnowflakeSourceConfig,
        binding: Option<SnowflakeConnectorBinding>,
        client: Box<dyn client::SnowflakeClient + Send + Sync>,
    ) -> Self {
        let source_id = format!("snowflake.config.{}", config.source_label);
        Self {
            config,
            source_id,
            binding,
            client: Arc::from(client),
        }
    }

    fn source_id(&self) -> &str {
        &self.source_id
    }
}

#[async_trait]
impl SourceAdapter for SnowflakeSource {
    async fn spec(&self) -> SourceResult<SourceSpec> {
        let spec = SourceSpec {
            source_id: self.source_id().to_string(),
            source_class: SourceClass::DatabaseCdc,
        };
        validate_source_spec(&spec)?;
        Ok(spec)
    }

    async fn check(&self) -> SourceResult<SourceCheckReport> {
        let mut capabilities = BTreeSet::new();
        capabilities.insert(SourceCapability::AppendOnly);
        capabilities.insert(SourceCapability::InitialSnapshot);
        capabilities.insert(SourceCapability::ChangeFeed);
        capabilities.insert(SourceCapability::SnapshotHandoff);
        capabilities.insert(SourceCapability::Deletes);
        capabilities.insert(SourceCapability::Resume);
        capabilities.insert(SourceCapability::DeterministicCheckpoints);

        let mut warnings = Vec::new();
        let mut details = BTreeMap::from([
            ("account".to_string(), self.config.account.clone()),
            ("database".to_string(), self.config.database.clone()),
            ("warehouse".to_string(), self.config.warehouse.clone()),
            ("role".to_string(), self.config.role.clone()),
        ]);
        if needs_external_password_warning(&self.config) {
            warnings.push(
                "Snowflake password is empty; connection relies on external ADBC authentication environment"
                    .to_string(),
            );
        }

        if let Some(binding) = &self.binding {
            let metadata = metadata::load_table_metadata(self.client.as_ref(), binding)
                .map_err(source_error)?;
            details.insert("source_schema".to_string(), binding.source_schema.clone());
            details.insert("source_table".to_string(), binding.source_table.clone());
            details.insert(
                "managed_stream".to_string(),
                binding.managed_stream_name.clone(),
            );
            details.insert("primary_keys".to_string(), metadata.primary_keys.join(","));
            let stream_grants = metadata::validate_stream_grants(
                self.client.as_ref(),
                &self.config.role,
                &self.config.database,
                binding,
            )
            .map_err(source_error)?;
            details.insert(
                "stream_privilege_database".to_string(),
                stream_grants.database_privilege,
            );
            details.insert(
                "stream_privilege_schema".to_string(),
                stream_grants.schema_privilege,
            );
            details.insert(
                "stream_privilege_table".to_string(),
                stream_grants.table_privilege,
            );
            details.insert(
                "stream_privilege_change_tracking".to_string(),
                "not_verified_side_effect_free".to_string(),
            );
            warnings.extend(stream_grants.warnings);
            warnings.push(
                "Snowflake updates and deletes are captured, but current real sinks do not yet converge mutable row state"
                    .to_string(),
            );
        }

        Ok(SourceCheckReport {
            capabilities,
            warnings,
            details,
        })
    }

    async fn open_capture(
        &self,
        req: OpenCaptureRequest,
    ) -> SourceResult<Box<dyn iceflow_source::SourceCaptureSession + Send>> {
        let binding = self
            .binding
            .as_ref()
            .ok_or_else(|| source_error("snowflake open_capture requires a bound connector"))?;

        if req.table.source_schema != binding.source_schema
            || req.table.source_table != binding.source_table
        {
            return Err(source_error(
                "open_capture request does not match bound Snowflake table",
            ));
        }

        let metadata =
            metadata::load_table_metadata(self.client.as_ref(), binding).map_err(source_error)?;
        let resume_from = req
            .resume_from
            .clone()
            .or_else(|| binding.durable_checkpoint.clone());
        if let Some(checkpoint) = resume_from {
            let decoded = decode_checkpoint(&checkpoint).map_err(source_error)?;
            recreate_stream_at_checkpoint(self.client.as_ref(), binding, &decoded)
                .map_err(source_error)?;
            return Ok(Box::new(session::SnowflakeCaptureSession::new_incremental(
                self.source_id().to_string(),
                req.table.table_id,
                req.table.table_mode,
                binding.clone(),
                Arc::clone(&self.client),
                metadata,
                decoded,
            )));
        }

        let anchor = self
            .client
            .exec(&create_stream_on_table_sql(binding))
            .map_err(source_error)?;
        let checkpoint_end = encode_checkpoint(Checkpoint::Snapshot {
            anchor_query_id: anchor.query_id.clone(),
        });
        let snapshot = self
            .client
            .query_rows(&snapshot_query(binding, &metadata, &anchor.query_id))
            .map_err(source_error)?;
        let records = value::snapshot_rows_to_mutations(
            value::MutationContext::new(
                req.table.table_id.clone(),
                self.source_id().to_string(),
                req.table.table_mode,
                &metadata,
                checkpoint_end.clone(),
                snapshot.query_id.clone(),
            ),
            snapshot,
        )
        .map_err(source_error)?;
        let batch = iceflow_source::SourceBatch {
            batch_label: Some(format!("snowflake-snapshot-{}", anchor.query_id)),
            checkpoint_start: None,
            checkpoint_end,
            records,
        };

        Ok(Box::new(
            session::SnowflakeCaptureSession::new_with_snapshot(
                self.source_id().to_string(),
                req.table.table_id,
                req.table.table_mode,
                binding.clone(),
                Arc::clone(&self.client),
                metadata,
                batch,
            ),
        ))
    }
}

pub(crate) fn source_error(err: impl std::fmt::Display) -> SourceError {
    SourceError::msg(err.to_string())
}

fn needs_external_password_warning(config: &SnowflakeSourceConfig) -> bool {
    matches!(config.auth_method, SnowflakeAuthMethod::Password)
        && config.password.is_empty()
        && !has_external_adbc_auth_env()
}

fn has_external_adbc_auth_env() -> bool {
    let auth_type = std::env::var(ADBC_AUTH_TYPE_ENV)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let has_jwt_key = ADBC_JWT_KEY_ENV_VARS
        .iter()
        .any(|name| std::env::var(name).is_ok_and(|value| !value.trim().is_empty()));

    auth_type == "auth_jwt" && has_jwt_key
}

fn recreate_stream_at_checkpoint(
    client: &dyn client::SnowflakeClient,
    binding: &SnowflakeConnectorBinding,
    checkpoint: &Checkpoint,
) -> Result<()> {
    let query_id = match checkpoint {
        Checkpoint::Snapshot { anchor_query_id } => anchor_query_id,
        Checkpoint::Stream { boundary_query_id } => boundary_query_id,
    };
    client.exec(&create_stream_at_statement_sql(binding, query_id))?;
    Ok(())
}

fn snapshot_query(
    binding: &SnowflakeConnectorBinding,
    metadata: &metadata::TableMetadata,
    _anchor_query_id: &str,
) -> String {
    let select_list = metadata
        .columns
        .iter()
        .map(|column| {
            format!(
                "TO_VARCHAR({}) AS {}",
                client::quote_identifier(column),
                client::quote_identifier(column),
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let order_by = metadata
        .primary_keys
        .iter()
        .map(|key| client::quote_identifier(key))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "SELECT {select_list} FROM {} AT (STREAM => '{}') ORDER BY {}",
        client::qualified_table_name(&binding.source_schema, &binding.source_table),
        client::quote_literal_value(&managed_stream_sql_name(binding)),
        order_by,
    )
}

fn managed_stream_sql_name(binding: &SnowflakeConnectorBinding) -> String {
    client::qualified_table_name(&binding.source_schema, &binding.managed_stream_name)
}

fn create_stream_at_statement_sql(binding: &SnowflakeConnectorBinding, query_id: &str) -> String {
    format!(
        "CREATE OR REPLACE STREAM {} ON TABLE {} AT (STATEMENT => '{}')",
        managed_stream_sql_name(binding),
        client::qualified_table_name(&binding.source_schema, &binding.source_table),
        client::quote_literal_value(query_id),
    )
}

fn create_stream_on_table_sql(binding: &SnowflakeConnectorBinding) -> String {
    format!(
        "CREATE OR REPLACE STREAM {} ON TABLE {}",
        managed_stream_sql_name(binding),
        client::qualified_table_name(&binding.source_schema, &binding.source_table),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env_test_support::SavedEnv;
    use iceflow_source::{
        BatchPoll, BatchRequest, SourceAdapter, SourceCapability, SourceTableSelection,
    };
    use iceflow_types::{TableId, TableMode};
    use std::sync::{Arc, Mutex};
    use tokio::runtime::Builder;

    #[derive(Default)]
    struct FakeSnowflakeClient;

    impl client::SnowflakeClient for FakeSnowflakeClient {
        fn exec(&self, _sql: &str) -> anyhow::Result<client::StatementOutcome> {
            unreachable!("spec/check should not execute SQL")
        }

        fn query_rows(&self, _sql: &str) -> anyhow::Result<client::RowSet> {
            unreachable!("spec/check should not query rows")
        }
    }

    fn sample_config() -> SnowflakeSourceConfig {
        SnowflakeSourceConfig {
            source_label: "local_snowflake".to_string(),
            account: "xy12345.us-east-1".to_string(),
            user: "ICEFLOW_DEMO".to_string(),
            password: "secret".to_string(),
            warehouse: "ICEFLOW_WH".to_string(),
            role: "ICEFLOW_ROLE".to_string(),
            database: "SOURCE_DB".to_string(),
            auth_method: SnowflakeAuthMethod::Password,
        }
    }

    fn sample_empty_password_config() -> SnowflakeSourceConfig {
        SnowflakeSourceConfig {
            password: String::new(),
            ..sample_config()
        }
    }

    #[test]
    fn spec_reports_snowflake_source_class() {
        let source = SnowflakeSource::new(sample_config(), None, Box::new(FakeSnowflakeClient));

        let spec = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(source.spec())
            .expect("spec");

        assert_eq!(spec.source_id, "snowflake.config.local_snowflake");
        assert_eq!(spec.source_class, iceflow_types::SourceClass::DatabaseCdc);
    }

    #[test]
    fn unbound_check_reports_expected_capabilities() {
        let source = SnowflakeSource::new(sample_config(), None, Box::new(FakeSnowflakeClient));

        let report = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(source.check())
            .expect("check");

        assert!(report
            .capabilities
            .contains(&SourceCapability::InitialSnapshot));
        assert!(report.capabilities.contains(&SourceCapability::ChangeFeed));
        assert!(!report.capabilities.contains(&SourceCapability::KeyedUpsert));
    }

    #[test]
    fn unbound_check_warns_when_password_is_empty_without_external_auth_env() {
        let _lock = env_lock().lock().expect("env lock");
        let _env = SavedEnv::capture(EXTERNAL_ADBC_AUTH_ENV_VARS);
        for name in EXTERNAL_ADBC_AUTH_ENV_VARS {
            std::env::remove_var(name);
        }
        let source = SnowflakeSource::new(
            sample_empty_password_config(),
            None,
            Box::new(FakeSnowflakeClient),
        );

        let report = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(source.check())
            .expect("check");

        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("external ADBC authentication environment")));
    }

    #[test]
    fn unbound_check_warns_when_auth_type_env_is_present_without_jwt_key() {
        let _lock = env_lock().lock().expect("env lock");
        let _env = SavedEnv::capture(EXTERNAL_ADBC_AUTH_ENV_VARS);
        for name in EXTERNAL_ADBC_AUTH_ENV_VARS {
            std::env::remove_var(name);
        }
        std::env::set_var("ADBC_SNOWFLAKE_SQL_AUTH_TYPE", "auth_jwt");
        let source = SnowflakeSource::new(
            sample_empty_password_config(),
            None,
            Box::new(FakeSnowflakeClient),
        );

        let report = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(source.check())
            .expect("check");

        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("Snowflake password is empty")));
    }

    #[test]
    fn unbound_check_suppresses_empty_password_warning_for_complete_jwt_auth_env() {
        let _lock = env_lock().lock().expect("env lock");
        let _env = SavedEnv::capture(EXTERNAL_ADBC_AUTH_ENV_VARS);
        for name in EXTERNAL_ADBC_AUTH_ENV_VARS {
            std::env::remove_var(name);
        }
        std::env::set_var("ADBC_SNOWFLAKE_SQL_AUTH_TYPE", "auth_jwt");
        std::env::set_var(
            "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY",
            "private-key",
        );
        let source = SnowflakeSource::new(
            sample_empty_password_config(),
            None,
            Box::new(FakeSnowflakeClient),
        );

        let report = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(source.check())
            .expect("check");

        assert!(!report
            .warnings
            .iter()
            .any(|warning| warning.contains("Snowflake password is empty")));
    }

    #[test]
    fn snapshot_query_casts_selected_columns_to_varchar() {
        let binding = SnowflakeConnectorBinding::from_request(SnowflakeBindingRequest {
            connector_name: "snowflake_customer_state_append".to_string(),
            tables: vec![SnowflakeTableBinding {
                source_schema: "PUBLIC".to_string(),
                source_table: "CUSTOMER_STATE".to_string(),
                destination_namespace: "customer_state".to_string(),
                destination_table: "customer_state".to_string(),
                table_mode: "append_only".to_string(),
            }],
            durable_checkpoint: None,
        })
        .expect("binding");
        let metadata = metadata::TableMetadata {
            primary_keys: vec!["CUSTOMER_ID".to_string()],
            columns: vec!["CUSTOMER_ID".to_string(), "UPDATED_AT".to_string()],
            schema_fingerprint: "fingerprint-v1".to_string(),
        };

        let query = snapshot_query(&binding, &metadata, "01b12345-0600-1234-0000-000000000000");

        assert!(query.contains("TO_VARCHAR(\"CUSTOMER_ID\") AS \"CUSTOMER_ID\""));
        assert!(query.contains("TO_VARCHAR(\"UPDATED_AT\") AS \"UPDATED_AT\""));
        assert!(query.contains("AT (STREAM => '\"PUBLIC\".\"_iceflow_"));
        assert!(!query.contains("AT (STATEMENT"));
    }

    #[test]
    fn fresh_open_capture_anchors_snapshot_on_create_stream_query_id() {
        let client = RecordingSnowflakeClient::default();
        let binding = sample_binding(None);
        let source = SnowflakeSource::new(
            sample_config(),
            Some(binding.clone()),
            Box::new(client.clone()),
        );

        let mut session = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(source.open_capture(OpenCaptureRequest {
                table: SourceTableSelection {
                    table_id: TableId::from("customer_state.customer_state"),
                    source_schema: binding.source_schema.clone(),
                    source_table: binding.source_table.clone(),
                    table_mode: TableMode::AppendOnly,
                },
                resume_from: None,
            }))
            .expect("open capture");
        let poll = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(session.poll_batch(BatchRequest::default()))
            .expect("poll");
        let BatchPoll::Batch(batch) = poll else {
            panic!("expected snapshot batch");
        };

        assert_eq!(
            decode_checkpoint(&batch.checkpoint_end).expect("checkpoint"),
            Checkpoint::Snapshot {
                anchor_query_id: "create-stream-query".to_string(),
            }
        );

        let execs = client.execs.lock().expect("execs");
        assert!(
            execs.iter().all(|sql| sql != "SELECT 1"),
            "fresh bootstrap should not use an unrelated SELECT 1 anchor: {execs:?}"
        );
        assert!(
            execs.iter().any(|sql| {
                sql.starts_with("CREATE OR REPLACE STREAM ")
                    && !sql.contains(" AT (STATEMENT")
                    && sql.contains(" ON TABLE \"PUBLIC\".\"CUSTOMER_STATE\"")
            }),
            "fresh bootstrap should create the stream at the table current offset: {execs:?}"
        );

        let queries = client.queries.lock().expect("queries");
        let snapshot_query = queries
            .iter()
            .find(|sql| sql.contains("AT (STREAM =>"))
            .expect("snapshot query should use the managed stream anchor");
        assert!(snapshot_query.contains(&binding.managed_stream_name));
    }

    #[test]
    fn bound_check_validates_stream_grants_without_stream_ddl() {
        let client = RecordingSnowflakeClient::default();
        let source = SnowflakeSource::new(
            sample_config(),
            Some(sample_binding(None)),
            Box::new(client.clone()),
        );

        let report = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(source.check())
            .expect("check");

        assert_eq!(
            report.details.get("stream_privilege_database"),
            Some(&"USAGE".to_string())
        );
        assert_eq!(
            report.details.get("stream_privilege_schema"),
            Some(&"CREATE STREAM".to_string())
        );
        assert_eq!(
            report.details.get("stream_privilege_table"),
            Some(&"SELECT".to_string())
        );
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning
                .contains("cannot prove Snowflake change tracking is already enabled")));

        let execs = client.execs.lock().expect("execs");
        assert!(
            execs
                .iter()
                .any(|sql| sql == "SHOW GRANTS TO ROLE \"ICEFLOW_ROLE\""),
            "check should introspect role grants: {execs:?}"
        );
        assert!(
            execs
                .iter()
                .all(|sql| !sql.starts_with("CREATE OR REPLACE STREAM")),
            "check must not run stream DDL: {execs:?}"
        );
    }

    #[test]
    fn bound_check_rejects_missing_stream_grants() {
        let client = RecordingSnowflakeClient {
            grant_rows: Arc::new(Mutex::new(vec![vec![
                "USAGE".to_string(),
                "SCHEMA".to_string(),
                "SOURCE_DB.PUBLIC".to_string(),
            ]])),
            ..RecordingSnowflakeClient::default()
        };
        let source = SnowflakeSource::new(
            sample_config(),
            Some(sample_binding(None)),
            Box::new(client),
        );

        let err = Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(source.check())
            .expect_err("check should reject missing stream grants");

        assert!(err.to_string().contains("CREATE STREAM"));
        assert!(err.to_string().contains("SELECT"));
    }

    fn env_lock() -> &'static std::sync::Mutex<()> {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        &ENV_LOCK
    }

    fn sample_binding(
        durable_checkpoint: Option<iceflow_types::CheckpointId>,
    ) -> SnowflakeConnectorBinding {
        SnowflakeConnectorBinding::from_request(SnowflakeBindingRequest {
            connector_name: "snowflake_customer_state_append".to_string(),
            tables: vec![SnowflakeTableBinding {
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

    #[derive(Clone, Default)]
    struct RecordingSnowflakeClient {
        execs: Arc<Mutex<Vec<String>>>,
        queries: Arc<Mutex<Vec<String>>>,
        grant_rows: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl client::SnowflakeClient for RecordingSnowflakeClient {
        fn exec(&self, sql: &str) -> anyhow::Result<client::StatementOutcome> {
            self.execs.lock().expect("execs").push(sql.to_string());
            let query_id = if sql.starts_with("SHOW PRIMARY KEYS") {
                "show-pk-query"
            } else if sql.starts_with("SHOW GRANTS TO ROLE") {
                "show-grants-query"
            } else if sql.starts_with("CREATE OR REPLACE STREAM") {
                "create-stream-query"
            } else if sql == "SELECT 1" {
                "select-one-query"
            } else {
                "exec-query"
            };
            Ok(client::StatementOutcome {
                query_id: query_id.to_string(),
            })
        }

        fn query_rows(&self, sql: &str) -> anyhow::Result<client::RowSet> {
            self.queries.lock().expect("queries").push(sql.to_string());
            if sql.contains("RESULT_SCAN('show-pk-query')") {
                return Ok(client::RowSet {
                    query_id: "pk-query".to_string(),
                    columns: vec!["column_name".to_string()],
                    rows: vec![vec!["CUSTOMER_ID".to_string()]],
                });
            }

            if sql.contains("RESULT_SCAN('show-grants-query')") {
                let rows = self.grant_rows.lock().expect("grant rows");
                let rows = if rows.is_empty() {
                    vec![
                        vec![
                            "USAGE".to_string(),
                            "DATABASE".to_string(),
                            "SOURCE_DB".to_string(),
                        ],
                        vec![
                            "CREATE STREAM".to_string(),
                            "SCHEMA".to_string(),
                            "SOURCE_DB.PUBLIC".to_string(),
                        ],
                        vec![
                            "SELECT".to_string(),
                            "TABLE".to_string(),
                            "SOURCE_DB.PUBLIC.CUSTOMER_STATE".to_string(),
                        ],
                    ]
                } else {
                    rows.clone()
                };
                return Ok(client::RowSet {
                    query_id: "grants-query".to_string(),
                    columns: vec![
                        "privilege".to_string(),
                        "granted_on".to_string(),
                        "name".to_string(),
                    ],
                    rows,
                });
            }

            if sql.starts_with("DESCRIBE TABLE") {
                return Ok(client::RowSet {
                    query_id: "describe-query".to_string(),
                    columns: Vec::new(),
                    rows: vec![
                        vec![
                            "CUSTOMER_ID".to_string(),
                            "VARCHAR".to_string(),
                            "COLUMN".to_string(),
                        ],
                        vec![
                            "STATUS".to_string(),
                            "VARCHAR".to_string(),
                            "COLUMN".to_string(),
                        ],
                    ],
                });
            }

            Ok(client::RowSet {
                query_id: "snapshot-query".to_string(),
                columns: vec!["CUSTOMER_ID".to_string(), "STATUS".to_string()],
                rows: vec![vec!["customer-001".to_string(), "active".to_string()]],
            })
        }
    }
}
