use anyhow::{Error, Result};
use iceflow_runtime::{CheckpointDecision, IntakeDecision, RuntimeCoordinator};
use iceflow_sink::{CommitRequest as SinkCommitRequest, Sink};
use iceflow_source::{
    BatchPoll, BatchRequest, CheckpointAck as SourceCheckpointAck, OpenCaptureRequest,
    SourceCapability, SourceTableSelection,
};
use iceflow_state::{
    checkpoint_ack as state_checkpoint_ack, checkpoint_ref, AttemptResolution,
    CommitRequest as StateCommitRequest, SnapshotRef as StateSnapshotRef, SqliteStateStore,
    StateStore,
};
use iceflow_types::TableMode;
use iceflow_worker_duckdb::DuckDbWorker;
use serde::Serialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::commands::run::{
    finalize_run_result, first_attempt_key, idle_backoff_sleep, state_files_for_manifest,
};
use crate::config::{
    build_bound_source_from_config, build_sink_from_config, connector_table_id,
    load_catalog_config, load_connector_config, load_destination_config,
    load_optional_catalog_config, load_source_config, resolve_catalog_name, BoundSourceContext,
    CaptureSettings, ConnectorConfig, DestinationConfig,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckArgs {
    pub connector_config: PathBuf,
    pub config_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunArgs {
    pub connector_config: PathBuf,
    pub config_root: PathBuf,
    pub batch_limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckReport {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub table_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunReport {
    pub tables_processed: usize,
    pub total_committed_batches: usize,
    pub total_committed_files: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TableRunStats {
    committed_batches: usize,
    committed_files: usize,
}

impl CheckArgs {
    pub fn parse(args: &[String]) -> Result<Self> {
        let connector_config = args
            .iter()
            .position(|arg| arg == "--connector")
            .and_then(|index| args.get(index + 1))
            .map(PathBuf::from)
            .ok_or_else(|| Error::msg("--connector <path> is required"))?;
        let config_root = infer_config_root(&connector_config)?;

        Ok(Self {
            connector_config,
            config_root,
        })
    }
}

impl RunArgs {
    pub fn parse(args: &[String]) -> Result<Self> {
        let mut connector_config = None;
        let mut batch_limit = None;
        let mut index = 0;

        while index < args.len() {
            match args[index].as_str() {
                "--connector" => {
                    index += 1;
                    connector_config = args.get(index).map(PathBuf::from);
                }
                "--batch-limit" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| Error::msg("--batch-limit requires a value"))?;
                    let parsed = value
                        .parse::<usize>()
                        .map_err(|_| Error::msg("--batch-limit must be a positive integer"))?;
                    if parsed == 0 {
                        return Err(Error::msg("--batch-limit must be greater than zero"));
                    }
                    batch_limit = Some(parsed);
                }
                other => {
                    return Err(Error::msg(format!(
                        "unknown connector run argument: {other}"
                    )));
                }
            }
            index += 1;
        }

        let connector_config =
            connector_config.ok_or_else(|| Error::msg("--connector <path> is required"))?;
        let config_root = infer_config_root(&connector_config)?;

        Ok(Self {
            connector_config,
            config_root,
            batch_limit,
        })
    }
}

impl CheckReport {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("connector check report serialization should not fail")
    }
}

impl RunReport {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("connector run report serialization should not fail")
    }
}

pub fn check_blocking(args: CheckArgs) -> Result<CheckReport> {
    crate::block_on(check(args))
}

pub fn run_blocking(args: RunArgs) -> Result<RunReport> {
    crate::block_on(run(args))
}

pub async fn check(args: CheckArgs) -> Result<CheckReport> {
    let connector = load_connector_config(&args.connector_config)?;
    let source_path = args
        .config_root
        .join("sources")
        .join(format!("{}.toml", connector.source));
    let source_config = load_source_config(&source_path)
        .map_err(|err| Error::msg(format!("source '{}': {err}", connector.source)))?;
    let destination_path = args
        .config_root
        .join("destinations")
        .join(format!("{}.toml", connector.destination));
    let destination_config = load_destination_config(&destination_path)
        .map_err(|err| Error::msg(format!("destination '{}': {err}", connector.destination)))?;
    let source = build_bound_source_from_config(
        &source_config,
        source_path.parent().unwrap_or_else(|| Path::new(".")),
        &BoundSourceContext {
            connector_name: connector_name(&args.connector_config)?,
            connector: connector.clone(),
            durable_checkpoint: None,
        },
    )
    .map_err(|err| Error::msg(format!("build source '{}': {err}", connector.source)))?;
    let source_report = source
        .check()
        .await
        .map_err(|err| Error::msg(format!("source check '{}': {err}", connector.source)))?;

    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let catalog_state = validate_catalog_configuration(
        &args.config_root,
        &connector,
        &destination_config,
        &mut errors,
    )?;

    match (&destination_config.kind[..], &catalog_state) {
        ("filesystem", CatalogValidation::Resolved(name)) => errors.push(format!(
            "destination '{}' does not support catalog '{}'",
            connector.destination, name
        )),
        ("polaris", CatalogValidation::Unresolved) => errors.push(format!(
            "destination '{}' requires a catalog reference",
            connector.destination
        )),
        _ => {}
    }

    if connector.tables.is_empty() {
        errors.push("connector must declare at least one table".to_string());
    }

    if source_config.kind == "file" && connector.capture != CaptureSettings::default() {
        errors.push(
            "file source connectors must not declare publication, slot, or bootstrap capture settings"
                .to_string(),
        );
    }

    for (index, table) in connector.tables.iter().enumerate() {
        match table.table_mode.as_str() {
            "append_only" | "keyed_upsert" => {}
            other => errors.push(format!("tables[{index}]: unsupported table_mode '{other}'")),
        }

        if table.source_table.is_empty() {
            errors.push(format!("tables[{index}]: source_table is required"));
        }
        if table.destination_table.is_empty() {
            errors.push(format!("tables[{index}]: destination_table is required"));
        }
        if table.destination_namespace.is_empty() {
            warnings.push(format!(
                "tables[{index}]: empty destination_namespace is allowed but should be deliberate"
            ));
        }
        if table.table_mode == "keyed_upsert" && table.key_columns.is_empty() {
            errors.push(format!(
                "tables[{index}]: keyed_upsert requires key_columns"
            ));
        }
        if table.table_mode == "keyed_upsert" && table.ordering_field.is_none() {
            errors.push(format!(
                "tables[{index}]: keyed_upsert requires ordering_field"
            ));
        }

        if table.table_mode == "append_only"
            && !source_report
                .capabilities
                .contains(&SourceCapability::AppendOnly)
        {
            errors.push(format!(
                "tables[{index}]: source does not advertise append_only capability"
            ));
        }
        if table.table_mode == "keyed_upsert"
            && !source_report
                .capabilities
                .contains(&SourceCapability::KeyedUpsert)
        {
            errors.push(format!(
                "tables[{index}]: source does not advertise keyed_upsert capability"
            ));
        }
        if table.table_mode == "keyed_upsert"
            && !source_report
                .capabilities
                .contains(&SourceCapability::StableLatestWinsOrdering)
        {
            errors.push(format!(
                "tables[{index}]: source does not advertise stable_latest_wins_ordering capability"
            ));
        }
    }

    Ok(CheckReport {
        valid: errors.is_empty(),
        errors,
        warnings,
        table_count: connector.tables.len(),
    })
}

pub async fn run(args: RunArgs) -> Result<RunReport> {
    let state = open_connector_state(&args.connector_config, &args.config_root).await?;
    run_with_state(args, &state).await
}

pub async fn run_with_state<S>(args: RunArgs, state: &S) -> Result<RunReport>
where
    S: StateStore,
{
    let connector = load_connector_config(&args.connector_config)?;
    let source_path = args
        .config_root
        .join("sources")
        .join(format!("{}.toml", connector.source));
    let source_config = load_source_config(&source_path)
        .map_err(|err| Error::msg(format!("source '{}': {err}", connector.source)))?;
    let destination_path = args
        .config_root
        .join("destinations")
        .join(format!("{}.toml", connector.destination));
    let destination_config = load_destination_config(&destination_path)
        .map_err(|err| Error::msg(format!("destination '{}': {err}", connector.destination)))?;
    let catalog_config =
        load_optional_catalog_config(&args.config_root, &connector, &destination_config)
            .map_err(|err| Error::msg(format!("catalog resolution: {err}")))?;
    let durable_checkpoint = match connector.tables.first() {
        Some(table) => {
            state
                .last_durable_checkpoint_for_table(&connector_table_id(table))
                .await?
                .map(|checkpoint| checkpoint.checkpoint)
        }
        None => None,
    };
    let source = build_bound_source_from_config(
        &source_config,
        source_path.parent().unwrap_or_else(|| Path::new(".")),
        &BoundSourceContext {
            connector_name: connector_name(&args.connector_config)?,
            connector: connector.clone(),
            durable_checkpoint,
        },
    )
    .map_err(|err| Error::msg(format!("build source '{}': {err}", connector.source)))?;
    let source_spec = source
        .spec()
        .await
        .map_err(|err| Error::msg(format!("source spec '{}': {err}", connector.source)))?;
    let worker = DuckDbWorker::in_memory()?;
    let mut runtime = RuntimeCoordinator::new();
    let mut total_committed_batches = 0;
    let mut total_committed_files = 0;
    let mut tables_processed = 0;

    for table_entry in &connector.tables {
        if matches!(args.batch_limit, Some(limit) if total_committed_batches >= limit) {
            break;
        }

        let table_mode = parse_table_mode(&table_entry.table_mode)?;
        let table_id = connector_table_id(table_entry);
        let resume_from = state
            .last_durable_checkpoint_for_table(&table_id)
            .await?
            .map(|checkpoint| checkpoint.checkpoint);

        let sink = build_sink_from_config(
            &destination_config,
            catalog_config.as_ref(),
            table_entry,
            &table_id,
        )
        .map_err(|err| Error::msg(format!("build sink '{}': {err}", connector.destination)))?;
        // connector run uses a single global batch limit across all configured tables.
        let remaining_batch_limit = args
            .batch_limit
            .map(|limit| limit.saturating_sub(total_committed_batches));

        let mut session = source
            .open_capture(OpenCaptureRequest {
                table: SourceTableSelection {
                    table_id: table_id.clone(),
                    source_schema: table_entry.source_schema.clone(),
                    source_table: table_entry.source_table.clone(),
                    table_mode,
                },
                resume_from,
            })
            .await
            .map_err(|err| {
                Error::msg(format!(
                    "open capture for '{}.{}': {err}",
                    table_entry.destination_namespace, table_entry.destination_table
                ))
            })?;
        tables_processed += 1;

        let table_result = async {
            let mut committed_batches = 0;
            let mut committed_files = 0;

            loop {
                if let Some(limit) = remaining_batch_limit {
                    if committed_batches >= limit {
                        break;
                    }
                }

                match runtime.try_admit(&table_id) {
                    IntakeDecision::Admitted => {}
                    IntakeDecision::Paused(reason) => {
                        return Err(Error::msg(format!("runtime intake paused: {reason}")));
                    }
                }

                let batch = match session.poll_batch(BatchRequest::default()).await? {
                    BatchPoll::Batch(batch) => batch,
                    BatchPoll::Idle => {
                        idle_backoff_sleep().await;
                        continue;
                    }
                    BatchPoll::Exhausted => {
                        runtime.clear_in_memory_batch(&table_id);
                        break;
                    }
                };

                let materialized = worker.materialize(batch).await?;
                runtime.clear_in_memory_batch(&table_id);
                runtime.record_durable_pending_batch(&table_id);

                let manifest = materialized.manifest;
                let batch_id = state.register_batch(manifest.clone()).await?;
                state
                    .record_files(batch_id.clone(), state_files_for_manifest(&manifest))
                    .await?;

                let idempotency_key = first_attempt_key(&batch_id);
                let destination_uri = sink.destination_uri().to_string();
                let prepared = sink
                    .prepare_commit(SinkCommitRequest {
                        batch_id: batch_id.clone(),
                        destination_uri: destination_uri.clone(),
                        manifest: manifest.clone(),
                        idempotency_key: idempotency_key.clone(),
                    })
                    .await?;

                let attempt = state
                    .begin_commit(
                        batch_id.clone(),
                        StateCommitRequest {
                            destination_uri,
                            snapshot: StateSnapshotRef {
                                uri: prepared.snapshot.uri.clone(),
                            },
                            actor: "iceflow-cli".to_string(),
                        },
                    )
                    .await?;

                if attempt.idempotency_key != idempotency_key.as_str() {
                    return Err(Error::msg(
                        "state store idempotency key drifted from CLI expectation",
                    ));
                }

                let committed = sink.commit(prepared).await?;
                state
                    .resolve_commit(attempt.id.clone(), AttemptResolution::Committed)
                    .await?;
                state
                    .link_checkpoint_pending(
                        batch_id.clone(),
                        checkpoint_ref(
                            source_spec.source_id.clone(),
                            manifest.source_checkpoint_end.clone(),
                        ),
                        StateSnapshotRef {
                            uri: committed.snapshot.uri.clone(),
                        },
                    )
                    .await?;

                runtime.clear_durable_pending_batch(&table_id);
                match runtime.checkpoint_decision(&table_id) {
                    CheckpointDecision::Advanced => {}
                    CheckpointDecision::Blocked(reason) => {
                        return Err(Error::msg(format!(
                            "checkpoint remained blocked after commit resolution: {reason}"
                        )));
                    }
                }

                session
                    .checkpoint(SourceCheckpointAck {
                        source_id: source_spec.source_id.clone(),
                        checkpoint: manifest.source_checkpoint_end.clone(),
                        snapshot_uri: committed.snapshot.uri.clone(),
                    })
                    .await?;
                state
                    .mark_checkpoint_durable(
                        batch_id,
                        state_checkpoint_ack(
                            source_spec.source_id.clone(),
                            manifest.source_checkpoint_end.clone(),
                            StateSnapshotRef {
                                uri: committed.snapshot.uri.clone(),
                            },
                        ),
                    )
                    .await?;

                committed_batches += 1;
                committed_files += manifest.file_set.len();
            }

            Ok::<TableRunStats, anyhow::Error>(TableRunStats {
                committed_batches,
                committed_files,
            })
        }
        .await;

        let table_stats = finalize_run_result(table_result, session.close().await)?;
        total_committed_batches += table_stats.committed_batches;
        total_committed_files += table_stats.committed_files;
    }

    Ok(RunReport {
        tables_processed,
        total_committed_batches,
        total_committed_files,
    })
}

fn infer_config_root(connector_config: &Path) -> Result<PathBuf> {
    let Some(connectors_dir) = connector_config.parent() else {
        return Err(Error::msg(
            "--connector must be nested under <config-root>/connectors/",
        ));
    };
    let Some(config_root) = connectors_dir.parent() else {
        return Err(Error::msg(
            "--connector must be nested under <config-root>/connectors/",
        ));
    };

    if connectors_dir.as_os_str().is_empty()
        || config_root.as_os_str().is_empty()
        || connectors_dir.file_name().and_then(|name| name.to_str()) != Some("connectors")
    {
        return Err(Error::msg(
            "--connector must be nested under <config-root>/connectors/",
        ));
    }

    Ok(config_root.to_path_buf())
}

fn connector_name(connector_config: &Path) -> Result<String> {
    connector_config
        .file_stem()
        .and_then(|value| value.to_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| Error::msg("connector file name must have a valid stem"))
}

fn resolve_connector_state_path(connector_config: &Path, config_root: &Path) -> Result<PathBuf> {
    let connector_stem = connector_config
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| Error::msg("connector file name must have a valid stem"))?;

    Ok(config_root
        .join(".iceflow")
        .join("state")
        .join(format!("{connector_stem}.sqlite3")))
}

async fn open_connector_state(
    connector_config: &Path,
    config_root: &Path,
) -> Result<SqliteStateStore> {
    let path = resolve_connector_state_path(connector_config, config_root)?;
    SqliteStateStore::open_persistent(path).await
}

fn parse_table_mode(value: &str) -> Result<TableMode> {
    match value {
        "append_only" => Ok(TableMode::AppendOnly),
        "keyed_upsert" => Ok(TableMode::KeyedUpsert),
        other => Err(Error::msg(format!("unsupported table_mode: {other}"))),
    }
}

enum CatalogValidation {
    Resolved(String),
    Unresolved,
    Conflict,
}

fn validate_catalog_configuration(
    config_root: &Path,
    connector: &ConnectorConfig,
    destination: &DestinationConfig,
    errors: &mut Vec<String>,
) -> Result<CatalogValidation> {
    match resolve_catalog_name(connector, destination) {
        Ok(Some(name)) => {
            validate_catalog_file(config_root, &name, errors);
            Ok(CatalogValidation::Resolved(name))
        }
        Ok(None) => Ok(CatalogValidation::Unresolved),
        Err(err) => {
            errors.push(err.to_string());
            for catalog_name in referenced_catalog_names(connector, destination) {
                validate_catalog_file(config_root, &catalog_name, errors);
            }
            Ok(CatalogValidation::Conflict)
        }
    }
}

fn referenced_catalog_names(
    connector: &ConnectorConfig,
    destination: &DestinationConfig,
) -> BTreeSet<String> {
    connector
        .catalog
        .iter()
        .chain(destination.catalog.iter())
        .cloned()
        .collect()
}

fn validate_catalog_file(config_root: &Path, catalog_name: &str, errors: &mut Vec<String>) {
    let catalog_path = config_root
        .join("catalogs")
        .join(format!("{catalog_name}.toml"));
    if let Err(err) = load_catalog_config(&catalog_path) {
        errors.push(format!("catalog '{catalog_name}': {err}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_args_parse_derives_config_root_from_nested_connector_path() -> Result<()> {
        let parsed = CheckArgs::parse(&[
            "--connector".to_string(),
            "fixtures/config_samples/connectors/orders_append.toml".to_string(),
        ])?;

        assert_eq!(parsed.config_root, PathBuf::from("fixtures/config_samples"));
        Ok(())
    }

    #[test]
    fn check_args_parse_rejects_bare_connector_filename() {
        let err = CheckArgs::parse(&["--connector".to_string(), "orders_append.toml".to_string()])
            .expect_err("bare connector path should fail");

        assert_eq!(
            err.to_string(),
            "--connector must be nested under <config-root>/connectors/"
        );
    }

    #[test]
    fn check_args_parse_rejects_single_component_connector_path() {
        let err = CheckArgs::parse(&[
            "--connector".to_string(),
            "connectors/orders_append.toml".to_string(),
        ])
        .expect_err("single-component connector path should fail");

        assert_eq!(
            err.to_string(),
            "--connector must be nested under <config-root>/connectors/"
        );
    }

    #[test]
    fn run_args_parse_derives_config_root_and_batch_limit() -> Result<()> {
        let parsed = RunArgs::parse(&[
            "--connector".to_string(),
            "fixtures/config_samples/connectors/orders_append.toml".to_string(),
            "--batch-limit".to_string(),
            "2".to_string(),
        ])?;

        assert_eq!(
            parsed.connector_config,
            PathBuf::from("fixtures/config_samples/connectors/orders_append.toml")
        );
        assert_eq!(parsed.config_root, PathBuf::from("fixtures/config_samples"));
        assert_eq!(parsed.batch_limit, Some(2));
        Ok(())
    }

    #[test]
    fn run_args_parse_rejects_zero_batch_limit() {
        let err = RunArgs::parse(&[
            "--connector".to_string(),
            "fixtures/config_samples/connectors/orders_append.toml".to_string(),
            "--batch-limit".to_string(),
            "0".to_string(),
        ])
        .expect_err("zero batch limit should fail");

        assert_eq!(err.to_string(), "--batch-limit must be greater than zero");
    }

    #[test]
    fn run_args_parse_rejects_unknown_argument() {
        let err = RunArgs::parse(&[
            "--connector".to_string(),
            "fixtures/config_samples/connectors/orders_append.toml".to_string(),
            "--bogus".to_string(),
        ])
        .expect_err("unknown connector run argument should fail");

        assert_eq!(err.to_string(), "unknown connector run argument: --bogus");
    }

    #[test]
    fn resolve_connector_state_path_is_stable_under_config_root() -> Result<()> {
        let path = resolve_connector_state_path(
            Path::new("/tmp/config/connectors/snowflake_customer_state_append.toml"),
            Path::new("/tmp/config"),
        )?;

        assert_eq!(
            path,
            PathBuf::from("/tmp/config/.iceflow/state/snowflake_customer_state_append.sqlite3")
        );
        Ok(())
    }
}
