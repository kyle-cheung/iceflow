use anyhow::{Error, Result};
use greytl_runtime::{CheckpointDecision, IntakeDecision, RuntimeCoordinator};
use greytl_sink::{
    CommitRequest as SinkCommitRequest, FilesystemSink, IdempotencyKey, PolarisSink, Sink,
};
use greytl_source::{
    CheckpointAck as SourceCheckpointAck, FileSource, SnapshotRef as SourceSnapshotRef,
    SnapshotRequest, SourceAdapter,
};
use greytl_state::{
    checkpoint_ack as state_checkpoint_ack, checkpoint_ref, AttemptResolution, BatchFile,
    CommitRequest as StateCommitRequest, SnapshotRef as StateSnapshotRef, SqliteStateStore,
    StateStore,
};
use greytl_types::{BatchId, BatchManifest};
use greytl_worker_duckdb::DuckDbWorker;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub workload: String,
    pub destination_uri: String,
    pub sink: SinkKind,
    pub catalog_uri: Option<String>,
    pub catalog_name: Option<String>,
    pub namespace: Option<String>,
    pub batch_limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkKind {
    Filesystem,
    Polaris,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    pub workload: String,
    pub committed_batches: usize,
    pub committed_files: usize,
    pub last_snapshot_uri: Option<String>,
    pub durable_checkpoint: Option<String>,
}

#[derive(Debug, Clone)]
enum ConfiguredSink {
    Filesystem(FilesystemSink),
    Polaris(PolarisSink),
}

impl Args {
    pub fn parse(args: Vec<String>) -> Result<Self> {
        let mut workload = None;
        let mut destination_uri = None;
        let mut sink = SinkKind::Filesystem;
        let mut catalog_uri = None;
        let mut catalog_name = None;
        let mut namespace = None;
        let mut batch_limit = None;
        let mut index = 0;

        while index < args.len() {
            match args[index].as_str() {
                "--workload" => {
                    index += 1;
                    workload = args.get(index).cloned();
                }
                "--destination-uri" => {
                    index += 1;
                    destination_uri = args.get(index).cloned();
                }
                "--sink" => {
                    index += 1;
                    sink = SinkKind::parse(
                        args.get(index)
                            .ok_or_else(|| Error::msg("--sink requires a value"))?,
                    )?;
                }
                "--catalog-uri" => {
                    index += 1;
                    catalog_uri = args.get(index).cloned();
                }
                "--catalog" => {
                    index += 1;
                    catalog_name = args.get(index).cloned();
                }
                "--namespace" => {
                    index += 1;
                    namespace = args.get(index).cloned();
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
                    return Err(Error::msg(format!("unknown run argument: {other}")));
                }
            }
            index += 1;
        }

        Ok(Self {
            workload: workload.ok_or_else(|| Error::msg("--workload is required"))?,
            destination_uri: destination_uri
                .ok_or_else(|| Error::msg("--destination-uri is required"))?,
            sink,
            catalog_uri,
            catalog_name,
            namespace,
            batch_limit,
        })
    }
}

impl SinkKind {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "filesystem" => Ok(Self::Filesystem),
            "polaris" => Ok(Self::Polaris),
            other => Err(Error::msg(format!("unsupported sink: {other}"))),
        }
    }
}

impl ConfiguredSink {
    async fn prepare_commit(&self, req: SinkCommitRequest) -> Result<greytl_sink::PreparedCommit> {
        match self {
            Self::Filesystem(sink) => sink.prepare_commit(req).await,
            Self::Polaris(sink) => sink.prepare_commit(req).await,
        }
    }

    async fn commit(
        &self,
        prepared: greytl_sink::PreparedCommit,
    ) -> Result<greytl_sink::CommitOutcome> {
        match self {
            Self::Filesystem(sink) => sink.commit(prepared).await,
            Self::Polaris(sink) => sink.commit(prepared).await,
        }
    }
}

pub fn execute_blocking(args: Args) -> Result<RunReport> {
    crate::block_on(execute(args))
}

pub async fn execute(args: Args) -> Result<RunReport> {
    let fixture_dir = fixture_dir_for_workload(&args.workload)?;
    let source = FileSource::from_fixture_dir(fixture_dir);
    let spec = source.spec().await;
    let worker = DuckDbWorker::in_memory()?;
    let state = SqliteStateStore::new().await?;
    let mut runtime = RuntimeCoordinator::new();
    let sink = build_sink(&args, &spec.table_id)?;

    let table_id = spec.table_id.clone().into();
    let batch_limit = args.batch_limit.unwrap_or(usize::MAX);
    let mut report = RunReport {
        workload: args.workload.clone(),
        committed_batches: 0,
        committed_files: 0,
        last_snapshot_uri: None,
        durable_checkpoint: None,
    };

    for batch_index in 1..=batch_limit {
        match runtime.try_admit(&table_id) {
            IntakeDecision::Admitted => {}
            IntakeDecision::Paused(reason) => {
                return Err(Error::msg(format!("runtime intake paused: {reason}")));
            }
        }

        let Some(batch) = source.snapshot(SnapshotRequest { batch_index }).await? else {
            runtime.clear_in_memory_batch(&table_id);
            break;
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
        let prepared = sink
            .prepare_commit(SinkCommitRequest {
                batch_id: batch_id.clone(),
                destination_uri: args.destination_uri.clone(),
                manifest: manifest.clone(),
                idempotency_key: idempotency_key.clone(),
            })
            .await?;

        let attempt = state
            .begin_commit(
                batch_id.clone(),
                StateCommitRequest {
                    destination_uri: args.destination_uri.clone(),
                    snapshot: StateSnapshotRef {
                        uri: prepared.snapshot.uri.clone(),
                    },
                    actor: "greytl-cli".to_string(),
                },
            )
            .await?;

        if attempt.idempotency_key != idempotency_key.as_str() {
            return Err(Error::msg("state store idempotency key drifted from CLI expectation"));
        }

        let committed = sink.commit(prepared).await?;
        state
            .resolve_commit(attempt.id.clone(), AttemptResolution::Committed)
            .await?;
        state
            .link_checkpoint_pending(
                batch_id.clone(),
                checkpoint_ref(spec.source_id.clone(), manifest.source_checkpoint_end.clone()),
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

        source
            .checkpoint(SourceCheckpointAck {
                source_id: spec.source_id.clone(),
                checkpoint: manifest.source_checkpoint_end.clone(),
                snapshot: SourceSnapshotRef {
                    uri: committed.snapshot.uri.clone(),
                },
            })
            .await?;
        state
            .mark_checkpoint_durable(
                batch_id.clone(),
                state_checkpoint_ack(
                    spec.source_id.clone(),
                    manifest.source_checkpoint_end.clone(),
                    StateSnapshotRef {
                        uri: committed.snapshot.uri.clone(),
                    },
                ),
            )
            .await?;

        report.committed_batches += 1;
        report.committed_files += manifest.file_set.len();
        report.last_snapshot_uri = Some(committed.snapshot.uri.clone());
        report.durable_checkpoint = state
            .durable_checkpoint(batch_id)
            .await?
            .map(|checkpoint| checkpoint.checkpoint.to_string());
    }

    Ok(report)
}

fn build_sink(args: &Args, table_id: &str) -> Result<ConfiguredSink> {
    match args.sink {
        SinkKind::Filesystem => Ok(ConfiguredSink::Filesystem(FilesystemSink::new(
            file_uri_path(&args.destination_uri)?,
        ))),
        SinkKind::Polaris => {
            let catalog_uri = args
                .catalog_uri
                .clone()
                .ok_or_else(|| Error::msg("--catalog-uri is required for --sink polaris"))?;
            let catalog_name = args
                .catalog_name
                .clone()
                .ok_or_else(|| Error::msg("--catalog is required for --sink polaris"))?;
            let namespace = args.namespace.clone().unwrap_or_else(|| table_id.to_string());
            Ok(ConfiguredSink::Polaris(PolarisSink::new(
                catalog_uri,
                catalog_name,
                namespace,
                args.destination_uri.clone(),
            )))
        }
    }
}

fn fixture_dir_for_workload(workload: &str) -> Result<&'static str> {
    match workload {
        "append_only.orders_events" => Ok("fixtures/reference_workload_v0/orders_events"),
        "keyed_upsert.customer_state" => Ok("fixtures/reference_workload_v0/customer_state"),
        other => Err(Error::msg(format!("unsupported workload: {other}"))),
    }
}

fn first_attempt_key(batch_id: &BatchId) -> IdempotencyKey {
    IdempotencyKey::from(format!("{}:1", batch_id.as_str()))
}

fn state_files_for_manifest(manifest: &BatchManifest) -> Vec<BatchFile> {
    manifest
        .file_set
        .iter()
        .map(|file| BatchFile {
            batch_id: manifest.batch_id.clone(),
            file_uri: file.file_uri.clone(),
            file_kind: file.file_kind.clone(),
            content_hash: file.content_hash.clone(),
            file_size_bytes: file.file_size_bytes,
            record_count: file.record_count,
            created_at: file.created_at.clone(),
        })
        .collect()
}

fn file_uri_path(uri: &str) -> Result<String> {
    uri.strip_prefix("file://")
        .map(str::to_string)
        .ok_or_else(|| Error::msg(format!("filesystem path must use file://, got {uri}")))
}
