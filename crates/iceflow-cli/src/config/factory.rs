use crate::config::loader::load_catalog_config;
use crate::config::types::{
    CatalogConfig, ConnectorConfig, DestinationConfig, SourceConfig, TableEntry,
};
use anyhow::{Error, Result};
use async_trait::async_trait;
use iceflow_sink::{
    CommitLocator, CommitOutcome, CommitRequest, FilesystemSink, IdempotencyKey, LookupResult,
    PolarisSink, PreparedCommit, ResolvedOutcome, Sink, SnapshotMeta,
};
use iceflow_source::{
    validate_source_spec, BatchPoll, BatchRequest, CheckpointAck, FileSource, OpenCaptureRequest,
    SourceAdapter, SourceCaptureSession, SourceCheckReport, SourceSpec,
};
use iceflow_state::SnapshotRef;
use iceflow_types::{SourceClass, TableId};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub enum ConfiguredSink {
    Filesystem {
        sink: FilesystemSink,
        destination_uri: String,
    },
    Polaris {
        sink: PolarisSink,
        destination_uri: String,
    },
}

impl ConfiguredSink {
    pub fn destination_uri(&self) -> &str {
        match self {
            Self::Filesystem {
                destination_uri, ..
            }
            | Self::Polaris {
                destination_uri, ..
            } => destination_uri,
        }
    }
}

impl Sink for ConfiguredSink {
    async fn prepare_commit(&self, req: CommitRequest) -> Result<PreparedCommit> {
        match self {
            Self::Filesystem { sink, .. } => sink.prepare_commit(req).await,
            Self::Polaris { sink, .. } => sink.prepare_commit(req).await,
        }
    }

    async fn commit(&self, prepared: PreparedCommit) -> Result<CommitOutcome> {
        match self {
            Self::Filesystem { sink, .. } => sink.commit(prepared).await,
            Self::Polaris { sink, .. } => sink.commit(prepared).await,
        }
    }

    async fn lookup_commit(&self, key: &IdempotencyKey) -> Result<LookupResult> {
        match self {
            Self::Filesystem { sink, .. } => sink.lookup_commit(key).await,
            Self::Polaris { sink, .. } => sink.lookup_commit(key).await,
        }
    }

    async fn lookup_snapshot(&self, snapshot: &SnapshotRef) -> Result<Option<SnapshotMeta>> {
        match self {
            Self::Filesystem { sink, .. } => sink.lookup_snapshot(snapshot).await,
            Self::Polaris { sink, .. } => sink.lookup_snapshot(snapshot).await,
        }
    }

    async fn resolve_uncertain_commit(&self, attempt: &CommitLocator) -> Result<ResolvedOutcome> {
        match self {
            Self::Filesystem { sink, .. } => sink.resolve_uncertain_commit(attempt).await,
            Self::Polaris { sink, .. } => sink.resolve_uncertain_commit(attempt).await,
        }
    }
}

pub fn build_source_from_config(
    config: &SourceConfig,
    config_base: &Path,
) -> Result<Box<dyn SourceAdapter>> {
    match config.kind.as_str() {
        "file" => {
            let fixture_root_label = required_property(&config.properties, "fixture_root")?;
            let fixture_root = resolve_config_path(config_base, fixture_root_label);
            Ok(Box::new(RoutedFileSource::new(
                fixture_root,
                fixture_root_label.to_string(),
            )))
        }
        other => Err(Error::msg(format!("unsupported source kind: {other}"))),
    }
}

pub fn build_sink_from_config(
    destination: &DestinationConfig,
    catalog: Option<&CatalogConfig>,
    table: &TableEntry,
    _table_id: &TableId,
) -> Result<ConfiguredSink> {
    match destination.kind.as_str() {
        "filesystem" => {
            let root_uri = required_property(&destination.properties, "root_uri")?;
            let destination_uri = normalize_filesystem_destination_uri(root_uri);
            Ok(ConfiguredSink::Filesystem {
                sink: FilesystemSink::new(filesystem_root_path(root_uri)),
                destination_uri,
            })
        }
        "polaris" => {
            let catalog = catalog.ok_or_else(|| {
                Error::msg("polaris destination requires a resolved catalog config")
            })?;
            let warehouse_uri = required_property(&destination.properties, "warehouse_uri")?;
            let mut sink = PolarisSink::new(
                catalog.endpoint.clone(),
                catalog.warehouse.clone(),
                table.destination_namespace.clone(),
                warehouse_uri.to_string(),
            );

            if let Some(token) = catalog.properties.get("bearer_token") {
                sink = sink.with_bearer_token(token.clone());
            } else {
                match (
                    catalog.properties.get("client_id"),
                    catalog.properties.get("client_secret"),
                ) {
                    (Some(client_id), Some(client_secret)) => {
                        sink =
                            sink.with_client_credentials(client_id.clone(), client_secret.clone());
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        return Err(Error::msg(
                            "catalog config must set both client_id and client_secret together",
                        ));
                    }
                    (None, None) => {}
                }
            }

            Ok(ConfiguredSink::Polaris {
                sink,
                destination_uri: warehouse_uri.to_string(),
            })
        }
        other => Err(Error::msg(format!("unsupported destination kind: {other}"))),
    }
}

pub fn resolve_catalog_name(
    connector: &ConnectorConfig,
    destination: &DestinationConfig,
) -> Result<Option<String>> {
    match (&connector.catalog, &destination.catalog) {
        (Some(left), Some(right)) if left != right => Err(Error::msg(format!(
            "connector catalog '{}' does not match destination catalog '{}'",
            left, right
        ))),
        (Some(name), _) => Ok(Some(name.clone())),
        (None, Some(name)) => Ok(Some(name.clone())),
        (None, None) => Ok(None),
    }
}

pub fn load_optional_catalog_config(
    config_root: &Path,
    connector: &ConnectorConfig,
    destination: &DestinationConfig,
) -> Result<Option<CatalogConfig>> {
    let Some(catalog_name) = resolve_catalog_name(connector, destination)? else {
        return Ok(None);
    };

    let catalog_path = config_root
        .join("catalogs")
        .join(format!("{catalog_name}.toml"));
    load_catalog_config(&catalog_path).map(Some)
}

pub fn connector_table_id(table: &TableEntry) -> TableId {
    TableId::new(format!(
        "{}.{}",
        table.destination_namespace, table.destination_table
    ))
}

#[derive(Debug, Clone)]
struct RoutedFileSource {
    fixture_root: PathBuf,
    source_id: String,
}

impl RoutedFileSource {
    fn new(fixture_root: PathBuf, source_label: String) -> Self {
        let source_label = source_label.replace('\\', "/");
        Self {
            fixture_root,
            source_id: format!("file.config.{source_label}"),
        }
    }
}

#[async_trait]
impl SourceAdapter for RoutedFileSource {
    async fn spec(&self) -> Result<SourceSpec> {
        let spec = SourceSpec {
            source_id: self.source_id.clone(),
            source_class: SourceClass::FileOrObjectDrop,
        };
        validate_source_spec(&spec)?;
        Ok(spec)
    }

    async fn check(&self) -> Result<SourceCheckReport> {
        if !self.fixture_root.exists() {
            return Err(Error::msg(format!(
                "fixture_root does not exist: {}",
                self.fixture_root.display()
            )));
        }

        let mut report = FileSource::from_fixture_dir(self.fixture_root.clone())
            .check()
            .await?;
        report.details.insert(
            "fixture_root".to_string(),
            self.fixture_root.display().to_string(),
        );
        Ok(report)
    }

    async fn open_capture(
        &self,
        req: OpenCaptureRequest,
    ) -> Result<Box<dyn SourceCaptureSession + Send>> {
        if req.table.source_table.trim().is_empty() {
            return Err(Error::msg("source_table is required"));
        }

        let delegated_source =
            FileSource::from_fixture_dir(self.fixture_root.join(&req.table.source_table));
        let delegated_spec = delegated_source.spec().await?;
        let delegated_req = OpenCaptureRequest {
            table: iceflow_source::SourceTableSelection {
                table_id: TableId::new(req.table.source_table.clone()),
                source_schema: req.table.source_schema,
                source_table: req.table.source_table,
                table_mode: req.table.table_mode,
            },
            resume_from: req.resume_from,
        };
        let session = delegated_source.open_capture(delegated_req).await?;

        Ok(Box::new(RoutedFileCaptureSession {
            outer_table_id: req.table.table_id,
            outer_source_id: self.source_id.clone(),
            inner_source_id: delegated_spec.source_id,
            inner: session,
        }))
    }
}

struct RoutedFileCaptureSession {
    outer_table_id: TableId,
    outer_source_id: String,
    inner_source_id: String,
    inner: Box<dyn SourceCaptureSession + Send>,
}

#[async_trait]
impl SourceCaptureSession for RoutedFileCaptureSession {
    async fn poll_batch(&mut self, req: BatchRequest) -> Result<BatchPoll> {
        match self.inner.poll_batch(req).await? {
            BatchPoll::Batch(mut batch) => {
                for record in &mut batch.records {
                    record.table_id = self.outer_table_id.clone();
                    record.source_id = self.outer_source_id.clone();
                }
                Ok(BatchPoll::Batch(batch))
            }
            other => Ok(other),
        }
    }

    async fn checkpoint(&mut self, ack: CheckpointAck) -> Result<()> {
        if ack.source_id != self.outer_source_id {
            return Err(Error::msg("checkpoint ack source_id does not match source"));
        }

        self.inner
            .checkpoint(CheckpointAck {
                source_id: self.inner_source_id.clone(),
                checkpoint: ack.checkpoint,
                snapshot_uri: ack.snapshot_uri,
            })
            .await
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}

fn required_property<'a>(properties: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> {
    properties
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| Error::msg(format!("missing required property '{key}'")))
}

fn resolve_config_path(base: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn filesystem_root_path(root_uri: &str) -> PathBuf {
    root_uri
        .strip_prefix("file://")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(root_uri))
}

fn normalize_filesystem_destination_uri(root_uri: &str) -> String {
    if root_uri.starts_with("file://") {
        root_uri.to_string()
    } else {
        format!("file://{}", Path::new(root_uri).display())
    }
}
