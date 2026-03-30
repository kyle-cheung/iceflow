use anyhow::Result;
use iceflow_types::{CheckpointId, LogicalMutation, SourceClass, TableMode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpec {
    pub source_id: String,
    pub fixture_dir: String,
    pub source_class: SourceClass,
    pub table_id: String,
    pub table_mode: TableMode,
    pub ordering_field: Option<String>,
    pub batch_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckReport {
    pub fixture_dir: String,
    pub batch_count: usize,
    pub record_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverReport {
    pub fixture_dir: String,
    pub batch_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRequest {
    pub batch_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRef {
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointAck {
    pub source_id: String,
    pub checkpoint: CheckpointId,
    pub snapshot: SnapshotRef,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceBatch {
    pub batch_file: String,
    pub records: Vec<LogicalMutation>,
}

#[allow(async_fn_in_trait)]
pub trait SourceAdapter {
    async fn spec(&self) -> SourceSpec;
    async fn check(&self) -> Result<CheckReport>;
    async fn discover(&self) -> Result<DiscoverReport>;
    async fn snapshot(&self, req: SnapshotRequest) -> Result<Option<SourceBatch>>;
    async fn checkpoint(&self, ack: CheckpointAck) -> Result<()>;
}
