use anyhow::Result;
use async_trait::async_trait;
use iceflow_types::{CheckpointId, LogicalMutation, TableId, TableMode};

#[allow(dead_code)] // Session implementations will own phase internally once live sources arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapturePhase {
    Snapshot,
    Streaming,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTableSelection {
    pub table_id: TableId,
    pub source_schema: String,
    pub source_table: String,
    pub table_mode: TableMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCaptureRequest {
    pub table: SourceTableSelection,
    pub resume_from: Option<CheckpointId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BatchRequest {
    pub max_records: Option<u64>,
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceBatch {
    pub batch_label: Option<String>,
    pub checkpoint_start: Option<CheckpointId>,
    pub checkpoint_end: CheckpointId,
    pub records: Vec<LogicalMutation>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BatchPoll {
    Batch(SourceBatch),
    Idle,
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointAck {
    pub source_id: String,
    pub checkpoint: CheckpointId,
    pub snapshot_uri: String,
}

#[async_trait]
pub trait SourceCaptureSession: Send {
    async fn poll_batch(&mut self, req: BatchRequest) -> Result<BatchPoll>;
    async fn checkpoint(&mut self, ack: CheckpointAck) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}
