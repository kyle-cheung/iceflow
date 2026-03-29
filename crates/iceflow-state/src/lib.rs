mod migrations;
mod reconcile;
mod recovery;
mod sqlite;

use anyhow::Result;
use chrono::{DateTime, Utc};
use iceflow_types::{BatchId, BatchManifest, CheckpointId, CommitAttemptId};

pub use sqlite::{SqliteStateStore, TestStateStore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRef {
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCheckpoint {
    pub source_id: String,
    pub checkpoint: CheckpointId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointAck {
    pub source_id: String,
    pub checkpoint: CheckpointId,
    pub snapshot: SnapshotRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    pub destination_uri: String,
    pub snapshot: SnapshotRef,
    pub actor: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAttempt {
    pub id: CommitAttemptId,
    pub batch_id: BatchId,
    pub attempt_no: u32,
    pub destination_uri: String,
    pub snapshot: SnapshotRef,
    pub actor: String,
    pub idempotency_key: String,
    pub status: AttemptStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchFile {
    pub batch_id: BatchId,
    pub file_uri: String,
    pub file_kind: String,
    pub content_hash: String,
    pub file_size_bytes: u64,
    pub record_count: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptResolution {
    Committed,
    Rejected,
    FailedRetryable,
    FailedTerminal,
    Unknown,
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptStatus {
    Started,
    Resolving,
    Unknown,
    Committed,
    Rejected,
    FailedRetryable,
    FailedTerminal,
    AmbiguousManual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchStatus {
    Registered,
    FilesWritten,
    CommitStarted,
    CommitUncertain,
    SchemaRevalidating,
    RetryReady,
    Committed,
    CheckpointPending,
    Checkpointed,
    Quarantined,
    FailedTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupAction {
    Deleted,
    Retained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineReason {
    OrderingViolation,
    SchemaViolation,
    MalformedBatch,
    AmbiguousResolution,
}

#[allow(async_fn_in_trait)]
pub trait StateStore {
    async fn register_batch(&self, manifest: BatchManifest) -> Result<BatchId>;
    async fn record_files(&self, batch_id: BatchId, files: Vec<BatchFile>) -> Result<()>;
    async fn begin_commit(&self, batch_id: BatchId, req: CommitRequest) -> Result<CommitAttempt>;
    async fn resolve_commit(
        &self,
        attempt_id: CommitAttemptId,
        resolution: AttemptResolution,
    ) -> Result<()>;
    async fn link_checkpoint_pending(
        &self,
        batch_id: BatchId,
        cp: SourceCheckpoint,
        snapshot: SnapshotRef,
    ) -> Result<()>;
    async fn mark_checkpoint_durable(&self, batch_id: BatchId, ack: CheckpointAck) -> Result<()>;
    async fn mark_quarantine(&self, batch_id: BatchId, reason: QuarantineReason) -> Result<()>;
    async fn list_recovery_candidates(&self) -> Result<Vec<BatchId>>;
    async fn list_orphan_candidates(&self) -> Result<Vec<BatchFile>>;
    async fn record_cleanup(&self, file: BatchFile, action: CleanupAction) -> Result<()>;
}

pub fn checkpoint_ref(source_id: impl Into<String>, checkpoint: CheckpointId) -> SourceCheckpoint {
    SourceCheckpoint {
        source_id: source_id.into(),
        checkpoint,
    }
}

pub fn checkpoint_ack(
    source_id: impl Into<String>,
    checkpoint: CheckpointId,
    snapshot: SnapshotRef,
) -> CheckpointAck {
    CheckpointAck {
        source_id: source_id.into(),
        checkpoint,
        snapshot,
    }
}
