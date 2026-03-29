use anyhow::Result;
use iceflow_state::SnapshotRef;
use iceflow_types::{
    structured_key_identity, BatchId, BatchManifest, LogicalMutation, Operation, StructuredKey,
    TableMode,
};
use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for IdempotencyKey {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for IdempotencyKey {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl Display for IdempotencyKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    pub batch_id: BatchId,
    pub destination_uri: String,
    pub manifest: BatchManifest,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitLocator {
    pub destination_uri: String,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedCommit {
    pub request: CommitRequest,
    pub snapshot_id: String,
    pub snapshot: SnapshotRef,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyedUpsertPlan {
    pub equality_delete_keys: Vec<StructuredKey>,
    pub append_rows: Vec<LogicalMutation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOutcome {
    pub snapshot_id: String,
    pub snapshot: SnapshotRef,
    pub idempotency_key: IdempotencyKey,
    pub destination_uri: String,
}

impl CommitOutcome {
    pub fn attempt(&self) -> CommitLocator {
        CommitLocator {
            destination_uri: self.destination_uri.clone(),
            idempotency_key: self.idempotency_key.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub snapshot_id: String,
    pub snapshot: SnapshotRef,
    pub batch_id: BatchId,
    pub destination_uri: String,
    pub idempotency_key: IdempotencyKey,
    pub committed_files: Vec<String>,
    pub record_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    Found(CommitOutcome),
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedOutcome {
    Committed(CommitOutcome),
    NotCommitted,
    Ambiguous,
}

#[allow(async_fn_in_trait)]
pub trait Sink {
    async fn prepare_commit(&self, req: CommitRequest) -> Result<PreparedCommit>;
    async fn commit(&self, prepared: PreparedCommit) -> Result<CommitOutcome>;
    async fn lookup_commit(&self, key: &IdempotencyKey) -> Result<LookupResult>;
    async fn lookup_snapshot(&self, snapshot: &SnapshotRef) -> Result<Option<SnapshotMeta>>;
    async fn resolve_uncertain_commit(&self, attempt: &CommitLocator) -> Result<ResolvedOutcome>;
}

pub fn prepare_append_only_commit(req: CommitRequest) -> Result<PreparedCommit> {
    prepare_commit_for_mode(
        req,
        TableMode::AppendOnly,
        "sink v0 only supports append_only commits",
    )
}

pub fn prepare_keyed_upsert_commit(req: CommitRequest) -> Result<PreparedCommit> {
    prepare_commit_for_mode(
        req,
        TableMode::KeyedUpsert,
        "sink v0 only supports keyed_upsert commits",
    )
}

fn prepare_commit_for_mode(
    mut req: CommitRequest,
    expected_mode: TableMode,
    mode_error: &str,
) -> Result<PreparedCommit> {
    if req.batch_id != req.manifest.batch_id {
        anyhow::bail!("commit request batch_id must match manifest batch_id");
    }
    if req.manifest.table_mode != expected_mode {
        anyhow::bail!("{}", mode_error);
    }
    if req.destination_uri.trim().is_empty() {
        anyhow::bail!("destination_uri is required");
    }
    if req.idempotency_key.as_str().trim().is_empty() {
        anyhow::bail!("idempotency_key is required");
    }

    req.destination_uri = trim_trailing_slash(&req.destination_uri);
    let snapshot_id = format!(
        "snapshot-{}",
        stable_hash([
            req.batch_id.to_string(),
            req.destination_uri.clone(),
            req.idempotency_key.to_string(),
            req.manifest.content_hash.clone(),
            req.manifest.schema_fingerprint.clone(),
        ])
    );
    let snapshot = SnapshotRef {
        uri: format!("{}/snapshots/{}", req.destination_uri, snapshot_id),
    };

    Ok(PreparedCommit {
        request: req,
        snapshot_id,
        snapshot,
    })
}

pub fn plan_keyed_upsert_commit(records: &[LogicalMutation]) -> Result<KeyedUpsertPlan> {
    if records.is_empty() {
        anyhow::bail!("keyed_upsert plan requires at least one record");
    }
    if records
        .iter()
        .any(|record| record.table_mode != TableMode::KeyedUpsert)
    {
        anyhow::bail!("keyed_upsert plan requires keyed_upsert records");
    }
    let mut seen_keys = BTreeSet::new();
    for record in records {
        if !seen_keys.insert(structured_key_identity(&record.key)) {
            anyhow::bail!("keyed_upsert plan requires normalized latest-per-key records");
        }
    }

    Ok(KeyedUpsertPlan {
        equality_delete_keys: records.iter().map(|row| row.key.clone()).collect(),
        append_rows: records
            .iter()
            .filter(|row| row.op != Operation::Delete)
            .cloned()
            .collect(),
    })
}

pub fn snapshot_meta(prepared: &PreparedCommit) -> SnapshotMeta {
    snapshot_meta_with_files(
        prepared,
        prepared
            .request
            .manifest
            .file_set
            .iter()
            .map(|file| file.file_uri.clone())
            .collect(),
    )
}

pub fn snapshot_meta_with_files(
    prepared: &PreparedCommit,
    committed_files: Vec<String>,
) -> SnapshotMeta {
    SnapshotMeta {
        snapshot_id: prepared.snapshot_id.clone(),
        snapshot: prepared.snapshot.clone(),
        batch_id: prepared.request.batch_id.clone(),
        destination_uri: prepared.request.destination_uri.clone(),
        idempotency_key: prepared.request.idempotency_key.clone(),
        committed_files,
        record_count: prepared.request.manifest.record_count,
    }
}

pub fn commit_outcome(prepared: &PreparedCommit) -> CommitOutcome {
    CommitOutcome {
        snapshot_id: prepared.snapshot_id.clone(),
        snapshot: prepared.snapshot.clone(),
        idempotency_key: prepared.request.idempotency_key.clone(),
        destination_uri: prepared.request.destination_uri.clone(),
    }
}

fn trim_trailing_slash(value: &str) -> String {
    value.trim_end_matches('/').to_string()
}

pub(crate) fn scoped_commit_key(destination_uri: &str, key: &IdempotencyKey) -> String {
    format!("{}::{}", trim_trailing_slash(destination_uri), key.as_str())
}

pub(crate) fn stable_hash(parts: impl IntoIterator<Item = String>) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}
