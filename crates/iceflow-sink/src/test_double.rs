use crate::commit_protocol::{
    commit_outcome, prepare_append_only_commit, prepare_keyed_upsert_commit, scoped_commit_key,
    snapshot_meta, CommitLocator, CommitOutcome, CommitRequest, IdempotencyKey, LookupResult,
    PreparedCommit, ResolvedOutcome, Sink, SnapshotMeta,
};
use anyhow::{Error, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkFailpoint {
    LoseAckOnce,
    FailCommitOnce,
    AmbiguousResolution,
}

#[derive(Debug, Clone)]
pub struct TestDoubleSink {
    inner: Arc<Mutex<TestDoubleState>>,
}

#[derive(Debug, Default)]
struct TestDoubleState {
    commits: BTreeMap<String, SnapshotMeta>,
    replay_identities: BTreeMap<String, String>,
    failpoint: Option<SinkFailpoint>,
    ambiguous: BTreeSet<String>,
}

impl TestDoubleSink {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestDoubleState::default())),
        }
    }

    pub fn with_failpoint(failpoint: SinkFailpoint) -> Self {
        let sink = Self::new();
        sink.set_failpoint(failpoint);
        sink
    }

    pub fn set_failpoint(&self, failpoint: SinkFailpoint) {
        let mut inner = self.inner.lock().expect("test double state lock");
        inner.failpoint = Some(failpoint);
    }
}

impl Default for TestDoubleSink {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(async_fn_in_trait)]
impl Sink for TestDoubleSink {
    async fn prepare_commit(&self, req: CommitRequest) -> Result<PreparedCommit> {
        match req.manifest.table_mode {
            iceflow_types::TableMode::AppendOnly => prepare_append_only_commit(req),
            iceflow_types::TableMode::KeyedUpsert => prepare_keyed_upsert_commit(req),
        }
    }

    async fn commit(&self, prepared: PreparedCommit) -> Result<CommitOutcome> {
        let key = scoped_commit_key(
            &prepared.request.destination_uri,
            &prepared.request.idempotency_key,
        );
        let outcome = commit_outcome(&prepared);
        let meta = snapshot_meta(&prepared);
        let replay_identity = prepared.request.manifest.replay_identity();

        let mut inner = self.inner.lock().expect("test double state lock");
        if let Some(existing) = inner.commits.get(&key) {
            if inner.replay_identities.get(&key) != Some(&replay_identity) {
                anyhow::bail!("idempotency key reused with different manifest");
            }
            return Ok(CommitOutcome {
                snapshot_id: existing.snapshot_id.clone(),
                snapshot: existing.snapshot.clone(),
                idempotency_key: prepared.request.idempotency_key,
                destination_uri: existing.destination_uri.clone(),
            });
        }

        match inner.failpoint.take() {
            Some(SinkFailpoint::LoseAckOnce) => {
                inner.commits.insert(key.clone(), meta);
                inner.replay_identities.insert(key, replay_identity);
                Err(Error::msg("commit ack lost"))
            }
            Some(SinkFailpoint::FailCommitOnce) => Err(Error::msg("commit failed")),
            Some(SinkFailpoint::AmbiguousResolution) => {
                inner.ambiguous.insert(key);
                Err(Error::msg("commit outcome ambiguous"))
            }
            None => {
                inner.commits.insert(key.clone(), meta);
                inner.replay_identities.insert(key, replay_identity);
                Ok(outcome)
            }
        }
    }

    async fn lookup_commit(&self, key: &IdempotencyKey) -> Result<LookupResult> {
        let inner = self.inner.lock().expect("test double state lock");
        let mut matching = inner
            .commits
            .values()
            .filter(|meta| meta.idempotency_key == *key)
            .cloned();
        let first = matching.next();
        if matching.next().is_some() {
            anyhow::bail!("idempotency key is ambiguous across destinations");
        }
        Ok(match first {
            Some(meta) => LookupResult::Found(CommitOutcome {
                snapshot_id: meta.snapshot_id.clone(),
                snapshot: meta.snapshot.clone(),
                idempotency_key: key.clone(),
                destination_uri: meta.destination_uri.clone(),
            }),
            None => LookupResult::NotFound,
        })
    }

    async fn lookup_snapshot(
        &self,
        snapshot: &iceflow_state::SnapshotRef,
    ) -> Result<Option<SnapshotMeta>> {
        let inner = self.inner.lock().expect("test double state lock");
        Ok(inner
            .commits
            .values()
            .find(|meta| meta.snapshot == *snapshot)
            .cloned())
    }

    async fn resolve_uncertain_commit(&self, attempt: &CommitLocator) -> Result<ResolvedOutcome> {
        let inner = self.inner.lock().expect("test double state lock");
        let scoped_key = scoped_commit_key(&attempt.destination_uri, &attempt.idempotency_key);
        if let Some(meta) = inner.commits.get(&scoped_key) {
            return Ok(ResolvedOutcome::Committed(CommitOutcome {
                snapshot_id: meta.snapshot_id.clone(),
                snapshot: meta.snapshot.clone(),
                idempotency_key: attempt.idempotency_key.clone(),
                destination_uri: meta.destination_uri.clone(),
            }));
        }
        if inner.ambiguous.contains(&scoped_key) {
            return Ok(ResolvedOutcome::Ambiguous);
        }
        Ok(ResolvedOutcome::NotCommitted)
    }
}
