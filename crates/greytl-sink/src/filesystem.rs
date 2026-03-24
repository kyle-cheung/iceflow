use crate::commit_protocol::{
    commit_outcome, prepare_append_only_commit, scoped_commit_key, snapshot_meta_with_files,
    stable_hash, CommitLocator, CommitOutcome, CommitRequest, IdempotencyKey, LookupResult,
    PreparedCommit, ResolvedOutcome, Sink, SnapshotMeta,
};
use anyhow::{Error, Result};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct FilesystemSink {
    root: PathBuf,
}

impl FilesystemSink {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    fn ensure_layout(&self) -> Result<()> {
        fs::create_dir_all(self.commits_dir())
            .map_err(|err| Error::msg(format!("{}: {err}", self.commits_dir().display())))?;
        fs::create_dir_all(self.snapshots_dir())
            .map_err(|err| Error::msg(format!("{}: {err}", self.snapshots_dir().display())))?;
        Ok(())
    }

    fn commits_dir(&self) -> PathBuf {
        self.root.join("commits")
    }

    fn snapshots_dir(&self) -> PathBuf {
        self.root.join("snapshots")
    }

    fn commit_path(&self, destination_uri: &str, key: &IdempotencyKey) -> PathBuf {
        let scope = scoped_commit_key(destination_uri, key);
        self.commits_dir()
            .join(format!("{}.txt", stable_hash([scope])))
    }

    fn snapshot_path(&self, snapshot_id: &str) -> PathBuf {
        self.snapshots_dir().join(format!("{snapshot_id}.txt"))
    }

    fn stage_batch_files(&self, prepared: &PreparedCommit) -> Result<Vec<String>> {
        let destination_root = destination_root(&prepared.request.destination_uri)?;
        let data_dir = destination_root.join("data");
        fs::create_dir_all(&data_dir)
            .map_err(|err| Error::msg(format!("{}: {err}", data_dir.display())))?;

        let mut staged_files = Vec::new();
        for (index, file) in prepared.request.manifest.file_set.iter().enumerate() {
            let source_path = file_uri_path(&file.file_uri)?;
            let extension = source_path
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or("bin");
            let staged_path =
                data_dir.join(format!("{}-{index:04}.{extension}", prepared.snapshot_id));
            let temp_path = data_dir.join(format!("{}-{index:04}.{extension}.tmp", prepared.snapshot_id));

            if temp_path.exists() {
                fs::remove_file(&temp_path).map_err(|err| {
                    Error::msg(format!("remove {} failed: {err}", temp_path.display()))
                })?;
            }
            fs::copy(&source_path, &temp_path).map_err(|err| {
                Error::msg(format!(
                    "copy {} -> {} failed: {err}",
                    source_path.display(),
                    temp_path.display()
                ))
            })?;
            if staged_path.exists() {
                fs::remove_file(&staged_path).map_err(|err| {
                    Error::msg(format!("remove {} failed: {err}", staged_path.display()))
                })?;
            }
            fs::rename(&temp_path, &staged_path).map_err(|err| {
                Error::msg(format!(
                    "rename {} -> {} failed: {err}",
                    temp_path.display(),
                    staged_path.display()
                ))
            })?;

            staged_files.push(file_uri(&staged_path));
        }

        Ok(staged_files)
    }

    fn snapshot_meta_for_key(&self, key: &IdempotencyKey) -> Result<Vec<SnapshotMeta>> {
        let mut matching = Vec::new();
        if !self.commits_dir().exists() {
            return Ok(matching);
        }

        for entry in fs::read_dir(self.commits_dir())
            .map_err(|err| Error::msg(format!("{}: {err}", self.commits_dir().display())))?
        {
            let entry = entry.map_err(|err| Error::msg(err.to_string()))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("txt") {
                continue;
            }

            if let Some(meta) = read_snapshot_meta(&self.root, &path)? {
                if meta.idempotency_key == *key {
                    matching.push(meta);
                }
            }
        }

        Ok(matching)
    }
}

#[allow(async_fn_in_trait)]
impl Sink for FilesystemSink {
    async fn prepare_commit(&self, req: CommitRequest) -> Result<PreparedCommit> {
        prepare_append_only_commit(req)
    }

    async fn commit(&self, prepared: PreparedCommit) -> Result<CommitOutcome> {
        self.ensure_layout()?;

        let commit_path =
            self.commit_path(&prepared.request.destination_uri, &prepared.request.idempotency_key);
        if commit_path.exists() {
            if let Some(meta) = read_snapshot_meta(&self.root, &commit_path)? {
                return Ok(CommitOutcome {
                    snapshot_id: meta.snapshot_id.clone(),
                    snapshot: meta.snapshot.clone(),
                    idempotency_key: meta.idempotency_key,
                    destination_uri: meta.destination_uri,
                });
            }
        }

        let committed_files = self.stage_batch_files(&prepared)?;
        let meta = snapshot_meta_with_files(&prepared, committed_files);
        write_snapshot_meta(&self.snapshot_path(&prepared.snapshot_id), &meta)?;
        write_commit_record(&commit_path, &meta)?;
        Ok(commit_outcome(&prepared))
    }

    async fn lookup_commit(&self, key: &IdempotencyKey) -> Result<LookupResult> {
        let mut matching = self.snapshot_meta_for_key(key)?.into_iter();
        let first = matching.next();
        if matching.next().is_some() {
            anyhow::bail!("idempotency key is ambiguous across destinations");
        }

        Ok(match first {
            Some(meta) => LookupResult::Found(CommitOutcome {
                snapshot_id: meta.snapshot_id.clone(),
                snapshot: meta.snapshot.clone(),
                idempotency_key: meta.idempotency_key,
                destination_uri: meta.destination_uri,
            }),
            None => LookupResult::NotFound,
        })
    }

    async fn lookup_snapshot(
        &self,
        snapshot: &greytl_state::SnapshotRef,
    ) -> Result<Option<SnapshotMeta>> {
        let snapshot_id = snapshot
            .uri
            .rsplit('/')
            .next()
            .ok_or_else(|| Error::msg("invalid snapshot uri"))?;
        read_snapshot_file(&self.snapshot_path(snapshot_id))
    }

    async fn resolve_uncertain_commit(&self, attempt: &CommitLocator) -> Result<ResolvedOutcome> {
        let commit_path = self.commit_path(&attempt.destination_uri, &attempt.idempotency_key);
        match read_snapshot_meta(&self.root, &commit_path)? {
            Some(meta) => Ok(ResolvedOutcome::Committed(CommitOutcome {
                snapshot_id: meta.snapshot_id.clone(),
                snapshot: meta.snapshot.clone(),
                idempotency_key: meta.idempotency_key,
                destination_uri: meta.destination_uri,
            })),
            None => Ok(ResolvedOutcome::NotCommitted),
        }
    }
}

fn write_commit_record(path: &Path, meta: &SnapshotMeta) -> Result<()> {
    fs::write(path, meta.snapshot_id.as_bytes())
        .map_err(|err| Error::msg(format!("{}: {err}", path.display())))
}

fn write_snapshot_meta(path: &Path, meta: &SnapshotMeta) -> Result<()> {
    let mut lines = vec![
        format!("snapshot_id={}", meta.snapshot_id),
        format!("snapshot_uri={}", meta.snapshot.uri),
        format!("batch_id={}", meta.batch_id),
        format!("destination_uri={}", meta.destination_uri),
        format!("idempotency_key={}", meta.idempotency_key),
        format!("record_count={}", meta.record_count),
    ];
    for file in &meta.committed_files {
        lines.push(format!("file={file}"));
    }
    fs::write(path, lines.join("\n"))
        .map_err(|err| Error::msg(format!("{}: {err}", path.display())))
}

fn read_snapshot_meta(root: &Path, commit_path: &Path) -> Result<Option<SnapshotMeta>> {
    if !commit_path.exists() {
        return Ok(None);
    }
    let snapshot_id = fs::read_to_string(commit_path)
        .map_err(|err| Error::msg(format!("{}: {err}", commit_path.display())))?;
    let snapshot_path = root
        .join("snapshots")
        .join(format!("{}.txt", snapshot_id.trim()));
    read_snapshot_file(&snapshot_path)
}

fn read_snapshot_file(path: &Path) -> Result<Option<SnapshotMeta>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(path)
        .map_err(|err| Error::msg(format!("{}: {err}", path.display())))?;
    let mut snapshot_id = None;
    let mut snapshot_uri = None;
    let mut batch_id = None;
    let mut destination_uri = None;
    let mut idempotency_key = None;
    let mut record_count = None;
    let mut committed_files = Vec::new();

    for line in content.lines() {
        if let Some(value) = line.strip_prefix("snapshot_id=") {
            snapshot_id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("snapshot_uri=") {
            snapshot_uri = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("batch_id=") {
            batch_id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("destination_uri=") {
            destination_uri = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("idempotency_key=") {
            idempotency_key = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("record_count=") {
            record_count = value.parse::<u64>().ok();
        } else if let Some(value) = line.strip_prefix("file=") {
            committed_files.push(value.to_string());
        }
    }

    Ok(Some(SnapshotMeta {
        snapshot_id: snapshot_id.ok_or_else(|| Error::msg("snapshot_id missing"))?,
        snapshot: greytl_state::SnapshotRef {
            uri: snapshot_uri.ok_or_else(|| Error::msg("snapshot_uri missing"))?,
        },
        batch_id: batch_id
            .ok_or_else(|| Error::msg("batch_id missing"))?
            .into(),
        destination_uri: destination_uri.ok_or_else(|| Error::msg("destination_uri missing"))?,
        idempotency_key: idempotency_key
            .ok_or_else(|| Error::msg("idempotency_key missing"))?
            .into(),
        committed_files,
        record_count: record_count.ok_or_else(|| Error::msg("record_count missing"))?,
    }))
}

fn destination_root(destination_uri: &str) -> Result<PathBuf> {
    file_uri_path(destination_uri)
}

fn file_uri_path(uri: &str) -> Result<PathBuf> {
    uri.strip_prefix("file://")
        .map(PathBuf::from)
        .ok_or_else(|| Error::msg(format!("filesystem sink requires file:// uri, got {uri}")))
}

fn file_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}
