use crate::commit_protocol::{
    commit_outcome, prepare_append_only_commit, scoped_commit_key, snapshot_meta_with_files,
    stable_hash, CommitLocator, CommitOutcome, CommitRequest, IdempotencyKey, LookupResult,
    PreparedCommit, ResolvedOutcome, Sink, SnapshotMeta,
};
use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const COMMIT_PREFIX: &str = "greytl.commit.";

#[derive(Debug, Clone)]
pub struct PolarisSink {
    client: reqwest::blocking::Client,
    catalog_uri: String,
    warehouse: String,
    namespace: String,
    destination_uri: String,
    bearer_token: Option<String>,
    client_credentials: Option<ClientCredentials>,
}

#[derive(Debug, Clone)]
struct ClientCredentials {
    client_id: String,
    client_secret: String,
}

impl PolarisSink {
    pub fn new(
        catalog_uri: impl Into<String>,
        warehouse: impl Into<String>,
        namespace: impl Into<String>,
        destination_uri: impl Into<String>,
    ) -> Self {
        Self {
            client: reqwest::blocking::Client::new(),
            catalog_uri: trim_trailing_slash(&catalog_uri.into()),
            warehouse: warehouse.into(),
            namespace: namespace.into(),
            destination_uri: trim_trailing_slash(&destination_uri.into()),
            bearer_token: None,
            client_credentials: None,
        }
    }

    pub fn with_bearer_token(mut self, bearer_token: impl Into<String>) -> Self {
        self.bearer_token = Some(bearer_token.into());
        self
    }

    pub fn with_client_credentials(
        mut self,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.client_credentials = Some(ClientCredentials {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        });
        self
    }

    fn config_url(&self) -> String {
        format!(
            "{}/v1/config?warehouse={}",
            self.catalog_uri, self.warehouse
        )
    }

    fn oauth_url(&self) -> String {
        format!("{}/v1/oauth/tokens", self.catalog_uri)
    }

    fn token(&self) -> Result<Option<String>> {
        if let Some(token) = &self.bearer_token {
            return Ok(Some(token.clone()));
        }

        let Some(credentials) = &self.client_credentials else {
            return Ok(None);
        };

        let response = self
            .client
            .post(self.oauth_url())
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", credentials.client_id.as_str()),
                ("client_secret", credentials.client_secret.as_str()),
                ("scope", "PRINCIPAL_ROLE:ALL"),
            ])
            .send()
            .map_err(http_error)?
            .error_for_status()
            .map_err(http_error)?;

        let token = response.json::<OAuthTokenResponse>().map_err(http_error)?;
        Ok(Some(token.access_token))
    }

    fn headers(&self) -> Result<reqwest::header::HeaderMap> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        if let Some(token) = self.token()? {
            let value = format!("Bearer {token}");
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&value).expect("valid bearer token header"),
            );
        }
        Ok(headers)
    }

    fn catalog_prefix(&self) -> Result<String> {
        let response = self
            .client
            .get(self.config_url())
            .headers(self.headers()?)
            .send()
            .map_err(http_error)?
            .error_for_status()
            .map_err(http_error)?;
        let config = response.json::<ConfigResponse>().map_err(http_error)?;

        config
            .defaults
            .get("prefix")
            .cloned()
            .or_else(|| config.overrides.get("prefix").cloned())
            .or_else(|| Some(self.warehouse.clone()))
            .ok_or_else(|| Error::msg("Polaris config response missing prefix"))
    }

    fn namespace_url(&self, prefix: &str) -> String {
        format!(
            "{}/v1/{prefix}/namespaces/{}",
            self.catalog_uri, self.namespace
        )
    }

    fn namespace_properties_url(&self, prefix: &str) -> String {
        format!("{}/properties", self.namespace_url(prefix))
    }

    fn ensure_namespace(&self) -> Result<String> {
        let prefix = self.catalog_prefix()?;
        let response = self
            .client
            .get(self.namespace_url(&prefix))
            .headers(self.headers()?)
            .send()
            .map_err(http_error)?;
        if response.status().is_success() {
            return Ok(prefix);
        }
        if response.status().as_u16() != 404 {
            return Err(http_error(
                response
                    .error_for_status()
                    .expect_err("expected non-success status"),
            ));
        }

        self.client
            .post(format!("{}/v1/{prefix}/namespaces", self.catalog_uri))
            .headers(self.headers()?)
            .json(&CreateNamespaceRequest {
                namespace: vec![self.namespace.clone()],
                properties: BTreeMap::new(),
            })
            .send()
            .map_err(http_error)?
            .error_for_status()
            .map_err(http_error)?;

        Ok(prefix)
    }

    fn load_namespace_properties(&self) -> Result<Option<BTreeMap<String, String>>> {
        let prefix = self.catalog_prefix()?;
        let response = self
            .client
            .get(self.namespace_url(&prefix))
            .headers(self.headers()?)
            .send()
            .map_err(http_error)?;

        if response.status().as_u16() == 404 {
            return Ok(None);
        }

        let response = response.error_for_status().map_err(http_error)?;
        let namespace = response.json::<NamespaceResponse>().map_err(http_error)?;
        Ok(Some(namespace.properties.unwrap_or_default()))
    }

    fn load_commit_record(&self, key: &IdempotencyKey) -> Result<Option<StoredCommitRecord>> {
        let properties = match self.load_namespace_properties()? {
            Some(properties) => properties,
            None => return Ok(None),
        };
        Ok(StoredCommitRecord::from_properties(
            &properties,
            self.destination_uri.as_str(),
            key,
        ))
    }

    fn update_commit_record(&self, prefix: &str, prepared: &PreparedCommit) -> Result<()> {
        let key_hash =
            commit_record_key_hash(&self.destination_uri, &prepared.request.idempotency_key);
        let mut updates = BTreeMap::new();
        updates.insert(
            format!("{COMMIT_PREFIX}{key_hash}.snapshot_id"),
            prepared.snapshot_id.clone(),
        );
        updates.insert(
            format!("{COMMIT_PREFIX}{key_hash}.snapshot_uri"),
            prepared.snapshot.uri.clone(),
        );
        updates.insert(
            format!("{COMMIT_PREFIX}{key_hash}.replay_identity"),
            prepared.request.manifest.replay_identity(),
        );

        self.client
            .post(self.namespace_properties_url(prefix))
            .headers(self.headers()?)
            .json(&UpdateNamespacePropertiesRequest {
                removals: Vec::new(),
                updates,
            })
            .send()
            .map_err(http_error)?
            .error_for_status()
            .map_err(http_error)?;
        Ok(())
    }

    fn destination_root(&self) -> Result<PathBuf> {
        file_uri_path(&self.destination_uri)
    }

    fn snapshots_dir(&self) -> Result<PathBuf> {
        Ok(self.destination_root()?.join("_greytl_snapshots"))
    }

    fn snapshot_path(&self, snapshot_id: &str) -> Result<PathBuf> {
        Ok(self.snapshots_dir()?.join(format!("{snapshot_id}.txt")))
    }

    fn stage_batch_files(&self, prepared: &PreparedCommit) -> Result<Vec<String>> {
        let destination_root = self.destination_root()?;
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
            let temp_path = data_dir.join(format!(
                "{}-{index:04}.{extension}.tmp",
                prepared.snapshot_id
            ));

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
            replace_staged_file(&temp_path, &staged_path)?;
            staged_files.push(file_uri(&staged_path));
        }

        Ok(staged_files)
    }
}

#[allow(async_fn_in_trait)]
impl Sink for PolarisSink {
    async fn prepare_commit(&self, req: CommitRequest) -> Result<PreparedCommit> {
        if trim_trailing_slash(&req.destination_uri) != self.destination_uri {
            anyhow::bail!("Polaris sink destination_uri must match the configured destination");
        }
        match req.manifest.table_mode {
            iceflow_types::TableMode::AppendOnly => prepare_append_only_commit(req),
            iceflow_types::TableMode::KeyedUpsert => {
                anyhow::bail!("Polaris sink v0 only supports append_only commits")
            }
        }
    }

    async fn commit(&self, prepared: PreparedCommit) -> Result<CommitOutcome> {
        let prefix = self.ensure_namespace()?;

        if let Some(existing) = self.load_commit_record(&prepared.request.idempotency_key)? {
            if existing.replay_identity != prepared.request.manifest.replay_identity() {
                anyhow::bail!("idempotency key reused with different manifest");
            }
            return Ok(CommitOutcome {
                snapshot_id: existing.snapshot_id,
                snapshot: existing.snapshot,
                idempotency_key: prepared.request.idempotency_key,
                destination_uri: self.destination_uri.clone(),
            });
        }

        let committed_files = self.stage_batch_files(&prepared)?;
        let meta = snapshot_meta_with_files(&prepared, committed_files);
        write_snapshot_meta(&self.snapshot_path(&prepared.snapshot_id)?, &meta)?;
        self.update_commit_record(&prefix, &prepared)?;
        Ok(commit_outcome(&prepared))
    }

    async fn lookup_commit(&self, key: &IdempotencyKey) -> Result<LookupResult> {
        match self.load_commit_record(key)? {
            Some(record) => Ok(LookupResult::Found(CommitOutcome {
                snapshot_id: record.snapshot_id,
                snapshot: record.snapshot,
                idempotency_key: key.clone(),
                destination_uri: self.destination_uri.clone(),
            })),
            None => Ok(LookupResult::NotFound),
        }
    }

    async fn lookup_snapshot(
        &self,
        snapshot: &iceflow_state::SnapshotRef,
    ) -> Result<Option<SnapshotMeta>> {
        let snapshot_id = snapshot
            .uri
            .rsplit('/')
            .next()
            .ok_or_else(|| Error::msg("invalid snapshot uri"))?;
        read_snapshot_file(&self.snapshot_path(snapshot_id)?)
    }

    async fn resolve_uncertain_commit(&self, attempt: &CommitLocator) -> Result<ResolvedOutcome> {
        if trim_trailing_slash(&attempt.destination_uri) != self.destination_uri {
            return Ok(ResolvedOutcome::NotCommitted);
        }
        match self.load_commit_record(&attempt.idempotency_key)? {
            Some(record) => Ok(ResolvedOutcome::Committed(CommitOutcome {
                snapshot_id: record.snapshot_id,
                snapshot: record.snapshot,
                idempotency_key: attempt.idempotency_key.clone(),
                destination_uri: self.destination_uri.clone(),
            })),
            None => Ok(ResolvedOutcome::NotCommitted),
        }
    }
}

fn commit_record_key_hash(destination_uri: &str, key: &IdempotencyKey) -> String {
    stable_hash([scoped_commit_key(destination_uri, key)])
}

fn write_snapshot_meta(path: &Path, meta: &SnapshotMeta) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| Error::msg(format!("{}: {err}", parent.display())))?;
    }

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

fn read_snapshot_file(path: &Path) -> Result<Option<SnapshotMeta>> {
    if !path.exists() {
        return Ok(None);
    }
    let content =
        fs::read_to_string(path).map_err(|err| Error::msg(format!("{}: {err}", path.display())))?;
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
        snapshot: iceflow_state::SnapshotRef {
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

fn file_uri_path(uri: &str) -> Result<PathBuf> {
    uri.strip_prefix("file://")
        .map(PathBuf::from)
        .ok_or_else(|| Error::msg(format!("Polaris sink requires file:// uri, got {uri}")))
}

fn file_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

#[cfg(not(windows))]
fn replace_staged_file(temp_path: &Path, staged_path: &Path) -> Result<()> {
    fs::rename(temp_path, staged_path).map_err(|err| {
        Error::msg(format!(
            "rename {} -> {} failed: {err}",
            temp_path.display(),
            staged_path.display()
        ))
    })
}

#[cfg(windows)]
fn replace_staged_file(temp_path: &Path, staged_path: &Path) -> Result<()> {
    if staged_path.exists() {
        fs::remove_file(staged_path)
            .map_err(|err| Error::msg(format!("remove {} failed: {err}", staged_path.display())))?;
    }
    fs::rename(temp_path, staged_path).map_err(|err| {
        Error::msg(format!(
            "rename {} -> {} failed: {err}",
            temp_path.display(),
            staged_path.display()
        ))
    })
}

fn trim_trailing_slash(value: &str) -> String {
    value.trim_end_matches('/').to_string()
}

fn http_error(err: reqwest::Error) -> Error {
    Error::msg(err.to_string())
}

#[derive(Debug, Deserialize)]
struct ConfigResponse {
    #[serde(default)]
    defaults: BTreeMap<String, String>,
    #[serde(default)]
    overrides: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct NamespaceResponse {
    #[serde(default)]
    properties: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize)]
struct CreateNamespaceRequest {
    namespace: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct UpdateNamespacePropertiesRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    removals: Vec<String>,
    updates: BTreeMap<String, String>,
}

#[derive(Debug)]
struct StoredCommitRecord {
    snapshot_id: String,
    snapshot: iceflow_state::SnapshotRef,
    replay_identity: String,
}

impl StoredCommitRecord {
    fn from_properties(
        properties: &BTreeMap<String, String>,
        destination_uri: &str,
        key: &IdempotencyKey,
    ) -> Option<Self> {
        let prefix = commit_record_prefix(destination_uri, key);
        let snapshot_id = properties.get(&format!("{prefix}.snapshot_id"))?.clone();
        let snapshot_uri = properties.get(&format!("{prefix}.snapshot_uri"))?.clone();
        let replay_identity = properties
            .get(&format!("{prefix}.replay_identity"))?
            .clone();

        Some(Self {
            snapshot_id,
            snapshot: iceflow_state::SnapshotRef { uri: snapshot_uri },
            replay_identity,
        })
    }
}

fn commit_record_prefix(destination_uri: &str, key: &IdempotencyKey) -> String {
    format!(
        "{COMMIT_PREFIX}{}",
        commit_record_key_hash(destination_uri, key)
    )
}
