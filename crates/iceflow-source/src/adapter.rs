use anyhow::Result;
use async_trait::async_trait;
use iceflow_types::SourceClass;
use std::collections::{BTreeMap, BTreeSet};

use crate::capture::{OpenCaptureRequest, SourceCaptureSession};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpec {
    pub source_id: String,
    pub source_class: SourceClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SourceCapability {
    AppendOnly,
    KeyedUpsert,
    InitialSnapshot,
    ChangeFeed,
    SnapshotHandoff,
    Deletes,
    BeforeImages,
    Resume,
    DeterministicCheckpoints,
    StableLatestWinsOrdering,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCheckReport {
    pub capabilities: BTreeSet<SourceCapability>,
    pub warnings: Vec<String>,
    pub details: BTreeMap<String, String>,
}

#[async_trait]
pub trait SourceAdapter: Send + Sync {
    async fn spec(&self) -> Result<SourceSpec>;
    async fn check(&self) -> Result<SourceCheckReport>;
    async fn open_capture(
        &self,
        req: OpenCaptureRequest,
    ) -> Result<Box<dyn SourceCaptureSession + Send>>;
}
