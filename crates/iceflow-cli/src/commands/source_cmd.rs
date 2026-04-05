use anyhow::{Error, Result};
use iceflow_source::{SourceCapability, SourceCheckReport};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::config::{build_source_from_config, load_source_config};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub source_config: PathBuf,
}

impl Args {
    pub fn parse(args: &[String]) -> Result<Self> {
        let source_config = args
            .iter()
            .position(|arg| arg == "--source")
            .and_then(|index| args.get(index + 1))
            .map(PathBuf::from)
            .ok_or_else(|| Error::msg("--source <path> is required"))?;

        Ok(Self { source_config })
    }
}

pub fn execute_blocking(args: Args) -> Result<SourceCheckReport> {
    crate::block_on(execute(args))
}

pub async fn execute(args: Args) -> Result<SourceCheckReport> {
    let config = load_source_config(&args.source_config)?;
    let config_base = args
        .source_config
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let source = build_source_from_config(&config, config_base)?;

    source.check().await
}

pub fn format_report_json(report: &SourceCheckReport) -> String {
    SourceCheckCliReport::from_report(report).to_json()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SourceCheckCliReport {
    capabilities: Vec<String>,
    warnings: Vec<String>,
    details: BTreeMap<String, String>,
}

impl SourceCheckCliReport {
    fn from_report(report: &SourceCheckReport) -> Self {
        Self {
            capabilities: report
                .capabilities
                .iter()
                .map(|capability| capability_tag(*capability).to_string())
                .collect(),
            warnings: report.warnings.clone(),
            details: report.details.clone(),
        }
    }

    fn to_json(&self) -> String {
        serde_json::to_string(self).expect("source check report serialization should not fail")
    }
}

fn capability_tag(capability: SourceCapability) -> &'static str {
    match capability {
        SourceCapability::AppendOnly => "append_only",
        SourceCapability::KeyedUpsert => "keyed_upsert",
        SourceCapability::InitialSnapshot => "initial_snapshot",
        SourceCapability::ChangeFeed => "change_feed",
        SourceCapability::SnapshotHandoff => "snapshot_handoff",
        SourceCapability::Deletes => "deletes",
        SourceCapability::BeforeImages => "before_images",
        SourceCapability::Resume => "resume",
        SourceCapability::DeterministicCheckpoints => "deterministic_checkpoints",
        SourceCapability::StableLatestWinsOrdering => "stable_latest_wins_ordering",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceflow_source::SourceCheckReport;
    use std::collections::BTreeSet;

    #[test]
    fn source_check_report_serializes_as_json() {
        let report = SourceCheckReport {
            capabilities: BTreeSet::from([
                SourceCapability::InitialSnapshot,
                SourceCapability::Resume,
            ]),
            warnings: vec!["warning".to_string()],
            details: BTreeMap::from([("source_id".to_string(), "file.config".to_string())]),
        };

        let json = format_report_json(&report);

        assert!(json.contains("\"capabilities\":[\"initial_snapshot\",\"resume\"]"));
        assert!(json.contains("\"warnings\":[\"warning\"]"));
        assert!(json.contains("\"details\":{\"source_id\":\"file.config\"}"));
    }
}
