use anyhow::{Error, Result};
use iceflow_source::SourceCheckReport;
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
