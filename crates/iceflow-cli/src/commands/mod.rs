use anyhow::{Error, Result};
use std::path::PathBuf;

mod compact_catalog;
mod compact_history;

pub mod compact;
pub mod run;

pub(super) fn file_uri_path(uri: &str) -> Result<PathBuf> {
    uri.strip_prefix("file://")
        .map(PathBuf::from)
        .ok_or_else(|| Error::msg(format!("expected file:// uri, got {uri}")))
}
