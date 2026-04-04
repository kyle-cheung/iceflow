use anyhow::Result;

use crate::adapter::SourceSpec;

pub fn validate_source_spec(spec: &SourceSpec) -> Result<()> {
    if spec.source_id.trim().is_empty() {
        anyhow::bail!("source_id is required");
    }
    Ok(())
}
