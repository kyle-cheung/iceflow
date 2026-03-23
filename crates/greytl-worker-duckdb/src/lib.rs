//! In-process DuckDB worker.

mod normalize;
mod parquet_writer;

use anyhow::Result;
use greytl_source::SourceBatch;
pub use normalize::NormalizedBatch;
pub use parquet_writer::{MaterializedBatch, WriterConfig};

#[derive(Debug, Clone)]
pub struct DuckDbWorker {
    config: WriterConfig,
}

impl DuckDbWorker {
    pub fn in_memory() -> Result<Self> {
        Self::with_config(WriterConfig::default())
    }

    pub fn with_config(config: WriterConfig) -> Result<Self> {
        Ok(Self { config })
    }

    pub async fn normalize(&self, batch: SourceBatch) -> Result<NormalizedBatch> {
        normalize::normalize_batch(batch)
    }

    pub async fn materialize(&self, batch: SourceBatch) -> Result<MaterializedBatch> {
        let normalized = self.normalize(batch).await?;
        parquet_writer::materialize_batch(&self.config, normalized)
    }
}
