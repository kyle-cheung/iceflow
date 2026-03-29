//! In-process DuckDB worker.

mod compaction;
mod normalize;
mod parquet_writer;

use anyhow::Result;
pub use compaction::{compact_parquet_files, CompactedFiles};
use iceflow_source::SourceBatch;
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

#[cfg(test)]
pub(crate) mod test_support {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    pub(crate) fn run_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        struct NoopWake;

        impl Wake for NoopWake {
            fn wake(self: Arc<Self>) {}
        }

        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = Pin::from(Box::new(future));

        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("future unexpectedly pending"),
        }
    }
}
