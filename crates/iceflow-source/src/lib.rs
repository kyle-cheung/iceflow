mod adapter;
pub mod capture;
mod file_source;
mod validation;

pub use adapter::{SourceAdapter, SourceCapability, SourceCheckReport, SourceSpec};
pub use anyhow::{Error as SourceError, Result as SourceResult};
pub use capture::{
    BatchPoll, BatchRequest, CheckpointAck, OpenCaptureRequest, SourceBatch, SourceCaptureSession,
    SourceTableSelection,
};
pub use file_source::FileSource;
pub use validation::validate_source_spec;
