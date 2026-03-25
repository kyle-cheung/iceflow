//! Deterministic sink protocol and local append-only implementations.

pub mod commit_protocol;
pub mod filesystem;
pub mod polaris;
pub mod test_double;

pub use commit_protocol::*;
pub use filesystem::FilesystemSink;
pub use polaris::PolarisSink;
pub use test_double::{SinkFailpoint, TestDoubleSink};
