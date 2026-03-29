//! Minimal runtime coordinator for local backpressure and checkpoint sequencing.

mod backpressure;
mod failpoints;
mod pipeline;

pub use backpressure::{CheckpointDecision, IntakeDecision, TableBudget};
pub use iceflow_types::TableId;
pub use pipeline::RuntimeCoordinator;
