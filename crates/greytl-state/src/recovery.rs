use crate::{AttemptResolution, AttemptStatus, BatchStatus, QuarantineReason};

pub(crate) fn resolve_attempt(
    resolution: AttemptResolution,
) -> (AttemptStatus, BatchStatus, Option<QuarantineReason>) {
    match resolution {
        AttemptResolution::Committed => (AttemptStatus::Committed, BatchStatus::Committed, None),
        AttemptResolution::Rejected => (
            AttemptStatus::Rejected,
            BatchStatus::SchemaRevalidating,
            None,
        ),
        AttemptResolution::FailedRetryable => (
            AttemptStatus::FailedRetryable,
            BatchStatus::SchemaRevalidating,
            None,
        ),
        AttemptResolution::FailedTerminal => (
            AttemptStatus::FailedTerminal,
            BatchStatus::FailedTerminal,
            None,
        ),
        AttemptResolution::Unknown => (AttemptStatus::Unknown, BatchStatus::CommitUncertain, None),
        AttemptResolution::Ambiguous => (
            AttemptStatus::AmbiguousManual,
            BatchStatus::Quarantined,
            Some(QuarantineReason::AmbiguousResolution),
        ),
    }
}

impl AttemptStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Resolving => "resolving",
            Self::Unknown => "unknown",
            Self::Committed => "committed",
            Self::Rejected => "rejected",
            Self::FailedRetryable => "failed_retryable",
            Self::FailedTerminal => "failed_terminal",
            Self::AmbiguousManual => "ambiguous_manual",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "started" => Some(Self::Started),
            "resolving" => Some(Self::Resolving),
            "unknown" => Some(Self::Unknown),
            "committed" => Some(Self::Committed),
            "rejected" => Some(Self::Rejected),
            "failed_retryable" => Some(Self::FailedRetryable),
            "failed_terminal" => Some(Self::FailedTerminal),
            "ambiguous_manual" => Some(Self::AmbiguousManual),
            _ => None,
        }
    }
}

impl BatchStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::FilesWritten => "files_written",
            Self::CommitStarted => "commit_started",
            Self::CommitUncertain => "commit_uncertain",
            Self::SchemaRevalidating => "schema_revalidating",
            Self::RetryReady => "retry_ready",
            Self::Committed => "committed",
            Self::CheckpointPending => "checkpoint_pending",
            Self::Checkpointed => "checkpointed",
            Self::Quarantined => "quarantined",
            Self::FailedTerminal => "failed_terminal",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "registered" => Some(Self::Registered),
            "files_written" => Some(Self::FilesWritten),
            "commit_started" => Some(Self::CommitStarted),
            "commit_uncertain" => Some(Self::CommitUncertain),
            "schema_revalidating" => Some(Self::SchemaRevalidating),
            "retry_ready" => Some(Self::RetryReady),
            "committed" => Some(Self::Committed),
            "checkpoint_pending" => Some(Self::CheckpointPending),
            "checkpointed" => Some(Self::Checkpointed),
            "quarantined" => Some(Self::Quarantined),
            "failed_terminal" => Some(Self::FailedTerminal),
            _ => None,
        }
    }
}

impl QuarantineReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::OrderingViolation => "ordering_violation",
            Self::SchemaViolation => "schema_violation",
            Self::MalformedBatch => "malformed_batch",
            Self::AmbiguousResolution => "ambiguous_resolution",
        }
    }
}

impl crate::CleanupAction {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Deleted => "deleted",
            Self::Retained => "retained",
        }
    }
}
