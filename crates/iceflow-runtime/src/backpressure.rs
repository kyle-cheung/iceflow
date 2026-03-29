#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TableBudget {
    pub in_memory_batches: usize,
    pub durable_pending_batches: usize,
    pub unresolved_commits: usize,
}

impl TableBudget {
    pub const fn can_admit(&self) -> bool {
        self.in_memory_batches < 1
            && self.durable_pending_batches < 1
            && self.unresolved_commits < 1
    }

    pub const fn checkpoint_block_reason(&self) -> Option<&'static str> {
        if self.unresolved_commits >= 1 {
            Some("unresolved commit")
        } else if self.in_memory_batches >= 1 || self.durable_pending_batches >= 1 {
            Some("batch in flight")
        } else {
            None
        }
    }

    pub fn reserve_in_memory_slot(&mut self) {
        self.in_memory_batches += 1;
    }

    pub fn release_in_memory_slot(&mut self) {
        self.in_memory_batches = self.in_memory_batches.saturating_sub(1);
    }

    pub fn add_durable_pending_slot(&mut self) {
        self.durable_pending_batches = 1;
    }

    pub fn release_durable_pending_slot(&mut self) {
        self.durable_pending_batches = 0;
    }

    pub fn add_unresolved_commit(&mut self) {
        self.unresolved_commits = 1;
    }

    pub fn resolve_unresolved_commit(&mut self) {
        self.unresolved_commits = 0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntakeDecision {
    Admitted,
    Paused(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointDecision {
    Advanced,
    Blocked(&'static str),
}
