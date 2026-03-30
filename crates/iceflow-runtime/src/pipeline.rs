use crate::backpressure::{CheckpointDecision, IntakeDecision, TableBudget};
use crate::failpoints::RuntimeFailpoints;
use iceflow_types::TableId;
use std::collections::{btree_map::Entry, BTreeMap};

#[derive(Debug, Clone)]
pub struct RuntimeCoordinator {
    tables: BTreeMap<TableId, TableBudget>,
    failpoints: RuntimeFailpoints,
    external_recovery_queue_depth: usize,
    unresolved_recovery_items: usize,
    recovery_queue_limit: usize,
}

impl RuntimeCoordinator {
    pub fn new() -> Self {
        Self::with_recovery_queue_limit(2)
    }

    pub fn with_recovery_queue_limit(limit: usize) -> Self {
        Self {
            tables: BTreeMap::new(),
            failpoints: RuntimeFailpoints::new(),
            external_recovery_queue_depth: 0,
            unresolved_recovery_items: 0,
            recovery_queue_limit: limit.max(1),
        }
    }

    pub fn record_durable_pending_batch(&mut self, table_id: &TableId) {
        self.budget_mut(table_id).add_durable_pending_slot();
    }

    pub fn clear_durable_pending_batch(&mut self, table_id: &TableId) {
        self.update_existing_budget(table_id, TableBudget::release_durable_pending_slot);
    }

    pub fn record_unresolved_commit(&mut self, table_id: &TableId) {
        let budget = self.budget_mut(table_id);
        if budget.unresolved_commits == 0 {
            budget.add_unresolved_commit();
            self.unresolved_recovery_items += 1;
            self.refresh_recovery_failpoint();
        }
    }

    pub fn clear_unresolved_commit(&mut self, table_id: &TableId) {
        let mut cleared = false;
        self.update_existing_budget(table_id, |budget| {
            if budget.unresolved_commits > 0 {
                budget.resolve_unresolved_commit();
                cleared = true;
            }
        });
        if cleared {
            self.unresolved_recovery_items = self.unresolved_recovery_items.saturating_sub(1);
            self.refresh_recovery_failpoint();
        }
    }

    pub fn clear_in_memory_batch(&mut self, table_id: &TableId) {
        self.update_existing_budget(table_id, TableBudget::release_in_memory_slot);
    }

    pub fn set_external_recovery_queue_depth(&mut self, depth: usize) {
        self.external_recovery_queue_depth = depth;
        self.refresh_recovery_failpoint();
    }

    pub fn try_admit(&mut self, table_id: &TableId) -> IntakeDecision {
        if self.failpoints.recovery_queue_saturated() {
            return IntakeDecision::Paused("recovery queue saturation");
        }

        let budget = self.budget_mut(table_id);

        match budget.can_admit() {
            true => {
                budget.reserve_in_memory_slot();
                IntakeDecision::Admitted
            }
            false => IntakeDecision::Paused("table saturation"),
        }
    }

    pub fn checkpoint_decision(&self, table_id: &TableId) -> CheckpointDecision {
        match self
            .tables
            .get(table_id)
            .copied()
            .unwrap_or_default()
            .checkpoint_block_reason()
        {
            Some(reason) => CheckpointDecision::Blocked(reason),
            None => CheckpointDecision::Advanced,
        }
    }

    fn budget_mut(&mut self, table_id: &TableId) -> &mut TableBudget {
        match self.tables.entry(table_id.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(TableBudget::default()),
        }
    }

    fn update_existing_budget(
        &mut self,
        table_id: &TableId,
        update: impl FnOnce(&mut TableBudget),
    ) {
        let should_remove = match self.tables.get_mut(table_id) {
            Some(budget) => {
                update(budget);
                *budget == TableBudget::default()
            }
            None => false,
        };

        if should_remove {
            self.tables.remove(table_id);
        }
    }

    fn recovery_queue_depth(&self) -> usize {
        self.external_recovery_queue_depth + self.unresolved_recovery_items
    }

    fn refresh_recovery_failpoint(&mut self) {
        self.failpoints
            .set_recovery_queue_saturated(self.recovery_queue_depth() >= self.recovery_queue_limit);
    }
}

impl Default for RuntimeCoordinator {
    fn default() -> Self {
        Self::new()
    }
}
