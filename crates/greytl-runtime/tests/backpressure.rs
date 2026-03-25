use greytl_runtime::{CheckpointDecision, IntakeDecision, RuntimeCoordinator, TableId};

#[test]
fn paused_intake_when_in_memory_slot_is_occupied() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    assert_eq!(runtime.try_admit(&table_id), IntakeDecision::Admitted);

    assert_eq!(
        runtime.try_admit(&table_id),
        IntakeDecision::Paused("table saturation")
    );
}

#[test]
fn paused_intake_when_durable_slot_is_occupied() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    runtime.record_durable_pending_batch(&table_id);

    assert_eq!(
        runtime.try_admit(&table_id),
        IntakeDecision::Paused("table saturation")
    );
}

#[test]
fn paused_intake_when_unresolved_commit_exists() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    runtime.record_unresolved_commit(&table_id);

    assert_eq!(
        runtime.try_admit(&table_id),
        IntakeDecision::Paused("table saturation")
    );
}

#[test]
fn global_pause_on_recovery_queue_saturation() {
    let mut runtime = RuntimeCoordinator::with_recovery_queue_limit(1);
    let first_table = TableId::from("orders_events");
    let second_table = TableId::from("customer_state");

    runtime.set_external_recovery_queue_depth(1);

    assert_eq!(
        runtime.try_admit(&first_table),
        IntakeDecision::Paused("recovery queue saturation")
    );
    assert_eq!(
        runtime.try_admit(&second_table),
        IntakeDecision::Paused("recovery queue saturation")
    );
}

#[test]
fn intake_resumes_after_recovery_queue_clears() {
    let mut runtime = RuntimeCoordinator::with_recovery_queue_limit(1);
    let table_id = TableId::from("orders_events");

    runtime.set_external_recovery_queue_depth(1);
    assert_eq!(
        runtime.try_admit(&table_id),
        IntakeDecision::Paused("recovery queue saturation")
    );

    runtime.set_external_recovery_queue_depth(0);
    assert_eq!(runtime.try_admit(&table_id), IntakeDecision::Admitted);
}

#[test]
fn checkpoint_blocked_during_unresolved_commit() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    runtime.record_unresolved_commit(&table_id);

    assert_eq!(
        runtime.checkpoint_decision(&table_id),
        CheckpointDecision::Blocked("unresolved commit")
    );
}

#[test]
fn configured_recovery_queue_limit_blocks_only_at_threshold() {
    let mut runtime = RuntimeCoordinator::with_recovery_queue_limit(2);
    let table_id = TableId::from("orders_events");

    runtime.set_external_recovery_queue_depth(1);
    assert_eq!(runtime.try_admit(&table_id), IntakeDecision::Admitted);

    runtime.clear_in_memory_batch(&table_id);
    runtime.set_external_recovery_queue_depth(2);
    assert_eq!(
        runtime.try_admit(&table_id),
        IntakeDecision::Paused("recovery queue saturation")
    );
}

#[test]
fn external_and_local_recovery_backlog_share_the_same_limit() {
    let mut runtime = RuntimeCoordinator::with_recovery_queue_limit(2);
    let blocked_table = TableId::from("customer_state");
    let open_table = TableId::from("orders_events");

    runtime.set_external_recovery_queue_depth(1);
    runtime.record_unresolved_commit(&blocked_table);

    assert_eq!(
        runtime.try_admit(&open_table),
        IntakeDecision::Paused("recovery queue saturation")
    );
}

#[test]
fn unresolved_commit_reason_wins_over_other_in_flight_state() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    assert_eq!(runtime.try_admit(&table_id), IntakeDecision::Admitted);
    runtime.record_durable_pending_batch(&table_id);
    runtime.record_unresolved_commit(&table_id);

    assert_eq!(
        runtime.checkpoint_decision(&table_id),
        CheckpointDecision::Blocked("unresolved commit")
    );
}

#[test]
fn intake_and_checkpoint_resume_after_budget_clears() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    runtime.record_unresolved_commit(&table_id);

    assert_eq!(
        runtime.try_admit(&table_id),
        IntakeDecision::Paused("table saturation")
    );
    assert_eq!(
        runtime.checkpoint_decision(&table_id),
        CheckpointDecision::Blocked("unresolved commit")
    );

    runtime.clear_unresolved_commit(&table_id);

    assert_eq!(runtime.try_admit(&table_id), IntakeDecision::Admitted);
    assert_eq!(
        runtime.checkpoint_decision(&table_id),
        CheckpointDecision::Blocked("batch in flight")
    );

    runtime.clear_in_memory_batch(&table_id);

    assert_eq!(runtime.checkpoint_decision(&table_id), CheckpointDecision::Advanced);
}

#[test]
fn checkpoint_blocked_with_in_memory_batch() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    assert_eq!(runtime.try_admit(&table_id), IntakeDecision::Admitted);

    assert_eq!(
        runtime.checkpoint_decision(&table_id),
        CheckpointDecision::Blocked("batch in flight")
    );
}

#[test]
fn checkpoint_blocked_with_durable_pending_batch() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    runtime.record_durable_pending_batch(&table_id);

    assert_eq!(
        runtime.checkpoint_decision(&table_id),
        CheckpointDecision::Blocked("batch in flight")
    );
}

#[test]
fn durable_pending_clear_reopens_intake_and_checkpointing() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    runtime.record_durable_pending_batch(&table_id);

    assert_eq!(
        runtime.try_admit(&table_id),
        IntakeDecision::Paused("table saturation")
    );
    assert_eq!(
        runtime.checkpoint_decision(&table_id),
        CheckpointDecision::Blocked("batch in flight")
    );

    runtime.clear_durable_pending_batch(&table_id);

    assert_eq!(runtime.try_admit(&table_id), IntakeDecision::Admitted);
    runtime.clear_in_memory_batch(&table_id);
    assert_eq!(runtime.checkpoint_decision(&table_id), CheckpointDecision::Advanced);
}

#[test]
fn duplicate_durable_pending_records_are_idempotent() {
    let mut runtime = RuntimeCoordinator::new();
    let table_id = TableId::from("customer_state");

    runtime.record_durable_pending_batch(&table_id);
    runtime.record_durable_pending_batch(&table_id);

    assert_eq!(
        runtime.try_admit(&table_id),
        IntakeDecision::Paused("table saturation")
    );

    runtime.clear_durable_pending_batch(&table_id);
    assert_eq!(runtime.try_admit(&table_id), IntakeDecision::Admitted);
}

#[test]
fn table_saturation_does_not_block_other_tables() {
    let mut runtime = RuntimeCoordinator::new();
    let blocked_table = TableId::from("customer_state");
    let open_table = TableId::from("orders_events");

    assert_eq!(runtime.try_admit(&blocked_table), IntakeDecision::Admitted);

    assert_eq!(
        runtime.try_admit(&blocked_table),
        IntakeDecision::Paused("table saturation")
    );
    assert_eq!(runtime.try_admit(&open_table), IntakeDecision::Admitted);
}

#[test]
fn unresolved_commit_isolated_to_own_table() {
    let mut runtime = RuntimeCoordinator::new();
    let blocked_table = TableId::from("customer_state");
    let open_table = TableId::from("orders_events");

    runtime.record_unresolved_commit(&blocked_table);

    assert_eq!(
        runtime.checkpoint_decision(&blocked_table),
        CheckpointDecision::Blocked("unresolved commit")
    );
    assert_eq!(
        runtime.try_admit(&blocked_table),
        IntakeDecision::Paused("table saturation")
    );
    assert_eq!(
        runtime.checkpoint_decision(&open_table),
        CheckpointDecision::Advanced
    );
    assert_eq!(runtime.try_admit(&open_table), IntakeDecision::Admitted);
}

#[test]
fn duplicate_unresolved_commit_records_are_idempotent() {
    let mut runtime = RuntimeCoordinator::new();
    let blocked_table = TableId::from("customer_state");
    let open_table = TableId::from("orders_events");

    runtime.record_unresolved_commit(&blocked_table);
    runtime.record_unresolved_commit(&blocked_table);

    assert_eq!(
        runtime.checkpoint_decision(&blocked_table),
        CheckpointDecision::Blocked("unresolved commit")
    );
    assert_eq!(runtime.try_admit(&open_table), IntakeDecision::Admitted);

    runtime.clear_in_memory_batch(&open_table);
    runtime.clear_unresolved_commit(&blocked_table);
    assert_eq!(
        runtime.checkpoint_decision(&blocked_table),
        CheckpointDecision::Advanced
    );
}
