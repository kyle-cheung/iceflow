use greytl_types::{
    checkpoint, evaluate_schema_policy, key, ordering, table_id, validate_mutation, DataType,
    LogicalMutation, Operation, Schema, SchemaColumn, SchemaDecision, SourceClass, TableMode,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

fn schema_with_key(name: &str, data_type: DataType) -> Schema {
    Schema::new(
        vec![SchemaColumn {
            name: name.to_string(),
            data_type,
            nullable: false,
        }],
        vec![name.to_string()],
    )
}

fn fixed_time() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_system_time(SystemTime::UNIX_EPOCH + Duration::from_secs(1))
}

#[test]
fn delete_mutation_requires_null_after() {
    let mutation = LogicalMutation::delete(
        table_id("customer_state"),
        "source-a",
        SourceClass::DatabaseCdc,
        TableMode::KeyedUpsert,
        key([("tenant_id", "t1"), ("customer_id", "c1")]),
        ordering("source_position", 42),
        checkpoint("batch-0001"),
        1,
        fixed_time(),
        BTreeMap::new(),
    )
    .with_after(json!({"name": "bad"}))
    .build();

    assert!(validate_mutation(&mutation).is_err());
}

#[test]
fn validate_mutation_rejects_missing_canonical_metadata() {
    let mutation = LogicalMutation::builder(
        table_id("customer_state"),
        "",
        SourceClass::DatabaseCdc,
        TableMode::KeyedUpsert,
        Operation::Delete,
        key([("tenant_id", "t1"), ("customer_id", "c1")]),
        ordering("", 42),
        checkpoint(""),
        0,
        fixed_time(),
        BTreeMap::new(),
    )
    .build();

    assert!(validate_mutation(&mutation).is_err());
}

#[test]
fn schema_policy_rejects_key_column_widening() {
    let current = schema_with_key("customer_id", DataType::Int32);
    let next = schema_with_key("customer_id", DataType::Int64);

    assert_eq!(
        evaluate_schema_policy(&current, &next),
        SchemaDecision::Quarantine("key-column-type-change")
    );
}
