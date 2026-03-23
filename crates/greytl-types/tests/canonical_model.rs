use greytl_types::{
    checkpoint, evaluate_schema_policy, key, ordering, table_id, validate_mutation, DataType,
    LogicalMutation, Schema, SchemaColumn, SchemaDecision,
};
use serde_json::json;

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

#[test]
fn delete_mutation_requires_null_after() {
    let mutation = LogicalMutation::delete(
        table_id("customer_state"),
        key([("tenant_id", "t1"), ("customer_id", "c1")]),
        ordering("source_position", 42),
        checkpoint("batch-0001"),
    )
    .with_after(json!({"name": "bad"}));

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
