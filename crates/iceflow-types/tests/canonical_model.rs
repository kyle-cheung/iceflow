use iceflow_types::{
    checkpoint, evaluate_schema_policy, key, ordering, structured_key_identity, table_id,
    validate_mutation, DataType, KeyPart, LogicalMutation, Operation, Schema, SchemaColumn,
    SchemaDecision, SourceClass, StructuredKey, TableMode,
};
use serde_json::json;
use std::collections::BTreeMap;
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
    chrono::DateTime::from_timestamp(1, 0).expect("valid timestamp")
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
    .build()
    .expect("builder should validate the mutation");

    let mut mutation = mutation;
    mutation.after = Some(json!({"name": "bad"}));

    assert!(validate_mutation(&mutation).is_err());
}

#[test]
fn validate_mutation_rejects_missing_source_id() {
    let mut mutation = valid_keyed_mutation();
    mutation.source_id.clear();

    assert!(validate_mutation(&mutation).is_err());
}

#[test]
fn validate_mutation_rejects_empty_ordering_field() {
    let mut mutation = valid_keyed_mutation();
    mutation.ordering_field.clear();

    assert!(validate_mutation(&mutation).is_err());
}

#[test]
fn validate_mutation_rejects_missing_source_checkpoint() {
    let mut mutation = valid_keyed_mutation();
    mutation.source_checkpoint = checkpoint("");

    assert!(validate_mutation(&mutation).is_err());
}

#[test]
fn validate_mutation_rejects_nonpositive_schema_version() {
    let mut mutation = valid_keyed_mutation();
    mutation.schema_version = 0;

    assert!(validate_mutation(&mutation).is_err());
}

#[test]
fn validate_mutation_rejects_non_adjacent_duplicate_key_parts() {
    let invalid_key = StructuredKey::new(vec![
        KeyPart {
            name: "tenant_id".to_string(),
            value: json!("t1"),
        },
        KeyPart {
            name: "customer_id".to_string(),
            value: json!("c1"),
        },
        KeyPart {
            name: "tenant_id".to_string(),
            value: json!("t2"),
        },
    ]);

    let mut mutation = valid_keyed_mutation();
    mutation.key = invalid_key;

    assert!(validate_mutation(&mutation).is_err());
}

#[test]
fn structured_key_preserves_caller_order() {
    let structured = key([("tenant_id", "t1"), ("customer_id", "c1")]);

    let names: Vec<_> = structured
        .parts
        .iter()
        .map(|part| part.name.as_str())
        .collect();

    assert_eq!(names, vec!["tenant_id", "customer_id"]);
}

#[test]
fn structured_key_identity_is_stable_for_composite_keys() {
    let structured = key([("tenant_id", "t1"), ("customer_id", "c1")]);

    assert_eq!(
        structured_key_identity(&structured),
        "tenant_id=String(\"t1\")|customer_id=String(\"c1\")"
    );
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

#[test]
fn schema_policy_allows_additive_evolution() {
    let current = Schema::new(
        vec![SchemaColumn {
            name: "customer_id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }],
        vec!["customer_id".to_string()],
    );
    let next = Schema::new(
        vec![
            SchemaColumn {
                name: "customer_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            SchemaColumn {
                name: "customer_name".to_string(),
                data_type: DataType::String,
                nullable: true,
            },
        ],
        vec!["customer_id".to_string()],
    );

    assert_eq!(
        evaluate_schema_policy(&current, &next),
        SchemaDecision::Allow
    );
}

#[test]
fn append_only_mutation_allows_empty_key() {
    let mutation = LogicalMutation::insert(
        table_id("orders_events"),
        "source-a",
        SourceClass::DatabaseCdc,
        TableMode::AppendOnly,
        StructuredKey::new(vec![]),
        ordering("source_position", 1),
        checkpoint("batch-0001"),
        1,
        fixed_time(),
        BTreeMap::new(),
    )
    .with_after(json!({"order_id": "o-1"}))
    .build();

    assert!(mutation.is_ok());
}

fn valid_keyed_mutation() -> LogicalMutation {
    LogicalMutation::builder(
        table_id("customer_state"),
        "source-a",
        SourceClass::DatabaseCdc,
        TableMode::KeyedUpsert,
        Operation::Delete,
        key([("tenant_id", "t1"), ("customer_id", "c1")]),
        ordering("source_position", 42),
        checkpoint("batch-0001"),
        1,
        fixed_time(),
        BTreeMap::new(),
    )
    .build()
    .expect("builder should validate the mutation")
}
