use anyhow::{Error, Result};
use greytl_source::SourceBatch;
use greytl_types::{CheckpointId, LogicalMutation, SourceClass, TableId, TableMode};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedBatch {
    batch_file: String,
    table_id: TableId,
    table_mode: TableMode,
    source_id: String,
    source_class: SourceClass,
    ordering_field: String,
    ordering_min: i64,
    ordering_max: i64,
    source_checkpoint_start: CheckpointId,
    source_checkpoint_end: CheckpointId,
    schema_version: i32,
    records: Vec<LogicalMutation>,
}

impl NormalizedBatch {
    pub fn batch_file(&self) -> &str {
        &self.batch_file
    }

    pub fn table_id(&self) -> &TableId {
        &self.table_id
    }

    pub fn table_mode(&self) -> TableMode {
        self.table_mode
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn source_class(&self) -> SourceClass {
        self.source_class
    }

    pub fn ordering_field(&self) -> &str {
        &self.ordering_field
    }

    pub fn ordering_min(&self) -> i64 {
        self.ordering_min
    }

    pub fn ordering_max(&self) -> i64 {
        self.ordering_max
    }

    pub fn source_checkpoint_start(&self) -> &CheckpointId {
        &self.source_checkpoint_start
    }

    pub fn source_checkpoint_end(&self) -> &CheckpointId {
        &self.source_checkpoint_end
    }

    pub fn schema_version(&self) -> i32 {
        self.schema_version
    }

    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    pub fn records(&self) -> &[LogicalMutation] {
        &self.records
    }
}

pub fn normalize_batch(batch: SourceBatch) -> Result<NormalizedBatch> {
    let mut records = batch.records;
    let first = records
        .first()
        .ok_or_else(|| Error::msg("source batch must contain at least one record"))?
        .clone();

    let mut ordering_min = first.ordering_value;
    let mut ordering_max = first.ordering_value;
    let mut source_checkpoint_start = first.source_checkpoint.clone();
    let mut source_checkpoint_end = first.source_checkpoint.clone();

    for record in &records {
        if record.table_id != first.table_id {
            anyhow::bail!("table_id mismatch within batch");
        }
        if record.table_mode != first.table_mode {
            anyhow::bail!("table_mode mismatch within batch");
        }
        if record.source_id != first.source_id {
            anyhow::bail!("source_id mismatch within batch");
        }
        if record.source_class != first.source_class {
            anyhow::bail!("source_class mismatch within batch");
        }
        if record.schema_version != first.schema_version {
            anyhow::bail!("schema_version mismatch within batch");
        }
        if record.ordering_field != first.ordering_field {
            anyhow::bail!("ordering_field mismatch within batch");
        }

        if record.ordering_value < ordering_min {
            ordering_min = record.ordering_value;
            source_checkpoint_start = record.source_checkpoint.clone();
        }
        if record.ordering_value > ordering_max {
            ordering_max = record.ordering_value;
            source_checkpoint_end = record.source_checkpoint.clone();
        }
    }

    if first.table_mode == TableMode::KeyedUpsert {
        let mut last_ordering_by_key: BTreeMap<String, i64> = BTreeMap::new();
        for record in &records {
            let key = key_identity(record);
            if let Some(previous) = last_ordering_by_key.get(&key) {
                if record.ordering_value < *previous {
                    anyhow::bail!("ordering violation quarantine");
                }
            }
            last_ordering_by_key.insert(key, record.ordering_value);
        }
        records = reduce_to_latest_by_key(&records)?.into_values().collect();
    }

    Ok(NormalizedBatch {
        batch_file: batch.batch_file,
        table_id: first.table_id,
        table_mode: first.table_mode,
        source_id: first.source_id,
        source_class: first.source_class,
        ordering_field: first.ordering_field,
        ordering_min,
        ordering_max,
        source_checkpoint_start,
        source_checkpoint_end,
        schema_version: first.schema_version,
        records,
    })
}

fn reduce_to_latest_by_key(
    records: &[LogicalMutation],
) -> Result<BTreeMap<String, LogicalMutation>> {
    let mut latest_by_key: BTreeMap<String, LogicalMutation> = BTreeMap::new();

    for record in records {
        let key = key_identity(record);
        match latest_by_key.get(&key) {
            Some(existing) if existing.ordering_value > record.ordering_value => {}
            _ => {
                latest_by_key.insert(key, record.clone());
            }
        }
    }

    Ok(latest_by_key)
}

pub(crate) fn key_identity(record: &LogicalMutation) -> String {
    if record.key.parts.is_empty() {
        return String::new();
    }

    record
        .key
        .parts
        .iter()
        .map(|part| format!("{}={:?}", part.name, part.value))
        .collect::<Vec<_>>()
        .join("|")
}

#[cfg(test)]
mod tests {
    use greytl_source::SourceBatch;
    use greytl_types::{
        checkpoint, key, ordering, table_id, LogicalMutation, Operation, SourceClass, TableMode,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    use crate::test_support::run_ready;
    use crate::DuckDbWorker;

    #[test]
    fn keyed_upsert_normalize_keeps_latest_mutation_per_key() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let normalized =
            run_ready(worker.normalize(sample_keyed_batch())).expect("normalized batch");

        assert_eq!(normalized.record_count(), 2);

        let records = normalized.records();
        assert_eq!(records[0].key, key([("customer_id", 1)]));
        assert_eq!(records[0].ordering_value, 20);
        assert_eq!(records[0].op, Operation::Delete);
        assert_eq!(records[1].key, key([("customer_id", 2)]));
        assert_eq!(records[1].ordering_value, 15);
        assert_eq!(
            records[1].after,
            Some(json!({ "customer_id": 2, "status": "active" }))
        );
    }

    #[test]
    fn keyed_upsert_normalize_allows_interleaved_keys_when_each_key_is_ordered() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let normalized = run_ready(worker.normalize(sample_interleaved_key_batch()))
            .expect("per-key ordered batch should normalize");

        assert_eq!(normalized.record_count(), 2);
        assert_eq!(normalized.records()[0].key, key([("customer_id", 1)]));
        assert_eq!(normalized.records()[0].ordering_value, 15);
        assert_eq!(normalized.records()[1].key, key([("customer_id", 2)]));
        assert_eq!(normalized.records()[1].ordering_value, 20);
    }

    #[test]
    fn normalize_rejects_batches_with_mixed_ordering_fields() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let err = run_ready(worker.normalize(sample_mixed_ordering_batch()))
            .expect_err("invalid ordering");

        assert!(err.to_string().contains("ordering_field"));
    }

    #[test]
    fn append_only_normalize_preserves_records_and_bounds() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let normalized =
            run_ready(worker.normalize(sample_append_only_batch())).expect("normalized batch");

        assert_eq!(normalized.record_count(), 3);
        assert_eq!(normalized.ordering_min(), 11);
        assert_eq!(normalized.ordering_max(), 13);
        assert_eq!(normalized.source_checkpoint_start().as_str(), "cp-11");
        assert_eq!(normalized.source_checkpoint_end().as_str(), "cp-13");
        assert_eq!(normalized.records()[0].ordering_value, 11);
        assert_eq!(normalized.records()[1].ordering_value, 12);
        assert_eq!(normalized.records()[2].ordering_value, 13);
    }

    fn sample_keyed_batch() -> SourceBatch {
        SourceBatch {
            batch_file: "fixtures/customer_state/batch-0001.jsonl".to_string(),
            records: vec![
                keyed_upsert(1, 10, Some(json!({ "customer_id": 1, "status": "trial" }))),
                keyed_upsert(2, 15, Some(json!({ "customer_id": 2, "status": "active" }))),
                keyed_delete(1, 20),
            ],
        }
    }

    fn sample_mixed_ordering_batch() -> SourceBatch {
        let first = keyed_upsert(1, 10, Some(json!({ "customer_id": 1, "status": "trial" })));
        let mut second = keyed_upsert(2, 11, Some(json!({ "customer_id": 2, "status": "active" })));
        second.ordering_field = "lsn".to_string();

        SourceBatch {
            batch_file: "fixtures/customer_state/batch-0002.jsonl".to_string(),
            records: vec![first, second],
        }
    }

    fn sample_interleaved_key_batch() -> SourceBatch {
        SourceBatch {
            batch_file: "fixtures/customer_state/batch-0003.jsonl".to_string(),
            records: vec![
                keyed_upsert(1, 10, Some(json!({ "customer_id": 1, "status": "trial" }))),
                keyed_upsert(2, 20, Some(json!({ "customer_id": 2, "status": "active" }))),
                keyed_upsert(1, 15, Some(json!({ "customer_id": 1, "status": "active" }))),
            ],
        }
    }

    fn sample_append_only_batch() -> SourceBatch {
        SourceBatch {
            batch_file: "fixtures/orders/batch-0001.jsonl".to_string(),
            records: vec![
                append_insert(1, 11),
                append_insert(2, 12),
                append_insert(3, 13),
            ],
        }
    }

    fn keyed_upsert(
        customer_id: i64,
        ordering_value: i64,
        after: Option<serde_json::Value>,
    ) -> LogicalMutation {
        let builder = LogicalMutation::upsert(
            table_id("customer_state"),
            "source-a",
            SourceClass::DatabaseCdc,
            TableMode::KeyedUpsert,
            key([("customer_id", customer_id)]),
            ordering("source_position", ordering_value),
            checkpoint(format!("cp-{ordering_value}")),
            1,
            sample_time(),
            BTreeMap::new(),
        );

        match after {
            Some(after) => builder.with_after(after).build().expect("valid upsert"),
            None => builder.build().expect("valid upsert"),
        }
    }

    fn keyed_delete(customer_id: i64, ordering_value: i64) -> LogicalMutation {
        LogicalMutation::delete(
            table_id("customer_state"),
            "source-a",
            SourceClass::DatabaseCdc,
            TableMode::KeyedUpsert,
            key([("customer_id", customer_id)]),
            ordering("source_position", ordering_value),
            checkpoint(format!("cp-{ordering_value}")),
            1,
            sample_time(),
            BTreeMap::new(),
        )
        .build()
        .expect("valid delete")
    }

    fn append_insert(order_id: i64, ordering_value: i64) -> LogicalMutation {
        LogicalMutation::insert(
            table_id("orders"),
            "source-a",
            SourceClass::FileOrObjectDrop,
            TableMode::AppendOnly,
            greytl_types::StructuredKey::new(vec![]),
            ordering("line_number", ordering_value),
            checkpoint(format!("cp-{ordering_value}")),
            1,
            sample_time(),
            BTreeMap::new(),
        )
        .with_after(json!({ "order_id": order_id, "amount_cents": 100 }))
        .build()
        .expect("valid insert")
    }

    fn sample_time() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(1, 0).expect("valid timestamp")
    }
}
