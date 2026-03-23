use crate::ids::TableId;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceClass {
    DatabaseCdc,
    MessageLogOrQueue,
    FileOrObjectDrop,
    ApiIncremental,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMode {
    AppendOnly,
    KeyedUpsert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Operation {
    Insert,
    Upsert,
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyPart {
    pub name: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructuredKey {
    pub parts: Vec<KeyPart>,
}

impl StructuredKey {
    pub fn new(parts: Vec<KeyPart>) -> Self {
        Self { parts }
    }

    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Value>,
    {
        let mut parts: Vec<_> = pairs
            .into_iter()
            .map(|(name, value)| KeyPart {
                name: name.into(),
                value: value.into(),
            })
            .collect();
        parts.sort_by(|left, right| left.name.cmp(&right.name));
        Self { parts }
    }

    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ordering {
    pub field: String,
    pub value: i64,
}

impl Ordering {
    pub fn new(field: impl Into<String>, value: i64) -> Self {
        Self {
            field: field.into(),
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalMutation {
    pub table_id: TableId,
    pub source_id: String,
    pub source_class: SourceClass,
    pub table_mode: TableMode,
    pub op: Operation,
    pub key: StructuredKey,
    pub after: Option<Value>,
    pub before: Option<Value>,
    pub ordering_field: String,
    pub ordering_value: i64,
    pub source_checkpoint: String,
    pub source_event_id: Option<String>,
    pub schema_version: i32,
    pub ingestion_ts: DateTime<Utc>,
    pub source_metadata: BTreeMap<String, String>,
}

impl LogicalMutation {
    pub fn insert(
        table_id: TableId,
        key: StructuredKey,
        ordering: Ordering,
        source_checkpoint: impl Into<String>,
    ) -> Self {
        Self::new(
            table_id,
            SourceClass::DatabaseCdc,
            TableMode::AppendOnly,
            Operation::Insert,
            key,
            ordering,
            source_checkpoint,
        )
    }

    pub fn upsert(
        table_id: TableId,
        key: StructuredKey,
        ordering: Ordering,
        source_checkpoint: impl Into<String>,
    ) -> Self {
        Self::new(
            table_id,
            SourceClass::DatabaseCdc,
            TableMode::KeyedUpsert,
            Operation::Upsert,
            key,
            ordering,
            source_checkpoint,
        )
    }

    pub fn delete(
        table_id: TableId,
        key: StructuredKey,
        ordering: Ordering,
        source_checkpoint: impl Into<String>,
    ) -> Self {
        Self::new(
            table_id,
            SourceClass::DatabaseCdc,
            TableMode::KeyedUpsert,
            Operation::Delete,
            key,
            ordering,
            source_checkpoint,
        )
    }

    fn new(
        table_id: TableId,
        source_class: SourceClass,
        table_mode: TableMode,
        op: Operation,
        key: StructuredKey,
        ordering: Ordering,
        source_checkpoint: impl Into<String>,
    ) -> Self {
        Self {
            table_id,
            source_id: String::new(),
            source_class,
            table_mode,
            op,
            key,
            after: None,
            before: None,
            ordering_field: ordering.field,
            ordering_value: ordering.value,
            source_checkpoint: source_checkpoint.into(),
            source_event_id: None,
            schema_version: 0,
            ingestion_ts: Utc::now(),
            source_metadata: BTreeMap::new(),
        }
    }

    pub fn with_after(mut self, after: Value) -> Self {
        self.after = Some(after);
        self
    }

    pub fn with_before(mut self, before: Value) -> Self {
        self.before = Some(before);
        self
    }
}

pub fn validate_mutation(m: &LogicalMutation) -> Result<()> {
    match m.op {
        Operation::Delete if m.after.is_some() => anyhow::bail!("delete requires after=null"),
        Operation::Insert | Operation::Upsert if m.after.is_none() => {
            anyhow::bail!("insert/upsert require after payload")
        }
        _ => {}
    }

    match m.table_mode {
        TableMode::AppendOnly if m.op != Operation::Insert => {
            anyhow::bail!("append_only requires insert operations")
        }
        TableMode::KeyedUpsert if m.op == Operation::Insert => {
            anyhow::bail!("keyed_upsert requires upsert or delete operations")
        }
        _ => {}
    }

    if m.key.is_empty() {
        anyhow::bail!("key is required")
    }

    Ok(())
}

pub fn table_id(value: impl Into<String>) -> TableId {
    TableId::new(value)
}

pub fn key<I, K, V>(pairs: I) -> StructuredKey
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<Value>,
{
    StructuredKey::from_pairs(pairs)
}

pub fn ordering(field: impl Into<String>, value: i64) -> Ordering {
    Ordering::new(field, value)
}

pub fn checkpoint(value: impl Into<String>) -> String {
    value.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
