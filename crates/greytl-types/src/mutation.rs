use crate::ids::{CheckpointId, TableId};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceClass {
    DatabaseCdc,
    MessageLogOrQueue,
    FileOrObjectDrop,
    ApiIncremental,
}

impl SourceClass {
    pub const fn stable_tag(self) -> &'static str {
        match self {
            Self::DatabaseCdc => "database_cdc",
            Self::MessageLogOrQueue => "message_log_or_queue",
            Self::FileOrObjectDrop => "file_or_object_drop",
            Self::ApiIncremental => "api_incremental",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMode {
    AppendOnly,
    KeyedUpsert,
}

impl TableMode {
    pub const fn stable_tag(self) -> &'static str {
        match self {
            Self::AppendOnly => "append_only",
            Self::KeyedUpsert => "keyed_upsert",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Operation {
    Insert,
    Upsert,
    Delete,
}

impl Operation {
    pub const fn stable_tag(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Upsert => "upsert",
            Self::Delete => "delete",
        }
    }
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
        let parts: Vec<_> = pairs
            .into_iter()
            .map(|(name, value)| KeyPart {
                name: name.into(),
                value: value.into(),
            })
            .collect();
        Self { parts }
    }

    pub fn validate(&self) -> Result<()> {
        if self.parts.is_empty() {
            anyhow::bail!("key is required");
        }

        let mut seen = BTreeSet::new();
        for part in &self.parts {
            if part.name.trim().is_empty() {
                anyhow::bail!("key part name is required");
            }
            if !seen.insert(part.name.as_str()) {
                anyhow::bail!("key part names must be unique");
            }
        }

        Ok(())
    }
}

pub fn structured_key_identity(key: &StructuredKey) -> String {
    if key.parts.is_empty() {
        return String::new();
    }

    key.parts
        .iter()
        .map(|part| format!("{}={:?}", part.name, part.value))
        .collect::<Vec<_>>()
        .join("|")
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
    pub source_checkpoint: CheckpointId,
    pub source_event_id: Option<String>,
    pub schema_version: i32,
    pub ingestion_ts: DateTime<Utc>,
    pub source_metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct LogicalMutationBuilder {
    table_id: TableId,
    source_id: String,
    source_class: SourceClass,
    table_mode: TableMode,
    op: Operation,
    key: StructuredKey,
    after: Option<Value>,
    before: Option<Value>,
    ordering_field: String,
    ordering_value: i64,
    source_checkpoint: CheckpointId,
    source_event_id: Option<String>,
    schema_version: i32,
    ingestion_ts: DateTime<Utc>,
    source_metadata: BTreeMap<String, String>,
}

impl LogicalMutationBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: TableId,
        source_id: impl Into<String>,
        source_class: SourceClass,
        table_mode: TableMode,
        op: Operation,
        key: StructuredKey,
        ordering: Ordering,
        source_checkpoint: CheckpointId,
        schema_version: i32,
        ingestion_ts: DateTime<Utc>,
        source_metadata: BTreeMap<String, String>,
    ) -> Self {
        Self {
            table_id,
            source_id: source_id.into(),
            source_class,
            table_mode,
            op,
            key,
            after: None,
            before: None,
            ordering_field: ordering.field,
            ordering_value: ordering.value,
            source_checkpoint,
            source_event_id: None,
            schema_version,
            ingestion_ts,
            source_metadata,
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

    pub fn with_source_event_id(mut self, source_event_id: impl Into<String>) -> Self {
        self.source_event_id = Some(source_event_id.into());
        self
    }

    pub fn build(self) -> Result<LogicalMutation> {
        let mutation = LogicalMutation {
            table_id: self.table_id,
            source_id: self.source_id,
            source_class: self.source_class,
            table_mode: self.table_mode,
            op: self.op,
            key: self.key,
            after: self.after,
            before: self.before,
            ordering_field: self.ordering_field,
            ordering_value: self.ordering_value,
            source_checkpoint: self.source_checkpoint,
            source_event_id: self.source_event_id,
            schema_version: self.schema_version,
            ingestion_ts: self.ingestion_ts,
            source_metadata: self.source_metadata,
        };

        validate_mutation(&mutation)?;
        Ok(mutation)
    }
}

impl LogicalMutation {
    #[allow(clippy::too_many_arguments)]
    pub fn builder(
        table_id: TableId,
        source_id: impl Into<String>,
        source_class: SourceClass,
        table_mode: TableMode,
        op: Operation,
        key: StructuredKey,
        ordering: Ordering,
        source_checkpoint: CheckpointId,
        schema_version: i32,
        ingestion_ts: DateTime<Utc>,
        source_metadata: BTreeMap<String, String>,
    ) -> LogicalMutationBuilder {
        LogicalMutationBuilder::new(
            table_id,
            source_id,
            source_class,
            table_mode,
            op,
            key,
            ordering,
            source_checkpoint,
            schema_version,
            ingestion_ts,
            source_metadata,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        table_id: TableId,
        source_id: impl Into<String>,
        source_class: SourceClass,
        table_mode: TableMode,
        key: StructuredKey,
        ordering: Ordering,
        source_checkpoint: CheckpointId,
        schema_version: i32,
        ingestion_ts: DateTime<Utc>,
        source_metadata: BTreeMap<String, String>,
    ) -> LogicalMutationBuilder {
        Self::builder(
            table_id,
            source_id,
            source_class,
            table_mode,
            Operation::Insert,
            key,
            ordering,
            source_checkpoint,
            schema_version,
            ingestion_ts,
            source_metadata,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upsert(
        table_id: TableId,
        source_id: impl Into<String>,
        source_class: SourceClass,
        table_mode: TableMode,
        key: StructuredKey,
        ordering: Ordering,
        source_checkpoint: CheckpointId,
        schema_version: i32,
        ingestion_ts: DateTime<Utc>,
        source_metadata: BTreeMap<String, String>,
    ) -> LogicalMutationBuilder {
        Self::builder(
            table_id,
            source_id,
            source_class,
            table_mode,
            Operation::Upsert,
            key,
            ordering,
            source_checkpoint,
            schema_version,
            ingestion_ts,
            source_metadata,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn delete(
        table_id: TableId,
        source_id: impl Into<String>,
        source_class: SourceClass,
        table_mode: TableMode,
        key: StructuredKey,
        ordering: Ordering,
        source_checkpoint: CheckpointId,
        schema_version: i32,
        ingestion_ts: DateTime<Utc>,
        source_metadata: BTreeMap<String, String>,
    ) -> LogicalMutationBuilder {
        Self::builder(
            table_id,
            source_id,
            source_class,
            table_mode,
            Operation::Delete,
            key,
            ordering,
            source_checkpoint,
            schema_version,
            ingestion_ts,
            source_metadata,
        )
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

    if m.table_id.as_str().trim().is_empty() {
        anyhow::bail!("table_id is required");
    }
    if m.source_id.trim().is_empty() {
        anyhow::bail!("source_id is required");
    }
    if m.ordering_field.trim().is_empty() {
        anyhow::bail!("ordering_field is required");
    }
    if m.source_checkpoint.as_str().trim().is_empty() {
        anyhow::bail!("source_checkpoint is required");
    }
    if m.schema_version <= 0 {
        anyhow::bail!("schema_version must be positive");
    }
    if let Some(source_event_id) = &m.source_event_id {
        if source_event_id.trim().is_empty() {
            anyhow::bail!("source_event_id must not be empty");
        }
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

    match m.table_mode {
        TableMode::KeyedUpsert => m.key.validate()?,
        TableMode::AppendOnly if !m.key.parts.is_empty() => m.key.validate()?,
        TableMode::AppendOnly => {}
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

pub fn checkpoint(value: impl Into<String>) -> CheckpointId {
    CheckpointId::new(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1, 0).expect("valid timestamp")
    }

    #[test]
    fn delete_mutation_requires_null_after() {
        let result = LogicalMutation::delete(
            table_id("customer_state"),
            "source-a",
            SourceClass::DatabaseCdc,
            TableMode::KeyedUpsert,
            key([("tenant_id", "t1"), ("customer_id", "c1")]),
            ordering("source_position", 42),
            checkpoint("batch-0001"),
            1,
            now(),
            BTreeMap::new(),
        )
        .with_after(json!({"name": "bad"}))
        .build();

        assert!(result.is_err());
    }

    #[test]
    fn append_only_allows_empty_key() {
        let result = LogicalMutation::insert(
            table_id("orders_events"),
            "source-a",
            SourceClass::DatabaseCdc,
            TableMode::AppendOnly,
            StructuredKey::new(vec![]),
            ordering("source_position", 1),
            checkpoint("batch-0001"),
            1,
            now(),
            BTreeMap::new(),
        )
        .with_after(json!({"order_id": "o-1"}))
        .build();

        assert!(result.is_ok());
    }
}
