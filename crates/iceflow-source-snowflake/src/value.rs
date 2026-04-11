use crate::client::RowSet;
use crate::metadata::TableMetadata;
use anyhow::{Error, Result};
use chrono::Utc;
use iceflow_types::{
    ordering, CheckpointId, LogicalMutation, SourceClass, StructuredKey, TableId, TableMode,
};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub(crate) struct MutationContext {
    table_id: TableId,
    source_id: String,
    table_mode: TableMode,
    primary_keys: Vec<String>,
    checkpoint: CheckpointId,
    schema_fingerprint: String,
    batch_query_id: String,
}

impl MutationContext {
    pub(crate) fn new(
        table_id: TableId,
        source_id: String,
        table_mode: TableMode,
        metadata: &TableMetadata,
        checkpoint: CheckpointId,
        batch_query_id: String,
    ) -> Self {
        Self {
            table_id,
            source_id,
            table_mode,
            primary_keys: metadata.primary_keys.clone(),
            checkpoint,
            schema_fingerprint: metadata.schema_fingerprint.clone(),
            batch_query_id,
        }
    }

    #[cfg(test)]
    fn for_test() -> Self {
        Self {
            table_id: TableId::from("customer_state.customer_state"),
            source_id: "snowflake.config.local_snowflake".to_string(),
            table_mode: TableMode::AppendOnly,
            primary_keys: vec!["id".to_string()],
            checkpoint: CheckpointId::from(
                "snowflake:v1:stream:01b12345-0602-1234-0000-000000000000",
            ),
            schema_fingerprint: "fingerprint-v1".to_string(),
            batch_query_id: "01b12345-0602-1234-0000-000000000000".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChangeRow {
    pub metadata_row_id: String,
    pub metadata_action: String,
    pub metadata_is_update: bool,
    pub values: BTreeMap<String, Value>,
}

impl ChangeRow {
    fn is_action(&self, action: &str) -> bool {
        self.metadata_action.eq_ignore_ascii_case(action)
    }
}

pub(crate) fn snapshot_rows_to_mutations(
    ctx: MutationContext,
    row_set: RowSet,
) -> Result<Vec<LogicalMutation>> {
    row_set
        .rows
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            let values = values_from_columns(&row_set.columns, row)?;
            build_mutation(
                &ctx,
                OperationKind::Insert,
                Some(values),
                None,
                index as i64 + 1,
                BTreeMap::new(),
                Some(format!("snapshot-{}", index + 1)),
            )
        })
        .collect()
}

pub(crate) fn parse_change_rows(row_set: RowSet) -> Result<Vec<ChangeRow>> {
    let row_id_index = metadata_index(&row_set.columns, "METADATA$ROW_ID")?;
    let action_index = metadata_index(&row_set.columns, "METADATA$ACTION")?;
    let is_update_index = metadata_index(&row_set.columns, "METADATA$ISUPDATE")?;

    row_set
        .rows
        .into_iter()
        .map(|row| {
            let metadata_row_id = row
                .get(row_id_index)
                .cloned()
                .ok_or_else(|| Error::msg("Snowflake change row missing METADATA$ROW_ID"))?;
            let metadata_action = row
                .get(action_index)
                .cloned()
                .ok_or_else(|| Error::msg("Snowflake change row missing METADATA$ACTION"))?;
            let metadata_is_update = row
                .get(is_update_index)
                .map(|value| parse_bool(value))
                .ok_or_else(|| Error::msg("Snowflake change row missing METADATA$ISUPDATE"))??;

            let mut values = BTreeMap::new();
            for (index, column) in row_set.columns.iter().enumerate() {
                if index == row_id_index || index == action_index || index == is_update_index {
                    continue;
                }
                let value = row.get(index).cloned().ok_or_else(|| {
                    Error::msg(format!("Snowflake change row missing column {column}"))
                })?;
                values.insert(column.clone(), Value::String(value));
            }

            Ok(ChangeRow {
                metadata_row_id,
                metadata_action,
                metadata_is_update,
                values,
            })
        })
        .collect()
}

pub(crate) fn collapse_change_rows(
    ctx: MutationContext,
    rows: Vec<ChangeRow>,
) -> Result<Vec<LogicalMutation>> {
    let mut grouped: BTreeMap<String, Vec<ChangeRow>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry(row.metadata_row_id.clone())
            .or_default()
            .push(row);
    }

    grouped
        .into_values()
        .enumerate()
        .map(|(index, group)| collapse_group(&ctx, group, index as i64 + 1))
        .collect()
}

pub(crate) fn verify_schema_fingerprint(expected: &str, actual: &str) -> Result<()> {
    if expected != actual {
        return Err(Error::msg(
            "Snowflake schema drift detected; rebootstrap required",
        ));
    }
    Ok(())
}

fn collapse_group(
    ctx: &MutationContext,
    group: Vec<ChangeRow>,
    ordering_value: i64,
) -> Result<LogicalMutation> {
    if group.iter().any(|row| row.metadata_is_update) {
        return collapse_update_group(ctx, group, ordering_value);
    }

    let [single] = group.as_slice() else {
        return Err(Error::msg("invalid Snowflake change row group"));
    };

    if single.is_action("INSERT") {
        return build_change_mutation(
            ctx,
            single,
            OperationKind::Insert,
            Some(single.values.clone()),
            None,
            ordering_value,
        );
    }
    if single.is_action("DELETE") {
        let operation = match ctx.table_mode {
            TableMode::AppendOnly => OperationKind::Insert,
            TableMode::KeyedUpsert => OperationKind::Delete,
        };
        let after = match ctx.table_mode {
            TableMode::AppendOnly => Some(single.values.clone()),
            TableMode::KeyedUpsert => None,
        };
        return build_change_mutation(
            ctx,
            single,
            operation,
            after,
            Some(single.values.clone()),
            ordering_value,
        );
    }

    Err(Error::msg(format!(
        "unsupported Snowflake change action: {}",
        single.metadata_action
    )))
}

fn collapse_update_group(
    ctx: &MutationContext,
    group: Vec<ChangeRow>,
    ordering_value: i64,
) -> Result<LogicalMutation> {
    if group.len() != 2 {
        return Err(Error::msg("invalid Snowflake update pair"));
    }

    let delete = group
        .iter()
        .find(|row| row.is_action("DELETE"))
        .ok_or_else(|| Error::msg("invalid Snowflake update pair"))?;
    let insert = group
        .iter()
        .find(|row| row.is_action("INSERT"))
        .ok_or_else(|| Error::msg("invalid Snowflake update pair"))?;

    if !delete.metadata_is_update || !insert.metadata_is_update {
        return Err(Error::msg("invalid Snowflake update pair"));
    }

    if key_from_values(ctx, &delete.values)? != key_from_values(ctx, &insert.values)? {
        return Err(Error::msg(
            "Snowflake update pair primary key identity changed",
        ));
    }

    let operation = match ctx.table_mode {
        TableMode::AppendOnly => OperationKind::Insert,
        TableMode::KeyedUpsert => OperationKind::Upsert,
    };

    build_change_mutation(
        ctx,
        insert,
        operation,
        Some(insert.values.clone()),
        Some(delete.values.clone()),
        ordering_value,
    )
}

fn build_change_mutation(
    ctx: &MutationContext,
    row: &ChangeRow,
    operation: OperationKind,
    after: Option<BTreeMap<String, Value>>,
    before: Option<BTreeMap<String, Value>>,
    ordering_value: i64,
) -> Result<LogicalMutation> {
    let metadata = BTreeMap::from([
        ("snowflake_query_id".to_string(), ctx.batch_query_id.clone()),
        (
            "snowflake_schema_fingerprint".to_string(),
            ctx.schema_fingerprint.clone(),
        ),
        (
            "snowflake_metadata_row_id".to_string(),
            row.metadata_row_id.clone(),
        ),
        (
            "snowflake_metadata_action".to_string(),
            row.metadata_action.clone(),
        ),
        (
            "snowflake_metadata_is_update".to_string(),
            row.metadata_is_update.to_string(),
        ),
    ]);

    build_mutation(
        ctx,
        operation,
        after,
        before,
        ordering_value,
        metadata,
        Some(row.metadata_row_id.clone()),
    )
}

fn build_mutation(
    ctx: &MutationContext,
    operation: OperationKind,
    after: Option<BTreeMap<String, Value>>,
    before: Option<BTreeMap<String, Value>>,
    ordering_value: i64,
    mut source_metadata: BTreeMap<String, String>,
    source_event_id: Option<String>,
) -> Result<LogicalMutation> {
    source_metadata
        .entry("snowflake_query_id".to_string())
        .or_insert_with(|| ctx.batch_query_id.clone());
    source_metadata
        .entry("snowflake_schema_fingerprint".to_string())
        .or_insert_with(|| ctx.schema_fingerprint.clone());

    let key_source = after.as_ref().or(before.as_ref()).ok_or_else(|| {
        Error::msg("Snowflake mutation requires either an after or before row image")
    })?;
    let key = key_from_values(ctx, key_source)?;

    let builder = match operation {
        OperationKind::Insert => LogicalMutation::insert(
            ctx.table_id.clone(),
            ctx.source_id.clone(),
            SourceClass::DatabaseCdc,
            ctx.table_mode,
            key,
            ordering("snowflake_ordinal", ordering_value),
            ctx.checkpoint.clone(),
            1,
            Utc::now(),
            source_metadata,
        ),
        OperationKind::Upsert => LogicalMutation::upsert(
            ctx.table_id.clone(),
            ctx.source_id.clone(),
            SourceClass::DatabaseCdc,
            ctx.table_mode,
            key,
            ordering("snowflake_ordinal", ordering_value),
            ctx.checkpoint.clone(),
            1,
            Utc::now(),
            source_metadata,
        ),
        OperationKind::Delete => LogicalMutation::delete(
            ctx.table_id.clone(),
            ctx.source_id.clone(),
            SourceClass::DatabaseCdc,
            ctx.table_mode,
            key,
            ordering("snowflake_ordinal", ordering_value),
            ctx.checkpoint.clone(),
            1,
            Utc::now(),
            source_metadata,
        ),
    };

    let builder = if let Some(after) = after {
        builder.with_after(Value::Object(after))
    } else {
        builder
    };
    let builder = if let Some(before) = before {
        builder.with_before(Value::Object(before))
    } else {
        builder
    };
    let builder = if let Some(source_event_id) = source_event_id {
        builder.with_source_event_id(source_event_id)
    } else {
        builder
    };

    builder.build().map_err(|err| Error::msg(err.to_string()))
}

fn values_from_columns(columns: &[String], row: Vec<String>) -> Result<BTreeMap<String, Value>> {
    if row.len() != columns.len() {
        return Err(Error::msg(format!(
            "Snowflake row has {} values for {} columns",
            row.len(),
            columns.len()
        )));
    }

    Ok(columns
        .iter()
        .cloned()
        .zip(row.into_iter().map(Value::String))
        .collect())
}

fn key_from_values(
    ctx: &MutationContext,
    values: &BTreeMap<String, Value>,
) -> Result<StructuredKey> {
    let pairs = ctx
        .primary_keys
        .iter()
        .map(|key| {
            let value = lookup_value_case_insensitive(values, key).ok_or_else(|| {
                Error::msg(format!(
                    "Snowflake row is missing primary key column '{key}'"
                ))
            })?;
            Ok((key.clone(), value.clone()))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StructuredKey::from_pairs(pairs))
}

fn lookup_value_case_insensitive<'a>(
    values: &'a BTreeMap<String, Value>,
    key: &str,
) -> Option<&'a Value> {
    values.get(key).or_else(|| {
        values
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
            .map(|(_, value)| value)
    })
}

fn metadata_index(columns: &[String], name: &str) -> Result<usize> {
    columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::msg(format!("Snowflake change query missing {name} column")))
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(Error::msg(format!(
            "Snowflake boolean metadata value is invalid: {other}"
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    Insert,
    Upsert,
    Delete,
}

#[cfg(test)]
pub(crate) fn test_mutations(count: usize, checkpoint: &str) -> Result<Vec<LogicalMutation>> {
    let metadata = TableMetadata {
        primary_keys: vec!["id".to_string()],
        columns: vec!["id".to_string(), "name".to_string()],
        schema_fingerprint: "fingerprint-v1".to_string(),
    };
    let ctx = MutationContext::new(
        TableId::from("customer_state.customer_state"),
        "snowflake.config.local_snowflake".to_string(),
        TableMode::AppendOnly,
        &metadata,
        CheckpointId::from(checkpoint),
        "01b12345-0600-1234-0000-000000000000".to_string(),
    );
    let rows = RowSet {
        query_id: "snapshot-query".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        rows: (0..count)
            .map(|index| vec![index.to_string(), format!("name-{index}")])
            .collect(),
    };

    snapshot_rows_to_mutations(ctx, rows)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    fn change_row(
        row_id: &str,
        action: &str,
        is_update: bool,
        values: Vec<(&str, &str)>,
    ) -> super::ChangeRow {
        super::ChangeRow {
            metadata_row_id: row_id.to_string(),
            metadata_action: action.to_string(),
            metadata_is_update: is_update,
            values: values
                .into_iter()
                .map(|(name, value)| (name.to_string(), Value::String(value.to_string())))
                .collect(),
        }
    }

    #[test]
    fn collapses_standard_stream_update_pair_by_row_id() {
        let rows = vec![
            change_row(
                "row-1",
                "DELETE",
                true,
                vec![("id", "1"), ("name", "before")],
            ),
            change_row(
                "row-1",
                "INSERT",
                true,
                vec![("id", "1"), ("name", "after")],
            ),
        ];

        let collapsed = super::collapse_change_rows(super::MutationContext::for_test(), rows)
            .expect("collapse");

        assert_eq!(collapsed.len(), 1);
        assert_eq!(
            collapsed[0]
                .after
                .as_ref()
                .and_then(object_field)
                .and_then(|object| object.get("name")),
            Some(&Value::String("after".to_string()))
        );
        assert_eq!(
            collapsed[0]
                .before
                .as_ref()
                .and_then(object_field)
                .and_then(|object| object.get("name")),
            Some(&Value::String("before".to_string()))
        );
    }

    #[test]
    fn rejects_broken_update_pair() {
        let rows = vec![change_row(
            "row-1",
            "DELETE",
            true,
            vec![("id", "1"), ("name", "before")],
        )];

        let err = super::collapse_change_rows(super::MutationContext::for_test(), rows)
            .expect_err("broken pair should fail");

        assert!(err.to_string().contains("update pair"));
    }

    fn object_field(value: &Value) -> Option<&std::collections::BTreeMap<String, Value>> {
        match value {
            Value::Object(object) => Some(object),
            _ => None,
        }
    }
}
