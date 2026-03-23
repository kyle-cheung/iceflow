#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<SchemaColumn>,
    pub key_columns: Vec<String>,
}

impl Schema {
    pub fn new(columns: Vec<SchemaColumn>, key_columns: Vec<String>) -> Self {
        Self {
            columns,
            key_columns,
        }
    }

    pub fn column(&self, name: &str) -> Option<&SchemaColumn> {
        self.columns.iter().find(|column| column.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaDecision {
    Allow,
    Quarantine(&'static str),
}

pub fn evaluate_schema_policy(current: &Schema, next: &Schema) -> SchemaDecision {
    if key_column_type_changed(current, next) {
        return SchemaDecision::Quarantine("key-column-type-change");
    }

    if only_additive_or_allowed_widening(current, next) {
        SchemaDecision::Allow
    } else {
        SchemaDecision::Quarantine("incompatible-schema-change")
    }
}

pub fn key_column_type_changed(current: &Schema, next: &Schema) -> bool {
    current.key_columns.iter().any(|key_column| {
        match (current.column(key_column), next.column(key_column)) {
            (Some(current_column), Some(next_column)) => current_column.data_type != next_column.data_type,
            _ => false,
        }
    })
}

pub fn only_additive_or_allowed_widening(current: &Schema, next: &Schema) -> bool {
    current.columns.iter().all(|current_column| {
        match next.column(&current_column.name) {
            Some(next_column) => {
                current_column.data_type == next_column.data_type
                    || is_allowed_widening(&current_column.data_type, &next_column.data_type)
            }
            None => false,
        }
    })
}

fn is_allowed_widening(current: &DataType, next: &DataType) -> bool {
    matches!(
        (current, next),
        (DataType::Int32, DataType::Int64) | (DataType::Float32, DataType::Float64)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn schema_policy_rejects_key_column_widening() {
        let current = schema_with_key("customer_id", DataType::Int32);
        let next = schema_with_key("customer_id", DataType::Int64);

        assert_eq!(
            evaluate_schema_policy(&current, &next),
            SchemaDecision::Quarantine("key-column-type-change")
        );
    }
}
