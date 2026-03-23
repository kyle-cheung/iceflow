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

    if key_column_membership_or_order_changed(current, next) {
        return SchemaDecision::Quarantine("key-column-membership-or-order-change");
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
            (Some(current_column), Some(next_column)) => {
                current_column.data_type != next_column.data_type
            }
            _ => false,
        }
    })
}

pub fn key_column_membership_or_order_changed(current: &Schema, next: &Schema) -> bool {
    current.key_columns != next.key_columns
}

pub fn only_additive_or_allowed_widening(current: &Schema, next: &Schema) -> bool {
    current
        .columns
        .iter()
        .all(|current_column| match next.column(&current_column.name) {
            Some(next_column) => {
                current_column.data_type == next_column.data_type
                    || is_allowed_widening(&current_column.data_type, &next_column.data_type)
            }
            None => false,
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

    fn schema_with_keys(columns: Vec<SchemaColumn>, key_columns: Vec<&str>) -> Schema {
        Schema::new(
            columns,
            key_columns.into_iter().map(|key| key.to_string()).collect(),
        )
    }

    #[test]
    fn schema_policy_rejects_key_column_widening() {
        let current = schema_with_keys(
            vec![SchemaColumn {
                name: "customer_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            vec!["customer_id"],
        );
        let next = schema_with_keys(
            vec![SchemaColumn {
                name: "customer_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            }],
            vec!["customer_id"],
        );

        assert_eq!(
            evaluate_schema_policy(&current, &next),
            SchemaDecision::Quarantine("key-column-type-change")
        );
    }

    #[test]
    fn schema_policy_rejects_key_column_set_change() {
        let current = schema_with_keys(
            vec![
                SchemaColumn {
                    name: "tenant_id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
                SchemaColumn {
                    name: "customer_id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
            ],
            vec!["tenant_id", "customer_id"],
        );
        let next = schema_with_keys(
            vec![
                SchemaColumn {
                    name: "tenant_id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
                SchemaColumn {
                    name: "customer_id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
                SchemaColumn {
                    name: "region_id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
            ],
            vec!["tenant_id", "customer_id", "region_id"],
        );

        assert_eq!(
            evaluate_schema_policy(&current, &next),
            SchemaDecision::Quarantine("key-column-membership-or-order-change")
        );
    }

    #[test]
    fn schema_policy_rejects_key_column_order_change() {
        let current = schema_with_keys(
            vec![
                SchemaColumn {
                    name: "tenant_id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
                SchemaColumn {
                    name: "customer_id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
            ],
            vec!["tenant_id", "customer_id"],
        );
        let next = schema_with_keys(
            vec![
                SchemaColumn {
                    name: "tenant_id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
                SchemaColumn {
                    name: "customer_id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
            ],
            vec!["customer_id", "tenant_id"],
        );

        assert_eq!(
            evaluate_schema_policy(&current, &next),
            SchemaDecision::Quarantine("key-column-membership-or-order-change")
        );
    }
}
