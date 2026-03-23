use crate::TableMode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceWorkload {
    pub name: &'static str,
    pub table_mode: TableMode,
    pub batch_count: u32,
    pub rows_per_batch: u32,
    pub column_count: u32,
    pub approximate_row_width_bytes: u32,
    pub partitioning: &'static str,
    pub key_columns: &'static [&'static str],
    pub ordering_field: Option<&'static str>,
    pub update_rate_percent: Option<u8>,
    pub delete_rate_percent: Option<u8>,
}

pub const APPEND_ONLY_ORDERS_EVENTS: ReferenceWorkload = ReferenceWorkload {
    name: "append_only.orders_events",
    table_mode: TableMode::AppendOnly,
    batch_count: 100,
    rows_per_batch: 25_000,
    column_count: 24,
    approximate_row_width_bytes: 512,
    partitioning: "ingestion-day",
    key_columns: &[],
    ordering_field: None,
    update_rate_percent: None,
    delete_rate_percent: None,
};

pub const KEYED_UPSERT_CUSTOMER_STATE: ReferenceWorkload = ReferenceWorkload {
    name: "keyed_upsert.customer_state",
    table_mode: TableMode::KeyedUpsert,
    batch_count: 100,
    rows_per_batch: 10_000,
    column_count: 18,
    approximate_row_width_bytes: 768,
    partitioning: "ingestion-day",
    key_columns: &["tenant_id", "customer_id"],
    ordering_field: Some("source_position"),
    update_rate_percent: Some(20),
    delete_rate_percent: Some(10),
};

pub const REFERENCE_WORKLOAD_V0: [ReferenceWorkload; 2] = [
    APPEND_ONLY_ORDERS_EVENTS,
    KEYED_UPSERT_CUSTOMER_STATE,
];
