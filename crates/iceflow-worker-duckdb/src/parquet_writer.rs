use crate::normalize::NormalizedBatch;
use anyhow::{Error, Result};
use chrono::{DateTime, Utc};
use iceflow_types::BatchManifest;
use iceflow_types::{BatchId, LogicalMutation, ManifestFile, Operation};
use libduckdb_sys::{
    duckdb_append_int32, duckdb_append_int64, duckdb_append_null, duckdb_append_timestamp,
    duckdb_append_varchar_length, duckdb_appender, duckdb_appender_begin_row,
    duckdb_appender_close, duckdb_appender_create, duckdb_appender_destroy,
    duckdb_appender_end_row, duckdb_appender_error, duckdb_close, duckdb_connect,
    duckdb_connection, duckdb_database, duckdb_disconnect, duckdb_open, duckdb_query,
    duckdb_result, duckdb_result_error, duckdb_row_count, duckdb_state_DuckDBSuccess,
    duckdb_timestamp, duckdb_value_int64, DuckDBSuccess,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::fs;
use std::mem;
use std::os::raw::c_char;
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterConfig {
    pub target_file_bytes: u64,
    pub max_row_group_bytes: u64,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            target_file_bytes: 256 * 1024 * 1024,
            max_row_group_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedBatch {
    pub manifest: BatchManifest,
}

pub fn materialize_batch(
    config: &WriterConfig,
    normalized: NormalizedBatch,
) -> Result<MaterializedBatch> {
    let content_hash = batch_content_hash(normalized.records());
    let batch_id = batch_id_from_batch_label(normalized.batch_label(), &content_hash);
    let created_at = Utc::now();
    let output_dir = create_output_dir(batch_id.as_str())?;
    let average_record_bytes = average_record_bytes(normalized.records());
    let rows_per_file = rows_for_target(config.target_file_bytes, average_record_bytes);
    let row_group_rows = rows_for_target(config.max_row_group_bytes, average_record_bytes);
    let mut file_set = Vec::new();
    let db = DuckDb::open_in_memory()?;

    for (chunk_index, records) in normalized.records().chunks(rows_per_file).enumerate() {
        let table_name = format!(
            "materialized_{}_{}",
            sanitize_identifier(batch_id.as_str()),
            chunk_index
        );
        let file_path =
            output_dir.join(format!("{}-{:04}.parquet", batch_id.as_str(), chunk_index));

        db.create_staging_table(&table_name)?;
        db.append_records(&table_name, records)?;
        db.copy_table_to_parquet(&table_name, &file_path, row_group_rows)?;
        db.drop_table(&table_name)?;

        file_set.push(manifest_file(
            &file_path,
            records.len() as u64,
            created_at.clone(),
        )?);
    }

    let manifest = BatchManifest {
        batch_id,
        table_id: normalized.table_id().clone(),
        table_mode: normalized.table_mode(),
        source_id: normalized.source_id().to_string(),
        source_class: normalized.source_class(),
        source_checkpoint_start: normalized.source_checkpoint_start().clone(),
        source_checkpoint_end: normalized.source_checkpoint_end().clone(),
        ordering_field: normalized.ordering_field().to_string(),
        ordering_min: normalized.ordering_min(),
        ordering_max: normalized.ordering_max(),
        schema_version: normalized.schema_version(),
        schema_fingerprint: schema_fingerprint(normalized.records()),
        record_count: normalized.record_count() as u64,
        op_counts: op_counts(normalized.records()),
        file_set,
        content_hash,
        created_at,
    };

    Ok(MaterializedBatch { manifest })
}

pub(crate) struct DuckDb {
    database: duckdb_database,
    connection: duckdb_connection,
}

impl DuckDb {
    pub(crate) fn open_in_memory() -> Result<Self> {
        let mut database = ptr::null_mut();
        let open_state = unsafe { duckdb_open(ptr::null(), &mut database) };
        if open_state != DuckDBSuccess {
            anyhow::bail!("duckdb_open failed");
        }

        let mut connection = ptr::null_mut();
        let connect_state = unsafe { duckdb_connect(database, &mut connection) };
        if connect_state != DuckDBSuccess {
            unsafe {
                duckdb_close(&mut database);
            }
            anyhow::bail!("duckdb_connect failed");
        }

        Ok(Self {
            database,
            connection,
        })
    }

    fn create_staging_table(&self, table_name: &str) -> Result<()> {
        self.execute(&format!(
            "CREATE TEMP TABLE {table_name} (
                table_id VARCHAR,
                source_id VARCHAR,
                source_class VARCHAR,
                table_mode VARCHAR,
                op VARCHAR,
                key_json VARCHAR,
                after_json VARCHAR,
                before_json VARCHAR,
                ordering_field VARCHAR,
                ordering_value BIGINT,
                source_checkpoint VARCHAR,
                source_event_id VARCHAR,
                schema_version INTEGER,
                ingestion_ts TIMESTAMP,
                source_metadata_json VARCHAR
            )"
        ))
    }

    fn append_records(&self, table_name: &str, records: &[LogicalMutation]) -> Result<()> {
        let table_name_str = table_name.to_string();
        let table_name = c_string(table_name)?;
        let mut raw_appender: duckdb_appender = ptr::null_mut();
        let state = unsafe {
            duckdb_appender_create(
                self.connection,
                ptr::null(),
                table_name.as_ptr(),
                &mut raw_appender,
            )
        };
        if state != DuckDBSuccess {
            anyhow::bail!("duckdb_appender_create failed for {}", table_name_str);
        }

        let mut appender = DuckDbAppender { raw: raw_appender };
        for record in records {
            appender.begin_row()?;
            appender.append_string(record.table_id.as_str())?;
            appender.append_string(&record.source_id)?;
            appender.append_string(record.source_class.stable_tag())?;
            appender.append_string(record.table_mode.stable_tag())?;
            appender.append_string(record.op.stable_tag())?;
            appender.append_string(&key_json(record))?;
            appender.append_optional_string(record.after.as_ref().map(json_text).as_deref())?;
            appender.append_optional_string(record.before.as_ref().map(json_text).as_deref())?;
            appender.append_string(&record.ordering_field)?;
            appender.append_int64(record.ordering_value)?;
            appender.append_string(record.source_checkpoint.as_str())?;
            appender.append_optional_string(record.source_event_id.as_deref())?;
            appender.append_int32(record.schema_version)?;
            appender.append_timestamp(&record.ingestion_ts)?;
            appender.append_string(&source_metadata_json(&record.source_metadata))?;
            appender.end_row()?;
        }
        appender.close()?;
        Ok(())
    }

    fn copy_table_to_parquet(
        &self,
        table_name: &str,
        file_path: &Path,
        row_group_rows: usize,
    ) -> Result<()> {
        self.execute(&format!(
            "COPY (SELECT * FROM {table_name}) TO '{}' (FORMAT PARQUET, ROW_GROUP_SIZE {})",
            escape_sql_string(&file_path.display().to_string()),
            row_group_rows
        ))
    }

    pub(crate) fn drop_table(&self, table_name: &str) -> Result<()> {
        self.execute(&format!("DROP TABLE IF EXISTS {table_name}"))
    }

    pub(crate) fn create_parquet_view(
        &self,
        view_name: &str,
        input_files: &[PathBuf],
    ) -> Result<()> {
        if input_files.is_empty() {
            anyhow::bail!("input_files cannot be empty");
        }

        let file_list = input_files
            .iter()
            .map(|path| format!("'{}'", escape_sql_string(&path.display().to_string())))
            .collect::<Vec<_>>()
            .join(",");
        self.execute(&format!(
            "CREATE TEMP VIEW {view_name} AS SELECT * FROM read_parquet([{file_list}])"
        ))
    }

    pub(crate) fn create_temp_table_from_source(
        &self,
        table_name: &str,
        source_name: &str,
    ) -> Result<()> {
        self.execute(&format!(
            "CREATE TEMP TABLE {table_name} AS SELECT * FROM {source_name}"
        ))
    }

    pub(crate) fn count_rows(&self, source_name: &str) -> Result<u64> {
        let query = c_string(&format!(
            "SELECT COUNT(*)::BIGINT AS row_count FROM {source_name}"
        ))?;
        let mut result: duckdb_result = unsafe { mem::zeroed() };
        let state = unsafe { duckdb_query(self.connection, query.as_ptr(), &mut result) };
        if state != DuckDBSuccess {
            let message = unsafe { c_string_from_ptr(duckdb_result_error(&mut result)) };
            unsafe {
                libduckdb_sys::duckdb_destroy_result(&mut result);
            }
            anyhow::bail!("duckdb_query failed: {}", message);
        }

        let rows = unsafe { duckdb_row_count(&mut result) };
        if rows == 0 {
            unsafe {
                libduckdb_sys::duckdb_destroy_result(&mut result);
            }
            anyhow::bail!("duckdb_query returned no rows");
        }

        let count = unsafe { duckdb_value_int64(&mut result, 0, 0) };
        unsafe {
            libduckdb_sys::duckdb_destroy_result(&mut result);
        }
        Ok(count.max(0) as u64)
    }

    pub(crate) fn copy_table_slice_to_parquet(
        &self,
        table_name: &str,
        file_path: &Path,
        row_group_rows: usize,
        chunk_rows: usize,
        offset: u64,
    ) -> Result<()> {
        self.execute(&format!(
            "COPY ({}) TO '{}' (FORMAT PARQUET, ROW_GROUP_SIZE {})",
            copy_table_slice_query(table_name, chunk_rows.max(1), offset),
            escape_sql_string(&file_path.display().to_string()),
            row_group_rows.max(1)
        ))
    }

    pub(crate) fn drop_view(&self, view_name: &str) -> Result<()> {
        self.execute(&format!("DROP VIEW IF EXISTS {view_name}"))
    }

    #[cfg(test)]
    pub(crate) fn query_i64(&self, sql: &str) -> Result<i64> {
        let query = c_string(sql)?;
        let mut result: duckdb_result = unsafe { mem::zeroed() };
        let state = unsafe { duckdb_query(self.connection, query.as_ptr(), &mut result) };
        if state != DuckDBSuccess {
            let message = unsafe { c_string_from_ptr(duckdb_result_error(&mut result)) };
            unsafe {
                libduckdb_sys::duckdb_destroy_result(&mut result);
            }
            anyhow::bail!("duckdb_query failed: {}", message);
        }

        let rows = unsafe { duckdb_row_count(&mut result) };
        if rows == 0 {
            unsafe {
                libduckdb_sys::duckdb_destroy_result(&mut result);
            }
            anyhow::bail!("duckdb_query returned no rows");
        }

        let value = unsafe { duckdb_value_int64(&mut result, 0, 0) };
        unsafe {
            libduckdb_sys::duckdb_destroy_result(&mut result);
        }
        Ok(value)
    }

    fn execute(&self, sql: &str) -> Result<()> {
        let query = c_string(sql)?;
        let mut result: duckdb_result = unsafe { mem::zeroed() };
        let state = unsafe { duckdb_query(self.connection, query.as_ptr(), &mut result) };
        if state != DuckDBSuccess {
            let message = unsafe { c_string_from_ptr(duckdb_result_error(&mut result)) };
            unsafe {
                libduckdb_sys::duckdb_destroy_result(&mut result);
            }
            anyhow::bail!("duckdb_query failed: {}", message);
        }
        unsafe {
            libduckdb_sys::duckdb_destroy_result(&mut result);
        }
        Ok(())
    }
}

impl Drop for DuckDb {
    fn drop(&mut self) {
        unsafe {
            if !self.connection.is_null() {
                duckdb_disconnect(&mut self.connection);
            }
            if !self.database.is_null() {
                duckdb_close(&mut self.database);
            }
        }
    }
}

struct DuckDbAppender {
    raw: duckdb_appender,
}

impl DuckDbAppender {
    fn begin_row(&mut self) -> Result<()> {
        self.state(unsafe { duckdb_appender_begin_row(self.raw) })
    }

    fn end_row(&mut self) -> Result<()> {
        self.state(unsafe { duckdb_appender_end_row(self.raw) })
    }

    fn append_string(&mut self, value: &str) -> Result<()> {
        self.state(unsafe {
            duckdb_append_varchar_length(
                self.raw,
                value.as_ptr() as *const c_char,
                value.len() as u64,
            )
        })
    }

    fn append_optional_string(&mut self, value: Option<&str>) -> Result<()> {
        match value {
            Some(value) => self.append_string(value),
            None => self.state(unsafe { duckdb_append_null(self.raw) }),
        }
    }

    fn append_int64(&mut self, value: i64) -> Result<()> {
        self.state(unsafe { duckdb_append_int64(self.raw, value) })
    }

    fn append_int32(&mut self, value: i32) -> Result<()> {
        self.state(unsafe { duckdb_append_int32(self.raw, value) })
    }

    fn append_timestamp(&mut self, value: &DateTime<Utc>) -> Result<()> {
        self.state(unsafe { duckdb_append_timestamp(self.raw, timestamp(value)) })
    }

    fn close(&mut self) -> Result<()> {
        self.state(unsafe { duckdb_appender_close(self.raw) })
    }

    fn state(&self, state: libduckdb_sys::duckdb_state) -> Result<()> {
        if state == duckdb_state_DuckDBSuccess {
            return Ok(());
        }

        let message = unsafe { c_string_from_ptr(duckdb_appender_error(self.raw)) };
        anyhow::bail!("duckdb_appender error: {}", message);
    }
}

impl Drop for DuckDbAppender {
    fn drop(&mut self) {
        unsafe {
            if !self.raw.is_null() {
                duckdb_appender_destroy(&mut self.raw);
            }
        }
    }
}

pub(crate) fn rows_for_target(target_bytes: u64, average_record_bytes: usize) -> usize {
    if target_bytes == 0 {
        return 1;
    }

    ((target_bytes as usize) / average_record_bytes.max(1)).max(1)
}

pub(crate) fn copy_table_slice_query(table_name: &str, chunk_rows: usize, offset: u64) -> String {
    format!(
        "SELECT * FROM {table_name} ORDER BY rowid LIMIT {} OFFSET {}",
        chunk_rows.max(1),
        offset
    )
}

fn average_record_bytes(records: &[LogicalMutation]) -> usize {
    if records.is_empty() {
        return 1;
    }

    let total: usize = records.iter().map(estimated_record_size).sum();
    (total / records.len()).max(1)
}

fn estimated_record_size(record: &LogicalMutation) -> usize {
    mutation_signature(record).len().max(1)
}

pub(crate) fn manifest_file(
    path: &Path,
    record_count: u64,
    created_at: DateTime<Utc>,
) -> Result<ManifestFile> {
    let content_hash = file_hash(path)?;
    let metadata =
        fs::metadata(path).map_err(|err| Error::msg(format!("{}: {err}", path.display())))?;

    Ok(ManifestFile {
        file_uri: file_uri(path),
        file_kind: "parquet".to_string(),
        content_hash,
        file_size_bytes: metadata.len(),
        record_count,
        created_at,
    })
}

fn batch_content_hash(records: &[LogicalMutation]) -> String {
    stable_hash(records.iter().map(mutation_signature))
}

fn op_counts(records: &[LogicalMutation]) -> BTreeMap<Operation, u64> {
    let mut counts = BTreeMap::new();
    for record in records {
        *counts.entry(record.op).or_insert(0) += 1;
    }
    counts
}

fn schema_fingerprint(records: &[LogicalMutation]) -> String {
    let mut parts = Vec::new();
    for record in records {
        for part in &record.key.parts {
            parts.push(format!("key:{}:{}", part.name, value_type(&part.value)));
        }
        if let Some(after) = &record.after {
            collect_object_types("after", after, &mut parts);
        }
        if let Some(before) = &record.before {
            collect_object_types("before", before, &mut parts);
        }
    }
    parts.sort();
    parts.dedup();
    stable_hash(parts)
}

fn collect_object_types(prefix: &str, value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(entries) => {
            for (key, value) in entries {
                parts.push(format!("{prefix}:{key}:{}", value_type(value)));
            }
        }
        _ => {
            parts.push(format!("{prefix}:scalar:{}", value_type(value)));
        }
    }
}

fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn batch_id_from_batch_label(batch_label: Option<&str>, content_hash: &str) -> BatchId {
    if let Some(batch_label) = batch_label {
        let stem = Path::new(batch_label)
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("batch");
        return BatchId::from(stem.to_string());
    }

    BatchId::from(format!("batch-{content_hash}"))
}

fn create_output_dir(batch_id: &str) -> Result<PathBuf> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let root = std::env::temp_dir()
        .join("iceflow-worker-duckdb")
        .join(format!("{batch_id}-{nonce}"));
    fs::create_dir_all(&root).map_err(|err| Error::msg(format!("{}: {err}", root.display())))?;
    Ok(root)
}

pub(crate) fn sanitize_identifier(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    if sanitized.is_empty() {
        "batch".to_string()
    } else {
        sanitized
    }
}

fn c_string(value: &str) -> Result<CString> {
    CString::new(value).map_err(|_| Error::msg("string contains interior NUL byte"))
}

unsafe fn c_string_from_ptr(value: *const c_char) -> String {
    if value.is_null() {
        return "unknown error".to_string();
    }

    unsafe { CStr::from_ptr(value) }
        .to_string_lossy()
        .into_owned()
}

fn timestamp(value: &DateTime<Utc>) -> duckdb_timestamp {
    duckdb_timestamp {
        micros: value.timestamp() * 1_000_000 + (value.timestamp_subsec_nanos() as i64 / 1_000),
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn file_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

fn file_hash(path: &Path) -> Result<String> {
    let bytes = fs::read(path).map_err(|err| Error::msg(format!("{}: {err}", path.display())))?;
    Ok(stable_hash_bytes(&bytes))
}

fn mutation_signature(record: &LogicalMutation) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        record.table_id,
        record.source_id,
        record.op.stable_tag(),
        key_json(record),
        optional_json_text(record.after.as_ref()),
        optional_json_text(record.before.as_ref()),
        record.ordering_field,
        record.ordering_value,
        record.source_checkpoint,
        source_metadata_json(&record.source_metadata),
    )
}

fn key_json(record: &LogicalMutation) -> String {
    let mut json = String::from("{");
    for (index, part) in record.key.parts.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push('"');
        json.push_str(&escape_json_string(&part.name));
        json.push_str("\":");
        json.push_str(&json_text(&part.value));
    }
    json.push('}');
    json
}

fn optional_json_text(value: Option<&Value>) -> String {
    value.map(json_text).unwrap_or_else(|| "null".to_string())
}

fn source_metadata_json(metadata: &BTreeMap<String, String>) -> String {
    let mut json = String::from("{");
    for (index, (key, value)) in metadata.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push('"');
        json.push_str(&escape_json_string(key));
        json.push_str("\":\"");
        json.push_str(&escape_json_string(value));
        json.push('"');
    }
    json.push('}');
    json
}

fn json_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => format!("\"{}\"", escape_json_string(value)),
        Value::Array(values) => {
            let items = values.iter().map(json_text).collect::<Vec<_>>().join(",");
            format!("[{items}]")
        }
        Value::Object(values) => {
            let items = values
                .iter()
                .map(|(key, value)| format!("\"{}\":{}", escape_json_string(key), json_text(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{items}}}")
        }
    }
}

fn escape_json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\u{0008}' => escaped.push_str("\\b"),
            '\u{000c}' => escaped.push_str("\\f"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch <= '\u{001f}' => {
                escaped.push_str(&format!("\\u{:04x}", ch as u32));
            }
            ch => escaped.push(ch),
        }
    }
    escaped
}

fn stable_hash(parts: impl IntoIterator<Item = String>) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn stable_hash_bytes(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use iceflow_source::SourceBatch;
    use iceflow_types::{
        checkpoint, key, ordering, table_id, LogicalMutation, Operation, SourceClass,
        StructuredKey, TableMode,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    use crate::parquet_writer::{copy_table_slice_query, json_text};
    use crate::test_support::run_ready;
    use crate::{DuckDbWorker, WriterConfig};

    #[test]
    fn parquet_manifest_freezes_ordering_span_and_checksum() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let output = run_ready(worker.materialize(sample_customer_state_batch()))
            .expect("materialized batch");

        assert_eq!(output.manifest.ordering_min, 1);
        assert_eq!(output.manifest.ordering_max, 10_000);
        assert_eq!(output.manifest.record_count, 2);
        assert_eq!(output.manifest.op_counts.get(&Operation::Upsert), Some(&1));
        assert_eq!(output.manifest.op_counts.get(&Operation::Delete), Some(&1));
        assert!(!output.manifest.content_hash.is_empty());
        assert_eq!(output.manifest.file_set.len(), 1);
    }

    #[test]
    fn parquet_writer_rolls_files_when_target_budget_is_small() {
        let worker = DuckDbWorker::with_config(WriterConfig {
            target_file_bytes: 1,
            max_row_group_bytes: 1,
        })
        .expect("worker");
        let output =
            run_ready(worker.materialize(sample_append_only_batch())).expect("materialized batch");

        assert!(output.manifest.file_set.len() >= 2);
        assert_eq!(output.manifest.record_count, 3);
    }

    #[test]
    fn parquet_writer_uses_content_hash_when_batch_label_is_missing() {
        let worker = DuckDbWorker::in_memory().expect("worker");
        let output =
            run_ready(worker.materialize(sample_label_less_batch())).expect("materialized batch");

        assert_eq!(
            output.manifest.batch_id.as_str(),
            format!("batch-{}", output.manifest.content_hash)
        );
    }

    #[test]
    fn json_text_escapes_all_control_characters() {
        let value = serde_json::Value::String("bad\u{0008}\u{000c}\u{0001}\n\r\t".to_string());

        assert_eq!(json_text(&value), "\"bad\\b\\f\\u0001\\n\\r\\t\"");
    }

    #[test]
    fn copy_table_slice_query_orders_by_rowid_for_stable_chunking() {
        let query = copy_table_slice_query("compaction_rows", 10, 20);

        assert!(query.contains("ORDER BY rowid"));
        assert!(query.contains("LIMIT 10 OFFSET 20"));
    }

    fn sample_customer_state_batch() -> SourceBatch {
        SourceBatch {
            batch_label: Some("fixtures/customer_state/batch-0003.jsonl".to_string()),
            checkpoint_start: Some(checkpoint("cp-1")),
            checkpoint_end: checkpoint("cp-10000"),
            records: vec![
                customer_upsert(7, 1, Some(json!({ "customer_id": 7, "status": "trial" }))),
                customer_upsert(
                    7,
                    10_000,
                    Some(json!({ "customer_id": 7, "status": "active" })),
                ),
                customer_delete(8, 5_000),
            ],
        }
    }

    fn sample_append_only_batch() -> SourceBatch {
        SourceBatch {
            batch_label: Some("fixtures/orders/batch-0001.jsonl".to_string()),
            checkpoint_start: Some(checkpoint("cp-11")),
            checkpoint_end: checkpoint("cp-13"),
            records: vec![
                append_insert(1, 11),
                append_insert(2, 12),
                append_insert(3, 13),
            ],
        }
    }

    fn sample_label_less_batch() -> SourceBatch {
        SourceBatch {
            batch_label: None,
            checkpoint_start: Some(checkpoint("cp-11")),
            checkpoint_end: checkpoint("cp-13"),
            records: vec![
                append_insert(1, 11),
                append_insert(2, 12),
                append_insert(3, 13),
            ],
        }
    }

    fn customer_upsert(
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

    fn customer_delete(customer_id: i64, ordering_value: i64) -> LogicalMutation {
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
            StructuredKey::new(vec![]),
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
