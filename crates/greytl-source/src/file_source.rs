use crate::adapter::{
    CheckReport, CheckpointAck, DiscoverReport, SnapshotRequest, SourceAdapter, SourceBatch,
    SourceSpec,
};
use anyhow::{Error, Result};
use chrono::{DateTime, Utc};
use greytl_types::{
    ordering, CheckpointId, LogicalMutation, Operation, SourceClass, StructuredKey, TableId,
    TableMode,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct FileSource {
    fixture_dir_display: String,
    fixture_dir: PathBuf,
    source_id: String,
    table_id: String,
    table_mode: TableMode,
    source_class: SourceClass,
    ordering_field: String,
    last_checkpoint: Arc<Mutex<Option<CheckpointId>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkloadKind {
    CustomerState,
    OrdersEvents,
    Unknown,
}

impl FileSource {
    pub fn from_fixture_dir(fixture_dir: impl Into<PathBuf>) -> Self {
        let fixture_dir = fixture_dir.into();
        let fixture_dir_display = fixture_dir.to_string_lossy().into_owned();
        let fixture_dir = resolve_fixture_dir(&fixture_dir);
        let kind = workload_kind(&fixture_dir);
        let table_id = fixture_dir
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("unknown")
            .to_string();

        Self {
            fixture_dir_display,
            fixture_dir,
            source_id: format!("file.{table_id}"),
            table_id,
            table_mode: match kind {
                WorkloadKind::CustomerState => TableMode::KeyedUpsert,
                WorkloadKind::OrdersEvents | WorkloadKind::Unknown => TableMode::AppendOnly,
            },
            source_class: match kind {
                WorkloadKind::CustomerState => SourceClass::DatabaseCdc,
                WorkloadKind::OrdersEvents | WorkloadKind::Unknown => SourceClass::FileOrObjectDrop,
            },
            ordering_field: "source_position".to_string(),
            last_checkpoint: Arc::new(Mutex::new(None)),
        }
    }

    fn spec_sync(&self) -> SourceSpec {
        SourceSpec {
            source_id: self.source_id.clone(),
            fixture_dir: self.fixture_dir_display.clone(),
            source_class: self.source_class,
            table_id: self.table_id.clone(),
            table_mode: self.table_mode,
            ordering_field: match self.table_mode {
                TableMode::AppendOnly => None,
                TableMode::KeyedUpsert => Some(self.ordering_field.clone()),
            },
            batch_count: batch_files(&self.fixture_dir).len(),
        }
    }

    fn check_sync(&self) -> Result<CheckReport> {
        let mut record_count = 0;
        for batch_file in batch_files(&self.fixture_dir) {
            record_count += self.load_batch_file(&batch_file)?.records.len();
        }

        Ok(CheckReport {
            fixture_dir: self.fixture_dir_display.clone(),
            batch_count: batch_files(&self.fixture_dir).len(),
            record_count,
        })
    }

    fn discover_sync(&self) -> Result<DiscoverReport> {
        Ok(DiscoverReport {
            fixture_dir: self.fixture_dir_display.clone(),
            batch_files: batch_files(&self.fixture_dir)
                .into_iter()
                .map(|path| {
                    path.file_name()
                        .and_then(|value| value.to_str())
                        .unwrap_or_default()
                        .to_string()
                })
                .collect(),
        })
    }

    fn load_batch_file(&self, batch_file: &Path) -> Result<SourceBatch> {
        let content = fs::read_to_string(batch_file)
            .map_err(|err| Error::msg(format!("{}: {err}", batch_file.display())))?;
        let mut records = Vec::new();

        for (line_no, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let parsed = parse_json_value(line).map_err(|err| {
                Error::msg(format!("{}:{}: {err}", batch_file.display(), line_no + 1))
            })?;
            let record = record_from_value(
                &self.source_id,
                &self.table_id,
                self.table_mode,
                self.source_class,
                &self.ordering_field,
                parsed,
            )?;
            records.push(record);
        }

        Ok(SourceBatch {
            batch_file: batch_file
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_string(),
            records,
        })
    }

    fn batch_file_path(&self, batch_index: usize) -> PathBuf {
        self.fixture_dir
            .join(format!("batch-{batch_index:04}.jsonl"))
    }
}

#[allow(async_fn_in_trait)]
impl SourceAdapter for FileSource {
    async fn spec(&self) -> SourceSpec {
        self.spec_sync()
    }

    async fn check(&self) -> Result<CheckReport> {
        self.check_sync()
    }

    async fn discover(&self) -> Result<DiscoverReport> {
        self.discover_sync()
    }

    async fn snapshot(&self, req: SnapshotRequest) -> Result<Option<SourceBatch>> {
        let batch_file = self.batch_file_path(req.batch_index);
        if !batch_file.exists() {
            return Ok(None);
        }

        self.load_batch_file(&batch_file).map(Some)
    }

    async fn checkpoint(&self, ack: CheckpointAck) -> Result<()> {
        if ack.source_id.trim().is_empty() {
            return Err(Error::msg("checkpoint ack source_id is required"));
        }
        if ack.snapshot.uri.trim().is_empty() {
            return Err(Error::msg("checkpoint ack snapshot uri is required"));
        }
        if ack.source_id != self.source_id {
            return Err(Error::msg("checkpoint ack source_id does not match source"));
        }

        let mut last_checkpoint = self
            .last_checkpoint
            .lock()
            .map_err(|_| Error::msg("checkpoint state lock poisoned"))?;
        if let Some(previous) = last_checkpoint.as_ref() {
            if ack.checkpoint < previous.clone() {
                return Err(Error::msg("checkpoint regression is not allowed"));
            }
        }
        *last_checkpoint = Some(ack.checkpoint);

        Ok(())
    }
}

fn workload_kind(fixture_dir: &Path) -> WorkloadKind {
    match fixture_dir
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
    {
        "customer_state" => WorkloadKind::CustomerState,
        "orders_events" => WorkloadKind::OrdersEvents,
        _ => WorkloadKind::Unknown,
    }
}

fn resolve_fixture_dir(fixture_dir: &Path) -> PathBuf {
    if fixture_dir.exists() {
        return fixture_dir.to_path_buf();
    }

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    let candidate = workspace_root.join(fixture_dir);
    if candidate.exists() {
        return candidate;
    }

    fixture_dir.to_path_buf()
}

fn batch_files(fixture_dir: &Path) -> Vec<PathBuf> {
    let mut batch_files = Vec::new();
    if let Ok(entries) = fs::read_dir(fixture_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .and_then(|value| value.to_str())
                .map(|name| name.starts_with("batch-") && name.ends_with(".jsonl"))
                .unwrap_or(false)
            {
                batch_files.push(path);
            }
        }
    }
    batch_files.sort();
    batch_files
}

fn record_from_value(
    source_id: &str,
    table_id: &str,
    table_mode: TableMode,
    source_class: SourceClass,
    ordering_field: &str,
    value: Value,
) -> Result<LogicalMutation> {
    let object = value_as_object(value)?;
    let op = operation_from_str(value_string(&object, "op")?)?;
    let key = key_from_value(object.get("key"))?;
    let after = object.get("after").and_then(nullable_value);
    let before = object.get("before").and_then(nullable_value);
    let ordering_value = value_i64(&object, "ordering_value")?;
    let source_checkpoint = checkpoint_id(value_string(&object, "source_checkpoint")?);
    let source_event_id = object.get("source_event_id").and_then(value_to_string);
    let schema_version = value_i64(&object, "schema_version")? as i32;
    let ingestion_ts = timestamp_from_value(object.get("ingestion_ts"))?;
    let source_metadata = value_to_string_map(object.get("source_metadata"))?;

    let builder = match op {
        Operation::Insert => LogicalMutation::insert(
            TableId::from(table_id),
            source_id.to_string(),
            source_class,
            table_mode,
            key,
            ordering(ordering_field.to_string(), ordering_value),
            source_checkpoint,
            schema_version,
            ingestion_ts,
            source_metadata,
        ),
        Operation::Upsert => LogicalMutation::upsert(
            TableId::from(table_id),
            source_id.to_string(),
            source_class,
            table_mode,
            key,
            ordering(ordering_field.to_string(), ordering_value),
            source_checkpoint,
            schema_version,
            ingestion_ts,
            source_metadata,
        ),
        Operation::Delete => LogicalMutation::delete(
            TableId::from(table_id),
            source_id.to_string(),
            source_class,
            table_mode,
            key,
            ordering(ordering_field.to_string(), ordering_value),
            source_checkpoint,
            schema_version,
            ingestion_ts,
            source_metadata,
        ),
    };

    let builder = if let Some(after) = after {
        builder.with_after(after)
    } else {
        builder
    };
    let builder = if let Some(before) = before {
        builder.with_before(before)
    } else {
        builder
    };
    let builder = if let Some(source_event_id) = source_event_id {
        builder.with_source_event_id(source_event_id)
    } else {
        builder
    };

    builder.build()
}

fn key_from_value(value: Option<&Value>) -> Result<StructuredKey> {
    let Some(Value::Object(object)) = value else {
        return Ok(StructuredKey::new(vec![]));
    };

    let mut pairs = Vec::new();
    for (name, value) in object {
        pairs.push((name.clone(), value.clone()));
    }
    Ok(StructuredKey::from_pairs(pairs))
}

fn timestamp_from_value(value: Option<&Value>) -> Result<DateTime<Utc>> {
    match value {
        Some(Value::Number(secs)) => DateTime::from_timestamp(*secs, 0)
            .ok_or_else(|| Error::msg(format!("invalid timestamp: {secs}"))),
        Some(Value::String(value)) => {
            let secs = value
                .parse::<i64>()
                .map_err(|_| Error::msg(format!("invalid timestamp: {value}")))?;
            DateTime::from_timestamp(secs, 0)
                .ok_or_else(|| Error::msg(format!("invalid timestamp: {value}")))
        }
        None => Err(Error::msg("ingestion_ts is required")),
        _ => Err(Error::msg("ingestion_ts must be a number or string")),
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn nullable_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        value => Some(value.clone()),
    }
}

fn value_to_string_map(value: Option<&Value>) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    let Some(Value::Object(object)) = value else {
        return Ok(map);
    };
    for (key, value) in object {
        map.insert(
            key.clone(),
            value_to_string(value)
                .ok_or_else(|| Error::msg(format!("metadata value for {key} must be a string")))?,
        );
    }
    Ok(map)
}

fn value_string(object: &BTreeMap<String, Value>, field: &str) -> Result<String> {
    object
        .get(field)
        .and_then(value_to_string)
        .ok_or_else(|| Error::msg(format!("{field} is required")))
}

fn value_i64(object: &BTreeMap<String, Value>, field: &str) -> Result<i64> {
    match object.get(field) {
        Some(Value::Number(value)) => Ok(*value),
        Some(Value::String(value)) => value
            .parse::<i64>()
            .map_err(|_| Error::msg(format!("{field} must be a number"))),
        _ => Err(Error::msg(format!("{field} is required"))),
    }
}

fn value_as_object(value: Value) -> Result<BTreeMap<String, Value>> {
    match value {
        Value::Object(object) => Ok(object),
        _ => Err(Error::msg("record must be a JSON object")),
    }
}

fn checkpoint_id(value: String) -> CheckpointId {
    CheckpointId::from(value)
}

fn operation_from_str(value: String) -> Result<Operation> {
    match value.as_str() {
        "insert" => Ok(Operation::Insert),
        "upsert" => Ok(Operation::Upsert),
        "delete" => Ok(Operation::Delete),
        _ => Err(Error::msg(format!("unknown op: {value}"))),
    }
}

fn parse_json_value(input: &str) -> Result<Value> {
    let mut parser = Parser::new(input);
    let value = parser.parse_value()?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(Error::msg("unexpected trailing characters"));
    }
    Ok(value)
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn parse_value(&mut self) -> Result<Value> {
        self.skip_ws();
        match self.peek_byte() {
            Some(b'"') => self.parse_string().map(Value::String),
            Some(b'{') => self.parse_object().map(Value::Object),
            Some(b'n') => {
                self.expect_bytes(b"null")?;
                Ok(Value::Null)
            }
            Some(b't') => {
                self.expect_bytes(b"true")?;
                Ok(Value::Bool(true))
            }
            Some(b'f') => {
                self.expect_bytes(b"false")?;
                Ok(Value::Bool(false))
            }
            Some(b'-') | Some(b'0'..=b'9') => self.parse_number().map(Value::Number),
            _ => Err(Error::msg("unexpected token")),
        }
    }

    fn parse_object(&mut self) -> Result<BTreeMap<String, Value>> {
        self.expect_byte(b'{')?;
        let mut object = BTreeMap::new();
        loop {
            self.skip_ws();
            if self.try_byte(b'}') {
                break;
            }

            let key = self.parse_string()?;
            self.skip_ws();
            self.expect_byte(b':')?;
            let value = self.parse_value()?;
            object.insert(key, value);
            self.skip_ws();
            if self.try_byte(b'}') {
                break;
            }
            self.expect_byte(b',')?;
        }
        Ok(object)
    }

    fn parse_string(&mut self) -> Result<String> {
        self.expect_byte(b'"')?;
        let mut out = String::new();
        while let Some(byte) = self.next_byte() {
            match byte {
                b'"' => return Ok(out),
                b'\\' => {
                    let escaped = self
                        .next_byte()
                        .ok_or_else(|| Error::msg("unterminated escape sequence"))?;
                    match escaped {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        _ => return Err(Error::msg("unsupported escape sequence")),
                    }
                }
                _ => out.push(byte as char),
            }
        }
        Err(Error::msg("unterminated string"))
    }

    fn parse_number(&mut self) -> Result<i64> {
        let start = self.pos;
        if self.try_byte(b'-') {}
        while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        let text = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|_| Error::msg("invalid number"))?;
        text.parse::<i64>()
            .map_err(|_| Error::msg(format!("invalid number: {text}")))
    }

    fn expect_bytes(&mut self, expected: &[u8]) -> Result<()> {
        for byte in expected {
            self.expect_byte(*byte)?;
        }
        Ok(())
    }

    fn expect_byte(&mut self, expected: u8) -> Result<()> {
        match self.next_byte() {
            Some(byte) if byte == expected => Ok(()),
            _ => Err(Error::msg("unexpected character")),
        }
    }

    fn try_byte(&mut self, expected: u8) -> bool {
        if self.peek_byte() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn next_byte(&mut self) -> Option<u8> {
        let byte = self.input.get(self.pos).copied()?;
        self.pos += 1;
        Some(byte)
    }

    fn peek_byte(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek_byte(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.pos += 1;
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }
}
