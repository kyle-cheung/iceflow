use crate::migrations::{apply as apply_migrations, invalid_data};
use crate::reconcile;
use crate::recovery::resolve_attempt;
use crate::{
    AttemptResolution, AttemptStatus, BatchFile, BatchStatus, CheckpointAck, CommitAttempt,
    CommitRequest, QuarantineReason, SnapshotRef, SourceCheckpoint, StateStore,
};
use anyhow::{Error, Result};
use chrono::{DateTime, Utc};
use greytl_types::{BatchId, BatchManifest, CommitAttemptId};
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(C)]
struct sqlite3 {
    _private: [u8; 0],
}

#[repr(C)]
struct sqlite3_stmt {
    _private: [u8; 0],
}

#[link(name = "sqlite3")]
unsafe extern "C" {
    fn sqlite3_open_v2(
        filename: *const c_char,
        pp_db: *mut *mut sqlite3,
        flags: c_int,
        z_vfs: *const c_char,
    ) -> c_int;
    fn sqlite3_close(db: *mut sqlite3) -> c_int;
    fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
    fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: Option<
            unsafe extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int,
        >,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    fn sqlite3_free(ptr: *mut c_void);
    fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        sql: *const c_char,
        n_byte: c_int,
        stmt: *mut *mut sqlite3_stmt,
        tail: *mut *const c_char,
    ) -> c_int;
    fn sqlite3_bind_text(
        stmt: *mut sqlite3_stmt,
        index: c_int,
        value: *const c_char,
        n: c_int,
        destructor: Option<unsafe extern "C" fn(*mut c_void)>,
    ) -> c_int;
    fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, index: c_int, value: i64) -> c_int;
    fn sqlite3_column_text(stmt: *mut sqlite3_stmt, index: c_int) -> *const u8;
    fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, index: c_int) -> i64;
    fn sqlite3_column_type(stmt: *mut sqlite3_stmt, index: c_int) -> c_int;
    fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;
}

const SQLITE_OK: c_int = 0;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;
const SQLITE_NULL: c_int = 5;
const SQLITE_OPEN_READWRITE: c_int = 0x0000_0002;
const SQLITE_OPEN_CREATE: c_int = 0x0000_0004;
const SQLITE_OPEN_FULLMUTEX: c_int = 0x0001_0000;

static NEXT_DB_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct SqliteStateStore {
    path: PathBuf,
}

pub use SqliteStateStore as TestStateStore;

impl SqliteStateStore {
    pub async fn new() -> Result<Self> {
        let path = next_db_path();
        let store = Self { path };
        let conn = Connection::open(&store.path)?;
        conn.exec("PRAGMA journal_mode = WAL;")?;
        conn.exec("PRAGMA synchronous = FULL;")?;
        apply_migrations(&conn)?;
        Ok(store)
    }

    pub async fn batch_status(&self, batch_id: BatchId) -> Result<BatchStatus> {
        let conn = Connection::open(&self.path)?;
        let mut stmt = conn.prepare("SELECT batch_status FROM batches WHERE batch_id = ?1")?;
        stmt.bind_text(1, batch_id.as_str())?;
        if step_row(&mut stmt)? {
            let value = stmt.column_text(0)?;
            step_done(&mut stmt)?;
            return BatchStatus::from_str(&value)
                .ok_or_else(|| invalid_data(format!("unknown batch status: {value}")));
        }
        Err(Error::msg(format!(
            "batch not found: {}",
            batch_id.as_str()
        )))
    }

    pub async fn attempt_status(&self, attempt_id: CommitAttemptId) -> Result<AttemptStatus> {
        let conn = Connection::open(&self.path)?;
        let mut stmt =
            conn.prepare("SELECT attempt_status FROM commit_attempts WHERE attempt_id = ?1")?;
        stmt.bind_text(1, attempt_id.as_str())?;
        if step_row(&mut stmt)? {
            let value = stmt.column_text(0)?;
            step_done(&mut stmt)?;
            return AttemptStatus::from_str(&value)
                .ok_or_else(|| invalid_data(format!("unknown attempt status: {value}")));
        }
        Err(Error::msg(format!(
            "attempt not found: {}",
            attempt_id.as_str()
        )))
    }

    pub async fn durable_checkpoint(&self, batch_id: BatchId) -> Result<Option<SourceCheckpoint>> {
        let conn = Connection::open(&self.path)?;
        reconcile::durable_checkpoint(&conn, &batch_id)
    }

    pub async fn force_attempt_status(
        &self,
        attempt_id: CommitAttemptId,
        status: AttemptStatus,
    ) -> Result<()> {
        let conn = Connection::open(&self.path)?;
        conn.with_transaction(|transaction| {
            let mut stmt = transaction
                .prepare("UPDATE commit_attempts SET attempt_status = ?1 WHERE attempt_id = ?2")?;
            stmt.bind_text(1, status.as_str())?;
            stmt.bind_text(2, attempt_id.as_str())?;
            step_done(&mut stmt)?;

            if matches!(status, AttemptStatus::Resolving | AttemptStatus::Unknown) {
                let batch_id = select_attempt_batch_id(transaction, attempt_id.as_str())?;
                update_batch_status(transaction, &batch_id, BatchStatus::CommitUncertain)?;
            }

            Ok(())
        })
    }

    pub async fn mark_attempt_resolving(&self, attempt_id: CommitAttemptId) -> Result<()> {
        self.force_attempt_status(attempt_id, AttemptStatus::Resolving)
            .await
    }

    pub async fn mark_retry_ready(&self, batch_id: BatchId) -> Result<()> {
        let conn = Connection::open(&self.path)?;
        conn.with_transaction(|transaction| {
            update_batch_status(transaction, batch_id.as_str(), BatchStatus::RetryReady)
        })
    }

    pub async fn synchronous_mode(&self) -> Result<u32> {
        let conn = Connection::open(&self.path)?;
        let mut stmt = conn.prepare("PRAGMA synchronous")?;
        if step_row(&mut stmt)? {
            let value = stmt.column_u32(0)?;
            step_done(&mut stmt)?;
            Ok(value)
        } else {
            Err(Error::msg("PRAGMA synchronous returned no row"))
        }
    }
}

impl Drop for SqliteStateStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl StateStore for SqliteStateStore {
    async fn register_batch(&self, manifest: BatchManifest) -> Result<BatchId> {
        let conn = Connection::open(&self.path)?;
        let batch_id = manifest.batch_id.clone();

        let mut stmt = conn.prepare(
            "INSERT OR IGNORE INTO batches (
                batch_id, table_id, table_mode, source_id, source_class,
                source_checkpoint_start, source_checkpoint_end, ordering_field,
                ordering_min, ordering_max, schema_version, schema_fingerprint,
                record_count, content_hash, batch_status, quarantine_reason,
                created_at_secs, created_at_nanos
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                ?9, ?10, ?11, ?12, ?13, ?14, ?15, NULL, ?16, ?17
            )",
        )?;
        stmt.bind_text(1, manifest.batch_id.as_str())?;
        stmt.bind_text(2, manifest.table_id.as_str())?;
        stmt.bind_text(3, manifest.table_mode.stable_tag())?;
        stmt.bind_text(4, &manifest.source_id)?;
        stmt.bind_text(5, manifest.source_class.stable_tag())?;
        stmt.bind_text(6, manifest.source_checkpoint_start.as_str())?;
        stmt.bind_text(7, manifest.source_checkpoint_end.as_str())?;
        stmt.bind_text(8, &manifest.ordering_field)?;
        stmt.bind_i64(9, manifest.ordering_min)?;
        stmt.bind_i64(10, manifest.ordering_max)?;
        stmt.bind_i64(11, i64::from(manifest.schema_version))?;
        stmt.bind_text(12, &manifest.schema_fingerprint)?;
        stmt.bind_i64(13, manifest.record_count as i64)?;
        stmt.bind_text(14, &manifest.content_hash)?;
        stmt.bind_text(15, BatchStatus::Registered.as_str())?;
        bind_timestamp(&mut stmt, 16, &manifest.created_at)?;
        step_done(&mut stmt)?;
        Ok(batch_id)
    }

    async fn record_files(&self, batch_id: BatchId, files: Vec<BatchFile>) -> Result<()> {
        let conn = Connection::open(&self.path)?;
        conn.with_transaction(|transaction| {
            for file in files {
                if file.batch_id != batch_id {
                    return Err(Error::msg(
                        "record_files received a file for the wrong batch",
                    ));
                }

                let mut stmt = transaction.prepare(
                    "INSERT OR REPLACE INTO batch_files (
                        batch_id, file_uri, file_kind, content_hash, file_size_bytes,
                        record_count, created_at_secs, created_at_nanos
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                )?;
                stmt.bind_text(1, batch_id.as_str())?;
                stmt.bind_text(2, &file.file_uri)?;
                stmt.bind_text(3, &file.file_kind)?;
                stmt.bind_text(4, &file.content_hash)?;
                stmt.bind_i64(5, file.file_size_bytes as i64)?;
                stmt.bind_i64(6, file.record_count as i64)?;
                bind_timestamp(&mut stmt, 7, &file.created_at)?;
                step_done(&mut stmt)?;
            }

            if current_batch_status(transaction, batch_id.as_str())? == BatchStatus::Registered {
                update_batch_status(transaction, batch_id.as_str(), BatchStatus::FilesWritten)?;
            }
            Ok(())
        })
    }

    async fn begin_commit(&self, batch_id: BatchId, req: CommitRequest) -> Result<CommitAttempt> {
        let conn = Connection::open(&self.path)?;
        conn.with_transaction(|transaction| {
            let attempt_no = next_attempt_no(transaction, batch_id.as_str())?;
            let attempt_id =
                CommitAttemptId::from(format!("{}-attempt-{attempt_no:04}", batch_id.as_str()));
            let idempotency_key = format!("{}:{attempt_no}", batch_id.as_str());
            let request_payload_hash = stable_hash([
                batch_id.to_string(),
                req.destination_uri.clone(),
                req.snapshot.uri.clone(),
                req.actor.clone(),
            ]);

            let mut stmt = transaction.prepare(
                "INSERT INTO commit_attempts (
                    attempt_id, batch_id, attempt_no, destination_uri, snapshot_uri,
                    actor, idempotency_key, request_payload_hash, attempt_status,
                    resolved_at_secs, resolved_at_nanos
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, NULL)",
            )?;
            stmt.bind_text(1, attempt_id.as_str())?;
            stmt.bind_text(2, batch_id.as_str())?;
            stmt.bind_i64(3, i64::from(attempt_no))?;
            stmt.bind_text(4, &req.destination_uri)?;
            stmt.bind_text(5, &req.snapshot.uri)?;
            stmt.bind_text(6, &req.actor)?;
            stmt.bind_text(7, &idempotency_key)?;
            stmt.bind_text(8, &request_payload_hash)?;
            stmt.bind_text(9, AttemptStatus::Started.as_str())?;
            step_done(&mut stmt)?;

            update_batch_status(transaction, batch_id.as_str(), BatchStatus::CommitStarted)?;

            Ok(CommitAttempt {
                id: attempt_id,
                batch_id,
                attempt_no,
                destination_uri: req.destination_uri,
                snapshot: req.snapshot,
                actor: req.actor,
                idempotency_key,
                status: AttemptStatus::Started,
            })
        })
    }

    async fn resolve_commit(
        &self,
        attempt_id: CommitAttemptId,
        resolution: AttemptResolution,
    ) -> Result<()> {
        let conn = Connection::open(&self.path)?;
        conn.with_transaction(|transaction| {
            let batch_id = select_attempt_batch_id(transaction, attempt_id.as_str())?;
            let (attempt_status, batch_status, quarantine_reason) = resolve_attempt(resolution);

            let mut stmt = transaction.prepare(
                "UPDATE commit_attempts
                 SET attempt_status = ?1, resolved_at_secs = ?2, resolved_at_nanos = ?3
                 WHERE attempt_id = ?4",
            )?;
            stmt.bind_text(1, attempt_status.as_str())?;
            bind_timestamp(&mut stmt, 2, &Utc::now())?;
            stmt.bind_text(4, attempt_id.as_str())?;
            step_done(&mut stmt)?;

            update_batch_status(transaction, &batch_id, batch_status)?;
            if let Some(reason) = quarantine_reason {
                write_quarantine_record(transaction, &batch_id, reason)?;
            }
            Ok(())
        })
    }

    async fn link_checkpoint_pending(
        &self,
        batch_id: BatchId,
        cp: SourceCheckpoint,
        snapshot: SnapshotRef,
    ) -> Result<()> {
        let conn = Connection::open(&self.path)?;
        conn.with_transaction(|transaction| {
            let mut stmt = transaction.prepare(
                "INSERT OR REPLACE INTO checkpoint_links (
                    batch_id, source_id, checkpoint_id, snapshot_uri, ack_status,
                    linked_at_secs, linked_at_nanos
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            stmt.bind_text(1, batch_id.as_str())?;
            stmt.bind_text(2, &cp.source_id)?;
            stmt.bind_text(3, cp.checkpoint.as_str())?;
            stmt.bind_text(4, &snapshot.uri)?;
            stmt.bind_text(5, "pending")?;
            bind_timestamp(&mut stmt, 6, &Utc::now())?;
            step_done(&mut stmt)?;
            update_batch_status(
                transaction,
                batch_id.as_str(),
                BatchStatus::CheckpointPending,
            )?;
            Ok(())
        })
    }

    async fn mark_checkpoint_durable(&self, batch_id: BatchId, ack: CheckpointAck) -> Result<()> {
        let conn = Connection::open(&self.path)?;
        conn.with_transaction(|transaction| {
            let mut query = transaction.prepare(
                "SELECT source_id, checkpoint_id, snapshot_uri
                 FROM checkpoint_links
                 WHERE batch_id = ?1 AND ack_status = 'pending'",
            )?;
            query.bind_text(1, batch_id.as_str())?;
            if !step_row(&mut query)? {
                return Err(Error::msg(format!(
                    "pending checkpoint link not found for batch {}",
                    batch_id.as_str()
                )));
            }

            let source_id = query.column_text(0)?;
            let checkpoint_id = query.column_text(1)?;
            let snapshot_uri = query.column_text(2)?;
            step_done(&mut query)?;

            if source_id != ack.source_id
                || checkpoint_id != ack.checkpoint.as_str()
                || snapshot_uri != ack.snapshot.uri
            {
                return Err(Error::msg(
                    "checkpoint durable ack does not match pending link",
                ));
            }

            let mut stmt = transaction.prepare(
                "UPDATE checkpoint_links SET ack_status = 'durable' WHERE batch_id = ?1",
            )?;
            stmt.bind_text(1, batch_id.as_str())?;
            step_done(&mut stmt)?;
            update_batch_status(transaction, batch_id.as_str(), BatchStatus::Checkpointed)?;
            Ok(())
        })
    }

    async fn mark_quarantine(&self, batch_id: BatchId, reason: QuarantineReason) -> Result<()> {
        let conn = Connection::open(&self.path)?;
        conn.with_transaction(|transaction| {
            update_batch_status(transaction, batch_id.as_str(), BatchStatus::Quarantined)?;
            write_quarantine_record(transaction, batch_id.as_str(), reason)
        })
    }

    async fn list_recovery_candidates(&self) -> Result<Vec<BatchId>> {
        let conn = Connection::open(&self.path)?;
        reconcile::list_recovery_candidates(&conn)
    }

    async fn list_orphan_candidates(&self) -> Result<Vec<BatchFile>> {
        let conn = Connection::open(&self.path)?;
        reconcile::list_orphan_candidates(&conn)
    }

    async fn record_cleanup(&self, file: BatchFile, action: crate::CleanupAction) -> Result<()> {
        let conn = Connection::open(&self.path)?;
        let mut stmt = conn.prepare(
            "INSERT OR REPLACE INTO cleanup_records (
                batch_id, file_uri, action, recorded_at_secs, recorded_at_nanos
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        stmt.bind_text(1, file.batch_id.as_str())?;
        stmt.bind_text(2, &file.file_uri)?;
        stmt.bind_text(3, action.as_str())?;
        bind_timestamp(&mut stmt, 4, &Utc::now())?;
        step_done(&mut stmt)?;
        Ok(())
    }
}

fn next_db_path() -> PathBuf {
    let id = NEXT_DB_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("greytl-state-{}-{id}.sqlite", std::process::id()))
}

fn bind_timestamp(
    stmt: &mut Statement<'_>,
    start_index: c_int,
    timestamp: &DateTime<Utc>,
) -> Result<()> {
    stmt.bind_i64(start_index, timestamp.timestamp())?;
    stmt.bind_i64(
        start_index + 1,
        i64::from(timestamp.timestamp_subsec_nanos()),
    )?;
    Ok(())
}

fn select_attempt_batch_id(conn: &Connection, attempt_id: &str) -> Result<String> {
    let mut stmt = conn.prepare("SELECT batch_id FROM commit_attempts WHERE attempt_id = ?1")?;
    stmt.bind_text(1, attempt_id)?;
    if step_row(&mut stmt)? {
        let batch_id = stmt.column_text(0)?;
        step_done(&mut stmt)?;
        Ok(batch_id)
    } else {
        Err(Error::msg(format!("attempt not found: {attempt_id}")))
    }
}

fn next_attempt_no(conn: &Connection, batch_id: &str) -> Result<u32> {
    let mut stmt = conn
        .prepare("SELECT COALESCE(MAX(attempt_no), 0) FROM commit_attempts WHERE batch_id = ?1")?;
    stmt.bind_text(1, batch_id)?;
    if step_row(&mut stmt)? {
        let attempt_no = stmt.column_i64(0)? as u32 + 1;
        step_done(&mut stmt)?;
        Ok(attempt_no)
    } else {
        Ok(1)
    }
}

fn write_quarantine_record(
    conn: &Connection,
    batch_id: &str,
    reason: QuarantineReason,
) -> Result<()> {
    let mut batch_update =
        conn.prepare("UPDATE batches SET quarantine_reason = ?1 WHERE batch_id = ?2")?;
    batch_update.bind_text(1, reason.as_str())?;
    batch_update.bind_text(2, batch_id)?;
    step_done(&mut batch_update)?;

    let now = Utc::now();
    let mut record = conn.prepare(
        "INSERT OR REPLACE INTO quarantine_records (
            batch_id, reason_code, details, opened_at_secs, opened_at_nanos,
            resolved_at_secs, resolved_at_nanos
         ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL)",
    )?;
    record.bind_text(1, batch_id)?;
    record.bind_text(2, reason.as_str())?;
    record.bind_text(3, reason.as_str())?;
    bind_timestamp(&mut record, 4, &now)?;
    step_done(&mut record)
}

fn update_batch_status(conn: &Connection, batch_id: &str, status: BatchStatus) -> Result<()> {
    let mut stmt = conn.prepare("UPDATE batches SET batch_status = ?1 WHERE batch_id = ?2")?;
    stmt.bind_text(1, status.as_str())?;
    stmt.bind_text(2, batch_id)?;
    step_done(&mut stmt)
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

pub(crate) struct Connection {
    raw: *mut sqlite3,
}

impl Connection {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let c_path = CString::new(path.to_string_lossy().as_bytes())
            .map_err(|_| Error::msg("sqlite database path contains interior null"))?;
        let mut raw = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_open_v2(
                c_path.as_ptr(),
                &mut raw,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                std::ptr::null(),
            )
        };
        if rc != SQLITE_OK {
            let message = if raw.is_null() {
                format!("sqlite open failed with rc={rc}")
            } else {
                sqlite_error(raw, "sqlite open failed")
            };
            if !raw.is_null() {
                unsafe {
                    sqlite3_close(raw);
                }
            }
            return Err(Error::msg(message));
        }
        let connection = Self { raw };
        connection.exec("PRAGMA foreign_keys = ON;")?;
        connection.exec("PRAGMA synchronous = FULL;")?;
        Ok(connection)
    }

    pub(crate) fn with_transaction<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        self.exec("BEGIN IMMEDIATE")?;
        match f(self) {
            Ok(value) => {
                if let Err(error) = self.exec("COMMIT") {
                    let _ = self.exec("ROLLBACK");
                    return Err(error);
                }
                Ok(value)
            }
            Err(error) => {
                let _ = self.exec("ROLLBACK");
                Err(error)
            }
        }
    }

    pub(crate) fn exec(&self, sql: &str) -> Result<()> {
        let sql = CString::new(sql).map_err(|_| Error::msg("sqlite sql contains interior null"))?;
        let mut error_message = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_exec(
                self.raw,
                sql.as_ptr(),
                None,
                std::ptr::null_mut(),
                &mut error_message,
            )
        };
        if rc != SQLITE_OK {
            let message = if error_message.is_null() {
                sqlite_error(self.raw, "sqlite exec failed")
            } else {
                let message =
                    unsafe { CStr::from_ptr(error_message).to_string_lossy().into_owned() };
                unsafe {
                    sqlite3_free(error_message.cast::<c_void>());
                }
                message
            };
            return Err(Error::msg(message));
        }
        Ok(())
    }

    pub(crate) fn prepare<'a>(&'a self, sql: &str) -> Result<Statement<'a>> {
        let sql = CString::new(sql).map_err(|_| Error::msg("sqlite sql contains interior null"))?;
        let mut raw = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(self.raw, sql.as_ptr(), -1, &mut raw, std::ptr::null_mut())
        };
        if rc != SQLITE_OK {
            return Err(Error::msg(sqlite_error(self.raw, "sqlite prepare failed")));
        }
        Ok(Statement {
            raw,
            connection: self,
            bound_text: Vec::new(),
            last_step_result: None,
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                sqlite3_close(self.raw);
            }
        }
    }
}

pub(crate) struct Statement<'connection> {
    raw: *mut sqlite3_stmt,
    connection: &'connection Connection,
    bound_text: Vec<CString>,
    last_step_result: Option<c_int>,
}

impl Statement<'_> {
    pub(crate) fn bind_text(&mut self, index: c_int, value: &str) -> Result<()> {
        let text = CString::new(value)
            .map_err(|_| Error::msg("sqlite bound text contains interior null"))?;
        let rc = unsafe { sqlite3_bind_text(self.raw, index, text.as_ptr(), -1, None) };
        if rc != SQLITE_OK {
            return Err(Error::msg(sqlite_error(
                self.connection.raw,
                "sqlite bind_text failed",
            )));
        }
        self.bound_text.push(text);
        Ok(())
    }

    pub(crate) fn bind_i64(&mut self, index: c_int, value: i64) -> Result<()> {
        let rc = unsafe { sqlite3_bind_int64(self.raw, index, value) };
        if rc != SQLITE_OK {
            return Err(Error::msg(sqlite_error(
                self.connection.raw,
                "sqlite bind_i64 failed",
            )));
        }
        Ok(())
    }

    pub(crate) fn column_text(&self, index: c_int) -> Result<String> {
        if unsafe { sqlite3_column_type(self.raw, index) } == SQLITE_NULL {
            return Err(invalid_data(format!(
                "unexpected null text at column {index}"
            )));
        }
        let ptr = unsafe { sqlite3_column_text(self.raw, index) };
        if ptr.is_null() {
            return Err(invalid_data(format!(
                "unexpected null text at column {index}"
            )));
        }
        Ok(unsafe { CStr::from_ptr(ptr.cast::<c_char>()) }
            .to_string_lossy()
            .into_owned())
    }

    pub(crate) fn column_i64(&self, index: c_int) -> Result<i64> {
        if unsafe { sqlite3_column_type(self.raw, index) } == SQLITE_NULL {
            return Err(invalid_data(format!(
                "unexpected null int at column {index}"
            )));
        }
        Ok(unsafe { sqlite3_column_int64(self.raw, index) })
    }

    pub(crate) fn column_u64(&self, index: c_int) -> Result<u64> {
        self.column_i64(index)?
            .try_into()
            .map_err(|_| invalid_data(format!("column {index} is out of range for u64")))
    }

    pub(crate) fn column_u32(&self, index: c_int) -> Result<u32> {
        self.column_i64(index)?
            .try_into()
            .map_err(|_| invalid_data(format!("column {index} is out of range for u32")))
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                sqlite3_finalize(self.raw);
            }
        }
    }
}

pub(crate) fn step_row(stmt: &mut Statement<'_>) -> Result<bool> {
    let rc = unsafe { sqlite3_step(stmt.raw) };
    stmt.last_step_result = Some(rc);
    match rc {
        SQLITE_ROW => Ok(true),
        SQLITE_DONE => Ok(false),
        _ => Err(Error::msg(sqlite_error(
            stmt.connection.raw,
            "sqlite step failed",
        ))),
    }
}

pub(crate) fn step_done(stmt: &mut Statement<'_>) -> Result<()> {
    if stmt.last_step_result == Some(SQLITE_DONE) {
        return Ok(());
    }

    let rc = unsafe { sqlite3_step(stmt.raw) };
    stmt.last_step_result = Some(rc);
    match rc {
        SQLITE_DONE => Ok(()),
        SQLITE_ROW => Err(Error::msg("sqlite statement returned unexpected row")),
        _ => Err(Error::msg(sqlite_error(
            stmt.connection.raw,
            "sqlite step failed",
        ))),
    }
}

fn sqlite_error(db: *mut sqlite3, context: &str) -> String {
    let message = unsafe { sqlite3_errmsg(db) };
    if message.is_null() {
        format!("{context}: unknown sqlite error")
    } else {
        format!(
            "{context}: {}",
            unsafe { CStr::from_ptr(message) }.to_string_lossy()
        )
    }
}

fn current_batch_status(conn: &Connection, batch_id: &str) -> Result<BatchStatus> {
    let mut stmt = conn.prepare("SELECT batch_status FROM batches WHERE batch_id = ?1")?;
    stmt.bind_text(1, batch_id)?;
    if step_row(&mut stmt)? {
        let value = stmt.column_text(0)?;
        step_done(&mut stmt)?;
        BatchStatus::from_str(&value)
            .ok_or_else(|| invalid_data(format!("unknown batch status: {value}")))
    } else {
        Err(Error::msg(format!("batch not found: {batch_id}")))
    }
}
