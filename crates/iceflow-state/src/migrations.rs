use crate::sqlite::Connection;
use anyhow::{Error, Result};

const MIGRATIONS: &[&str] = &[
    "
    CREATE TABLE IF NOT EXISTS batches (
        batch_id TEXT PRIMARY KEY,
        table_id TEXT NOT NULL,
        table_mode TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_class TEXT NOT NULL,
        source_checkpoint_start TEXT NOT NULL,
        source_checkpoint_end TEXT NOT NULL,
        ordering_field TEXT NOT NULL,
        ordering_min INTEGER NOT NULL,
        ordering_max INTEGER NOT NULL,
        schema_version INTEGER NOT NULL,
        schema_fingerprint TEXT NOT NULL,
        record_count INTEGER NOT NULL,
        content_hash TEXT NOT NULL,
        batch_status TEXT NOT NULL,
        quarantine_reason TEXT,
        created_at_secs INTEGER NOT NULL,
        created_at_nanos INTEGER NOT NULL
    );
    ",
    "
    CREATE TABLE IF NOT EXISTS batch_files (
        batch_id TEXT NOT NULL,
        file_uri TEXT NOT NULL,
        file_kind TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        file_size_bytes INTEGER NOT NULL,
        record_count INTEGER NOT NULL,
        created_at_secs INTEGER NOT NULL,
        created_at_nanos INTEGER NOT NULL,
        PRIMARY KEY (batch_id, file_uri),
        FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
    );
    ",
    "
    CREATE TABLE IF NOT EXISTS commit_attempts (
        attempt_id TEXT PRIMARY KEY,
        batch_id TEXT NOT NULL,
        attempt_no INTEGER NOT NULL,
        destination_uri TEXT NOT NULL,
        snapshot_uri TEXT NOT NULL,
        actor TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        request_payload_hash TEXT NOT NULL,
        attempt_status TEXT NOT NULL,
        resolved_at_secs INTEGER,
        resolved_at_nanos INTEGER,
        FOREIGN KEY (batch_id) REFERENCES batches(batch_id),
        UNIQUE (batch_id, attempt_no)
    );
    ",
    "
    CREATE TABLE IF NOT EXISTS checkpoint_links (
        batch_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        checkpoint_id TEXT NOT NULL,
        snapshot_uri TEXT NOT NULL,
        ack_status TEXT NOT NULL,
        linked_at_secs INTEGER NOT NULL,
        linked_at_nanos INTEGER NOT NULL,
        FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
    );
    ",
    "
    CREATE TABLE IF NOT EXISTS quarantine_records (
        batch_id TEXT PRIMARY KEY,
        reason_code TEXT NOT NULL,
        details TEXT NOT NULL,
        opened_at_secs INTEGER NOT NULL,
        opened_at_nanos INTEGER NOT NULL,
        resolved_at_secs INTEGER,
        resolved_at_nanos INTEGER,
        FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
    );
    ",
    "
    CREATE TABLE IF NOT EXISTS cleanup_records (
        batch_id TEXT NOT NULL,
        file_uri TEXT NOT NULL,
        action TEXT NOT NULL,
        recorded_at_secs INTEGER NOT NULL,
        recorded_at_nanos INTEGER NOT NULL,
        PRIMARY KEY (batch_id, file_uri),
        FOREIGN KEY (batch_id, file_uri) REFERENCES batch_files(batch_id, file_uri)
    );
    ",
    "
    CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(batch_status);
    ",
    "
    CREATE INDEX IF NOT EXISTS idx_batches_table_id ON batches(table_id);
    ",
    "
    CREATE INDEX IF NOT EXISTS idx_attempts_batch_status ON commit_attempts(batch_id, attempt_status);
    ",
];

pub(crate) fn apply(conn: &Connection) -> Result<()> {
    conn.with_transaction(|transaction| {
        for migration in MIGRATIONS {
            transaction.exec(migration)?;
        }
        Ok(())
    })
}

pub(crate) fn invalid_data(message: impl Into<String>) -> Error {
    Error::msg(message.into())
}
