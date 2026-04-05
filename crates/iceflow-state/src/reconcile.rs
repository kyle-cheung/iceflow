use crate::migrations::invalid_data;
use crate::sqlite::{step_done, step_row, Connection};
use crate::{BatchFile, BatchId, SourceCheckpoint};
use anyhow::Result;
use chrono::{DateTime, Utc};
use iceflow_types::TableId;

pub(crate) fn list_recovery_candidates(conn: &Connection) -> Result<Vec<BatchId>> {
    let mut stmt = conn.prepare(
        "
        SELECT DISTINCT b.batch_id
        FROM batches b
        LEFT JOIN commit_attempts a ON a.batch_id = b.batch_id
        WHERE b.batch_status IN (
                'files_written',
                'commit_started',
                'commit_uncertain',
                'schema_revalidating',
                'retry_ready',
                'checkpoint_pending',
                'quarantined'
            )
           OR a.attempt_status IN ('resolving', 'unknown', 'ambiguous_manual')
        ORDER BY b.batch_id
        ",
    )?;

    let mut batch_ids = Vec::new();
    while step_row(&mut stmt)? {
        batch_ids.push(BatchId::from(stmt.column_text(0)?));
    }
    step_done(&mut stmt)?;
    Ok(batch_ids)
}

pub(crate) fn list_orphan_candidates(conn: &Connection) -> Result<Vec<BatchFile>> {
    let mut stmt = conn.prepare(
        "
        SELECT f.batch_id, f.file_uri, f.file_kind, f.content_hash, f.file_size_bytes,
               f.record_count, f.created_at_secs, f.created_at_nanos
        FROM batch_files f
        INNER JOIN batches b ON b.batch_id = f.batch_id
        LEFT JOIN cleanup_records c
            ON c.batch_id = f.batch_id AND c.file_uri = f.file_uri
        WHERE b.batch_status = 'quarantined'
          AND c.file_uri IS NULL
        ORDER BY f.batch_id, f.file_uri
        ",
    )?;

    let mut files = Vec::new();
    while step_row(&mut stmt)? {
        files.push(BatchFile {
            batch_id: BatchId::from(stmt.column_text(0)?),
            file_uri: stmt.column_text(1)?,
            file_kind: stmt.column_text(2)?,
            content_hash: stmt.column_text(3)?,
            file_size_bytes: stmt.column_u64(4)?,
            record_count: stmt.column_u64(5)?,
            created_at: decode_timestamp(stmt.column_i64(6)?, stmt.column_u32(7)?)?,
        });
    }
    step_done(&mut stmt)?;
    Ok(files)
}

pub(crate) fn durable_checkpoint(
    conn: &Connection,
    batch_id: &BatchId,
) -> Result<Option<SourceCheckpoint>> {
    let mut stmt = conn.prepare(
        "
        SELECT source_id, checkpoint_id
        FROM checkpoint_links
        WHERE batch_id = ?1
          AND ack_status = 'durable'
        ",
    )?;
    stmt.bind_text(1, batch_id.as_str())?;

    if step_row(&mut stmt)? {
        let checkpoint = Some(SourceCheckpoint {
            source_id: stmt.column_text(0)?,
            checkpoint: stmt.column_text(1)?.into(),
        });
        step_done(&mut stmt)?;
        Ok(checkpoint)
    } else {
        Ok(None)
    }
}

pub(crate) fn last_durable_checkpoint_for_table(
    conn: &Connection,
    table_id: &TableId,
) -> Result<Option<SourceCheckpoint>> {
    // Resume must prefer the newest produced batch for a table. Checkpoint link
    // timestamps reflect control-plane timing and can lag behind batch creation
    // if an older batch is durably acknowledged later than a newer one.
    let mut stmt = conn.prepare(
        "
        SELECT cl.source_id, cl.checkpoint_id
        FROM checkpoint_links cl
        INNER JOIN batches b ON b.batch_id = cl.batch_id
        WHERE b.table_id = ?1
          AND cl.ack_status = 'durable'
        ORDER BY b.created_at_secs DESC, b.created_at_nanos DESC, b.rowid DESC, cl.rowid DESC
        LIMIT 1
        ",
    )?;
    stmt.bind_text(1, table_id.as_str())?;

    if step_row(&mut stmt)? {
        let checkpoint = Some(SourceCheckpoint {
            source_id: stmt.column_text(0)?,
            checkpoint: stmt.column_text(1)?.into(),
        });
        step_done(&mut stmt)?;
        Ok(checkpoint)
    } else {
        Ok(None)
    }
}

fn decode_timestamp(secs: i64, nanos: u32) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| invalid_data(format!("invalid timestamp {secs}:{nanos}")))
}
