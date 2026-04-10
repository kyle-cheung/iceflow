use anyhow::Result;
use iceflow_state::{checkpoint_ack, checkpoint_ref, SnapshotRef, SqliteStateStore, StateStore};
use iceflow_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn persistent_store_reopens_existing_durable_checkpoint() -> Result<()> {
    block_on(async {
        let db_path = next_temp_db_path("persistent-store");

        {
            let store = SqliteStateStore::open_persistent(&db_path).await?;
            let manifest = sample_manifest();
            let batch_id = store.register_batch(manifest).await?;
            durable_checkpoint_batch(
                &store,
                &batch_id,
                checkpoint("snowflake:v1:stream:01b12345-0601-1234-0000-000000000000"),
                "file:///tmp/customer-state/42",
            )
            .await?;
        }

        let reopened = SqliteStateStore::open_persistent(&db_path).await?;
        let durable = reopened
            .last_durable_checkpoint_for_table(&TableId::from("customer_state.customer_state"))
            .await?;

        assert_eq!(
            durable.map(|cp| cp.checkpoint.to_string()),
            Some("snowflake:v1:stream:01b12345-0601-1234-0000-000000000000".to_string())
        );
        Ok(())
    })
}

fn next_temp_db_path(label: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    std::env::temp_dir().join(format!(
        "iceflow-state-{label}-{}-{}.sqlite3",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn sample_manifest() -> BatchManifest {
    BatchManifest {
        batch_id: BatchId::from("batch-0001"),
        table_id: TableId::from("customer_state.customer_state"),
        table_mode: TableMode::AppendOnly,
        source_id: "snowflake.config.local_snowflake".to_string(),
        source_class: SourceClass::DatabaseCdc,
        source_checkpoint_start: checkpoint(
            "snowflake:v1:snapshot:01b12345-0600-1234-0000-000000000000",
        ),
        source_checkpoint_end: checkpoint(
            "snowflake:v1:stream:01b12345-0601-1234-0000-000000000000",
        ),
        ordering_field: "snowflake_ordinal".to_string(),
        ordering_min: 1,
        ordering_max: 2,
        schema_version: 1,
        schema_fingerprint: "customer-state-v1".to_string(),
        record_count: 2,
        op_counts: BTreeMap::from([(Operation::Insert, 2)]),
        file_set: vec![ManifestFile {
            file_uri: "file:///tmp/customer_state.parquet".to_string(),
            file_kind: "parquet".to_string(),
            content_hash: "content-hash-1".to_string(),
            file_size_bytes: 128,
            record_count: 2,
            created_at: chrono::DateTime::from_timestamp(5, 0).expect("valid timestamp"),
        }],
        content_hash: "batch-content-hash".to_string(),
        created_at: chrono::DateTime::from_timestamp(5, 0).expect("valid timestamp"),
    }
}

async fn durable_checkpoint_batch(
    store: &SqliteStateStore,
    batch_id: &BatchId,
    checkpoint_id: iceflow_types::CheckpointId,
    snapshot_uri: &str,
) -> Result<()> {
    let attempt = store
        .begin_commit(
            batch_id.clone(),
            iceflow_state::CommitRequest {
                destination_uri: "file:///tmp/warehouse".to_string(),
                snapshot: SnapshotRef {
                    uri: snapshot_uri.to_string(),
                },
                actor: "sqlite-store-test".to_string(),
            },
        )
        .await?;

    let snapshot = SnapshotRef {
        uri: snapshot_uri.to_string(),
    };

    store
        .resolve_commit(attempt.id, iceflow_state::AttemptResolution::Committed)
        .await?;
    store
        .link_checkpoint_pending(
            batch_id.clone(),
            checkpoint_ref("snowflake.config.local_snowflake", checkpoint_id.clone()),
            snapshot.clone(),
        )
        .await?;
    store
        .mark_checkpoint_durable(
            batch_id.clone(),
            checkpoint_ack("snowflake.config.local_snowflake", checkpoint_id, snapshot),
        )
        .await
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime should build")
        .block_on(future)
}
