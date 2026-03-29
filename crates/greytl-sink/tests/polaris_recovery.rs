mod support;

use anyhow::Result;
use greytl_sink::{CommitRequest, PolarisSink, ResolvedOutcome, Sink};
use greytl_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use support::polaris_mock::MockPolarisServer;

#[test]
fn polaris_sink_resolves_committed_attempt_after_recreate() -> Result<()> {
    block_on(async {
        let server = MockPolarisServer::start("quickstart_catalog");
        let root = warehouse_root("polaris-recovery");
        let sink = PolarisSink::new(
            server.catalog_uri(),
            server.warehouse(),
            "orders_events",
            format!("file://{}", root.join("warehouse/orders_events").display()),
        )
        .with_client_credentials("root", "s3cr3t");

        let request = sample_append_commit_request(&root);
        let prepared = sink.prepare_commit(request.clone()).await?;
        let committed = sink.commit(prepared).await?;

        let reloaded = PolarisSink::new(
            server.catalog_uri(),
            server.warehouse(),
            "orders_events",
            format!("file://{}", root.join("warehouse/orders_events").display()),
        )
        .with_client_credentials("root", "s3cr3t");
        let resolution = reloaded
            .resolve_uncertain_commit(&committed.attempt())
            .await?;
        match resolution {
            ResolvedOutcome::Committed(outcome) => {
                assert_eq!(outcome.snapshot_id, committed.snapshot_id);
                assert_eq!(outcome.snapshot, committed.snapshot);
            }
            other => panic!("unexpected resolution: {other:?}"),
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires infra/local Polaris stack"]
fn real_stack_recovery_resolves_committed_attempt_after_recreate() -> Result<()> {
    block_on(async {
        let env = real_stack_env();
        let root = warehouse_root("polaris-real-stack-recovery");
        let sink = PolarisSink::new(
            env.catalog_uri.clone(),
            env.catalog_name.clone(),
            env.namespace.clone(),
            format!("file://{}", root.join("warehouse/orders_events").display()),
        )
        .with_client_credentials(env.client_id.clone(), env.client_secret.clone());

        let request = sample_append_commit_request(&root);
        let prepared = sink.prepare_commit(request.clone()).await?;
        let committed = sink.commit(prepared).await?;

        let reloaded = PolarisSink::new(
            env.catalog_uri,
            env.catalog_name,
            env.namespace,
            format!("file://{}", root.join("warehouse/orders_events").display()),
        )
        .with_client_credentials(env.client_id, env.client_secret);
        match reloaded
            .resolve_uncertain_commit(&committed.attempt())
            .await?
        {
            ResolvedOutcome::Committed(found) => {
                assert_eq!(found.snapshot_id, committed.snapshot_id)
            }
            other => panic!("unexpected resolution: {other:?}"),
        }
        Ok(())
    })
}

fn sample_append_commit_request(root: &Path) -> CommitRequest {
    CommitRequest {
        batch_id: BatchId::from("batch-append-0001"),
        destination_uri: format!("file://{}", root.join("warehouse/orders_events").display()),
        manifest: BatchManifest {
            batch_id: BatchId::from("batch-append-0001"),
            table_id: TableId::from("orders_events"),
            table_mode: TableMode::AppendOnly,
            source_id: "source-a".to_string(),
            source_class: SourceClass::FileOrObjectDrop,
            source_checkpoint_start: checkpoint("cp-1"),
            source_checkpoint_end: checkpoint("cp-3"),
            ordering_field: "line_number".to_string(),
            ordering_min: 11,
            ordering_max: 13,
            schema_version: 1,
            schema_fingerprint: "schema".to_string(),
            record_count: 3,
            op_counts: BTreeMap::from([(Operation::Insert, 3)]),
            file_set: vec![ManifestFile {
                file_uri: support::sample_source_file_uri("polaris-recovery"),
                file_kind: "parquet".to_string(),
                content_hash: "hash-a".to_string(),
                file_size_bytes: 128,
                record_count: 3,
                created_at: support::fixed_time(1),
            }],
            content_hash: "content-a".to_string(),
            created_at: support::fixed_time(2),
        },
        idempotency_key: "batch-append-0001:append".into(),
    }
}

fn warehouse_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join("greytl-sink-tests").join(name);
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    support::block_on(future)
}

struct RealStackEnv {
    catalog_uri: String,
    catalog_name: String,
    namespace: String,
    client_id: String,
    client_secret: String,
}

fn real_stack_env() -> RealStackEnv {
    RealStackEnv {
        catalog_uri: env::var("POLARIS_CATALOG_URI")
            .unwrap_or_else(|_| "http://127.0.0.1:8181/api/catalog".to_string()),
        catalog_name: env::var("POLARIS_CATALOG_NAME")
            .unwrap_or_else(|_| "quickstart_catalog".to_string()),
        namespace: env::var("POLARIS_NAMESPACE").unwrap_or_else(|_| "orders_events".to_string()),
        client_id: env::var("POLARIS_CLIENT_ID").unwrap_or_else(|_| "root".to_string()),
        client_secret: env::var("POLARIS_CLIENT_SECRET").unwrap_or_else(|_| "s3cr3t".to_string()),
    }
}
