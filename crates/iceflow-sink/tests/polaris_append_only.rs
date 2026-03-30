mod support;

use anyhow::Result;
use iceflow_sink::{
    CommitRequest, IdempotencyKey, LookupResult, PolarisSink, ResolvedOutcome, Sink,
};
use iceflow_types::{
    checkpoint, BatchId, BatchManifest, ManifestFile, Operation, SourceClass, TableId, TableMode,
};
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use support::polaris_mock::MockPolarisServer;

#[test]
fn polaris_append_only_commit_is_recorded_in_namespace_properties() -> Result<()> {
    block_on(async {
        let server = MockPolarisServer::start("quickstart_catalog");
        let root = warehouse_root("polaris-append-only");
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
        let properties = server
            .namespace_properties("orders_events")
            .expect("namespace properties should exist after commit");
        assert_eq!(properties.len(), 3);

        let lookup = sink.lookup_commit(&request.idempotency_key).await?;
        match lookup {
            LookupResult::Found(found) => {
                assert_eq!(found.snapshot_id, committed.snapshot_id);
                assert_eq!(found.snapshot, committed.snapshot);
            }
            other => panic!("unexpected lookup result: {other:?}"),
        }

        let resolution = sink.resolve_uncertain_commit(&committed.attempt()).await?;
        match resolution {
            ResolvedOutcome::Committed(outcome) => {
                assert_eq!(outcome.snapshot_id, committed.snapshot_id);
                assert_eq!(outcome.snapshot, committed.snapshot);
            }
            other => panic!("unexpected resolution: {other:?}"),
        }

        assert!(server
            .namespace_properties("orders_events")
            .unwrap()
            .keys()
            .any(|key| key.contains("iceflow.commit")));
        Ok(())
    })
}

#[test]
fn polaris_oauth_client_credentials_are_form_encoded() -> Result<()> {
    block_on(async {
        let server = MockPolarisServer::start("quickstart_catalog");
        let root = warehouse_root("polaris-oauth-encoding");
        let sink = PolarisSink::new(
            server.catalog_uri(),
            server.warehouse(),
            "orders_events",
            format!("file://{}", root.join("warehouse/orders_events").display()),
        )
        .with_client_credentials("root&ops=admin+lead%west", "s3cr3t&rotate=yes+later%");

        let request = sample_append_commit_request(&root);
        let prepared = sink.prepare_commit(request).await?;
        let _committed = sink.commit(prepared).await?;

        let body = server
            .last_oauth_body()
            .expect("oauth body should be captured");
        let params = parse_form_body(&body);
        assert_eq!(
            params.get("grant_type").map(String::as_str),
            Some("client_credentials")
        );
        assert_eq!(
            params.get("client_id").map(String::as_str),
            Some("root&ops=admin+lead%west")
        );
        assert_eq!(
            params.get("client_secret").map(String::as_str),
            Some("s3cr3t&rotate=yes+later%")
        );
        assert_eq!(
            params.get("scope").map(String::as_str),
            Some("PRINCIPAL_ROLE:ALL")
        );
        assert_eq!(params.len(), 4);
        Ok(())
    })
}

#[test]
#[ignore = "requires infra/local Polaris stack"]
fn real_stack_append_only_commit_round_trips_against_polaris() -> Result<()> {
    block_on(async {
        let env = real_stack_env();
        let root = warehouse_root("polaris-real-stack-append-only");
        let sink = PolarisSink::new(
            env.catalog_uri.clone(),
            env.catalog_name.clone(),
            env.namespace.clone(),
            format!("file://{}", root.join("warehouse/orders_events").display()),
        )
        .with_client_credentials(env.client_id, env.client_secret);

        let request = sample_append_commit_request(&root);
        let prepared = sink.prepare_commit(request.clone()).await?;
        let committed = sink.commit(prepared).await?;

        match sink.lookup_commit(&request.idempotency_key).await? {
            LookupResult::Found(found) => assert_eq!(found.snapshot_id, committed.snapshot_id),
            other => panic!("unexpected lookup result: {other:?}"),
        }

        match sink.resolve_uncertain_commit(&committed.attempt()).await? {
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
                file_uri: support::sample_source_file_uri("polaris-append-only"),
                file_kind: "parquet".to_string(),
                content_hash: "hash-a".to_string(),
                file_size_bytes: 128,
                record_count: 3,
                created_at: support::fixed_time(1),
            }],
            content_hash: "content-a".to_string(),
            created_at: support::fixed_time(2),
        },
        idempotency_key: IdempotencyKey::from("batch-append-0001:append"),
    }
}

fn warehouse_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join("iceflow-sink-tests").join(name);
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn parse_form_body(body: &str) -> BTreeMap<String, String> {
    body.split('&')
        .map(|entry| {
            let (key, value) = entry
                .split_once('=')
                .unwrap_or_else(|| panic!("expected form key/value pair, got {entry}"));
            (percent_decode(key), percent_decode(value))
        })
        .collect()
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' => {
                let hi = *bytes
                    .get(index + 1)
                    .unwrap_or_else(|| panic!("incomplete percent escape in {value}"));
                let lo = *bytes
                    .get(index + 2)
                    .unwrap_or_else(|| panic!("incomplete percent escape in {value}"));
                decoded.push((hex_value(hi) << 4) | hex_value(lo));
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8(decoded).expect("valid utf-8 in form body")
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex digit: {}", byte as char),
    }
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
