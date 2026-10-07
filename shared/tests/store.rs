//! Record behaviour against a real Postgres.
//!
//! Gated because it needs a server. Point `BIP300_MONITOR_TEST_POSTGRES_URL` at
//! one, for example:
//!
//! ```text
//! docker run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=test \
//!     --name bip300-test-postgres postgres:18.2-alpine
//! BIP300_MONITOR_TEST_POSTGRES_URL='host=127.0.0.1 port=55432 user=postgres password=test dbname=postgres' \
//!     cargo test -p shared --features postgres_integration_tests
//! ```
#![cfg(feature = "postgres_integration_tests")]

use shared::nats::NatsArgs;
use shared::nats_subjects::Subject;
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::{Event, ObservedBlock, event::MonitorEvent};
use shared::recorder::Recorder;
use shared::store::{
    CaptureMethod, DatasetManifest, ExtractorWorker, HistoryPage, HistoryStatus, PostgresArgs,
    SidechainInstanceRef, SnapshotConsistency, SnapshotMetadata, Store, sidechain_instance_ref,
};
use std::time::{Duration, SystemTime};

/// Each test owns a database of its own so they can run concurrently.
async fn store_for(test: &str, source: &'static str) -> Store {
    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL")
        .expect("BIP300_MONITOR_TEST_POSTGRES_URL must point at a test Postgres");
    let (admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test Postgres");
    tokio::spawn(connection);

    let database = format!("bip300_test_{test}");
    admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS {database}"))
        .await
        .expect("drop any previous test database");
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .expect("create the test database");

    let store = Store::connect(&args_for(&admin_url, &database), source)
        .await
        .expect("connect and migrate the record");
    store
        .record(&[envelope(
            active_sidechains(),
            Some(ObservedBlock::at_height(vec![0x01; 32], 2)),
            1_700_000_000_000,
        )])
        .await
        .expect("seed current sidechain instances");
    store
}

fn args_for(admin_url: &str, database: &str) -> PostgresArgs {
    let mut args = PostgresArgs {
        postgres_db: database.to_owned(),
        ..PostgresArgs::default()
    };
    for setting in admin_url.split_whitespace() {
        let (key, value) = setting.split_once('=').expect("key=value setting");
        match key {
            "host" => args.postgres_host = value.to_owned(),
            "port" => args.postgres_port = value.parse().expect("numeric port"),
            "user" => args.postgres_user = value.to_owned(),
            "password" => args.postgres_password = Some(value.to_owned()),
            _ => {}
        }
    }
    args
}

async fn query_client(test: &str) -> tokio_postgres::Client {
    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL")
        .expect("BIP300_MONITOR_TEST_POSTGRES_URL must point at a test Postgres");
    let mut config = admin_url
        .parse::<tokio_postgres::Config>()
        .expect("parse the test Postgres URL");
    config.dbname(format!("bip300_test_{test}"));
    let (client, connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .expect("connect to the individual test database");
    tokio::spawn(connection);
    client
}

fn envelope(
    payload: events::enforcer_event::Event,
    anchor: Option<ObservedBlock>,
    timestamp: u64,
) -> Event {
    Event {
        timestamp,
        observed_at_block: anchor,
        monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
            event: Some(payload),
        })),
    }
}

fn ctip(sidechain_number: u32, value_sats: u64) -> events::enforcer_event::Event {
    events::enforcer_event::Event::Ctip(events::CtipSnapshot {
        sidechain_number,
        ctip: Some(events::Ctip {
            txid: vec![0x11; 32],
            vout: 0,
            value_sats,
            sequence_number: 1,
        }),
    })
}

/// A kind that carries no slot, so its row records `sidechain = NULL`.
fn chain_info() -> events::enforcer_event::Event {
    events::enforcer_event::Event::ChainInfo(events::ChainInfo {
        network: events::Network::Mainnet as i32,
        raw_network: events::Network::Mainnet as i32,
        bip300_constants: Some(events::Bip300Constants {
            withdrawal_bundle_max_age: 26_300,
            withdrawal_bundle_inclusion_threshold: 13_150,
            used_sidechain_slot_proposal_max_age: 26_300,
            used_sidechain_slot_activation_threshold: 160,
            unused_sidechain_slot_proposal_max_age: 2_016,
            unused_sidechain_slot_activation_threshold: 4,
            activation_height: 963_648,
        }),
    })
}

fn chain_tip(hash: u8, height: u32) -> events::enforcer_event::Event {
    events::enforcer_event::Event::ChainTip(events::ChainTip {
        header: Some(events::BlockHeader {
            hash: vec![hash; 32],
            previous_hash: vec![hash.wrapping_sub(1); 32],
            height,
            block_work: vec![1; 32],
            cumulative_work: vec![0x44; 32],
            timestamp: 1_750_000_000,
        }),
    })
}

fn bmm_requests(parent: u8, bid_sats: u64) -> events::enforcer_event::Event {
    events::enforcer_event::Event::BmmRequests(events::BmmRequestsSnapshot {
        previous_mainchain_block_hash: vec![parent; 32],
        requests: vec![events::BmmRequest {
            sidechain_number: 9,
            txid: vec![bid_sats as u8; 32],
            critical_hash: vec![0x33; 32],
            bid_sats,
        }],
    })
}

fn sidechain_proposals() -> events::enforcer_event::Event {
    events::enforcer_event::Event::SidechainProposals(events::SidechainProposalsSnapshot {
        proposals: Vec::new(),
    })
}

fn active_sidechain(sidechain_number: u32) -> events::ActiveSidechain {
    let mut raw_description = vec![32];
    raw_description.extend_from_slice(&[sidechain_number as u8; 32]);
    let description_hash = shared::bip300::sidechain_description_hash(&raw_description).unwrap();
    events::ActiveSidechain {
        sidechain_number,
        raw_description,
        vote_count: 4,
        proposal_height: 1,
        activation_height: 2,
        declaration: None,
        description_hash,
    }
}

fn replacement_sidechain(
    sidechain_number: u32,
    description: Vec<u8>,
    proposal_height: u32,
    activation_height: u32,
) -> events::ActiveSidechain {
    assert!(description.len() < 0xfd);
    let mut raw_description = vec![description.len() as u8];
    raw_description.extend_from_slice(&description);
    let description_hash = shared::bip300::sidechain_description_hash(&raw_description).unwrap();
    events::ActiveSidechain {
        sidechain_number,
        raw_description,
        proposal_height,
        activation_height,
        description_hash,
        ..Default::default()
    }
}

fn instance(sidechain_number: u8) -> SidechainInstanceRef {
    sidechain_instance_ref(&active_sidechain(u32::from(sidechain_number)))
        .expect("valid test sidechain instance")
}

fn instance_id(sidechain_number: u8) -> String {
    instance(sidechain_number).sidechain_instance_id
}

fn active_sidechains() -> events::enforcer_event::Event {
    events::enforcer_event::Event::ActiveSidechains(events::ActiveSidechainsSnapshot {
        sidechains: [9_u32, 98].into_iter().map(active_sidechain).collect(),
    })
}

fn connected(sidechain_number: u32, height: u32, hash: u8) -> events::enforcer_event::Event {
    events::enforcer_event::Event::BlockConnected(events::BlockConnected {
        header: Some(events::BlockHeader {
            hash: vec![hash; 32],
            previous_hash: vec![hash.wrapping_sub(1); 32],
            height,
            block_work: vec![1; 32],
            cumulative_work: vec![0x44; 32],
            timestamp: 1_750_000_000,
        }),
        sidechain_number,
        bmm_commitment: None,
        events: Vec::new(),
    })
}

#[tokio::test]
async fn instance_scoped_writes_survive_replacement_without_cross_tagging_slots() {
    let store = store_for("instance_scoped_write", "enforcer").await;
    let old_instance = instance(9);
    let replacement =
        events::enforcer_event::Event::ActiveSidechains(events::ActiveSidechainsSnapshot {
            sidechains: vec![replacement_sidechain(9, vec![0xbb; 32], 3, 4)],
        });
    store
        .record(&[envelope(
            replacement,
            Some(ObservedBlock::at_height(vec![0x04; 32], 4)),
            1_700_000_000_004,
        )])
        .await
        .expect("record replacement");

    let old_stream_event = envelope(
        connected(9, 5, 0x05),
        Some(ObservedBlock::at_height(vec![0x05; 32], 5)),
        1_700_000_000_005,
    );
    assert_eq!(
        store
            .record_with_method_for_instance(
                &[old_stream_event],
                CaptureMethod::Live,
                &old_instance,
            )
            .await
            .expect("record an in-flight event from the retired stream"),
        1
    );

    let wrong_slot_event = envelope(
        connected(98, 5, 0x15),
        Some(ObservedBlock::at_height(vec![0x15; 32], 5)),
        1_700_000_000_006,
    );
    assert!(
        store
            .record_with_method_for_instance(
                &[wrong_slot_event],
                CaptureMethod::Live,
                &old_instance,
            )
            .await
            .is_err(),
        "an explicit instance must not tag an event from another slot"
    );
    assert_eq!(
        store
            .current_sidechain_instance_id(130)
            .await
            .expect("query an inactive slot"),
        None,
        "an inactive slot is absence, not a fatal lookup error"
    );
    let current = store
        .current_sidechain_instances()
        .await
        .expect("query the durable active set");
    assert_eq!(
        current
            .iter()
            .map(|instance| (instance.sidechain, instance.activation_height))
            .collect::<Vec<_>>(),
        vec![(9, 4)]
    );
}

#[tokio::test]
async fn invalid_publisher_configuration_does_not_create_an_extractor_run() {
    let test = "publisher_before_run";
    let _store = store_for(test, "enforcer").await;
    let client = query_client(test).await;
    let before: i64 = client
        .query_one("SELECT count(*) FROM extractor_run", &[])
        .await
        .expect("count initial runs")
        .get(0);
    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL").unwrap();
    let invalid_nats = NatsArgs {
        nats_username: Some("monitor".to_owned()),
        nats_password: Some("secret".to_owned()),
        nats_password_file: Some("unused-conflicting-password-file".into()),
        ..NatsArgs::default()
    };

    let result = Recorder::connect(
        &args_for(&admin_url, &format!("bip300_test_{test}")),
        &invalid_nats,
        Subject::Enforcer,
        "enforcer",
        "publisher-before-run-test",
    )
    .await;
    let error = match result {
        Ok(_) => panic!("conflicting NATS credentials must fail startup"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("publisher"));

    let after: i64 = client
        .query_one("SELECT count(*) FROM extractor_run", &[])
        .await
        .expect("count runs after failed startup")
        .get(0);
    assert_eq!(after, before, "publisher failure must precede run creation");
}

#[tokio::test]
async fn a_new_start_closes_an_orphaned_run_before_claiming_the_dataset() {
    let test = "orphaned_run_reconciliation";
    let first = store_for(test, "enforcer").await;
    assert_eq!(first.previous_run_tip(), None, "a new dataset has no gap");
    let last_seen = ObservedBlock::at_height(vec![0x42; 32], 42);
    first
        .record_tip_observation(&last_seen, None, CaptureMethod::Poll, SystemTime::now())
        .await
        .unwrap();
    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL").unwrap();
    let replacement = Store::connect(
        &args_for(&admin_url, &format!("bip300_test_{test}")),
        "enforcer",
    )
    .await
    .expect("replace the orphaned run");
    // The replacement subscription opens a gap that starts at the last tip the
    // previous run saw; nothing between it and the new tip is evidence.
    assert_eq!(replacement.previous_run_tip(), Some(&last_seen));

    let client = query_client(test).await;
    let counts = client
        .query_one(
            "SELECT count(*) FILTER (WHERE status = 'running'),
                    count(*) FILTER (
                        WHERE status = 'failed'
                          AND finish_reason = 'superseded by extractor startup after unclean termination'
                    )
               FROM extractor_run
              WHERE source = 'enforcer'",
            &[],
        )
        .await
        .expect("query reconciled runs");
    assert_eq!(counts.get::<_, i64>(0), 1);
    assert_eq!(counts.get::<_, i64>(1), 1);
    let gaps = client
        .query_one(
            "SELECT count(*) FROM observation_failure f JOIN extractor_run r USING (run_id)
             WHERE f.worker = 'mainchain_events' AND r.status = 'running'
               AND f.error LIKE '%offline transitions are unknown%'",
            &[],
        )
        .await
        .expect("query the restart discontinuity");
    assert_eq!(
        gaps.get::<_, i64>(0),
        1,
        "one gap, only on the replacement run"
    );
}

#[tokio::test]
async fn v8_refuses_to_reuse_an_older_contract_dataset() {
    let test = "v6_identity_boundary";
    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL").unwrap();
    let (admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test Postgres");
    tokio::spawn(connection);
    let database = format!("bip300_test_{test}");
    admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS {database}"))
        .await
        .expect("drop any previous legacy test database");
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .expect("create the legacy test database");

    // The previous contract also had a fresh-dataset boundary: v8 facts must
    // not share a dataset with v9 facts either.
    let legacy_manifest = DatasetManifest {
        event_contract_version: 8,
        ..DatasetManifest::default()
    };
    let legacy = Store::connect_with_manifest(
        &args_for(&admin_url, &database),
        "enforcer",
        legacy_manifest,
    )
    .await
    .expect("start a legacy-contract run");
    legacy
        .record(&[envelope(
            active_sidechains(),
            Some(ObservedBlock::at_height(vec![0x01; 32], 2)),
            1_700_000_000_000,
        )])
        .await
        .expect("seed legacy sidechain identities");

    let error = match Store::connect(&args_for(&admin_url, &database), "enforcer").await {
        Ok(_) => panic!("v9 must require a fresh dataset after legacy identities exist"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("event contract v9 requires a fresh v9 dataset"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn worker_errors_are_isolated_and_aggregated() {
    let test = "worker_status_isolation";
    let store = store_for(test, "enforcer").await;
    let client = query_client(test).await;
    store
        .initialize_worker_statuses(&[ExtractorWorker::MainchainTip, ExtractorWorker::BmmRequests])
        .await
        .expect("initialize worker statuses");

    store
        .record_worker_failure(ExtractorWorker::MainchainTip, "tip RPC unavailable", 1)
        .await
        .expect("record tip worker error");
    for attempt in 1..=3 {
        let failures = store
            .record_worker_failure(
                ExtractorWorker::BmmRequests,
                &format!("BMM RPC unavailable attempt {attempt}"),
                3,
            )
            .await
            .expect("record BMM worker error");
        assert_eq!(failures, attempt);
    }
    let recorded: Option<String> = client
        .query_one(
            "SELECT last_error FROM extractor_status WHERE source = 'enforcer'",
            &[],
        )
        .await
        .expect("query extractor error")
        .get(0);
    assert_eq!(
        recorded.as_deref(),
        Some("bmm_requests: BMM RPC unavailable attempt 3; mainchain_tip: tip RPC unavailable")
    );

    store
        .record_worker_success(ExtractorWorker::MainchainTip)
        .await
        .expect("recover tip worker");
    let bmm_only: Option<String> = client
        .query_one(
            "SELECT last_error FROM extractor_status WHERE source = 'enforcer'",
            &[],
        )
        .await
        .expect("query BMM-only extractor status")
        .get(0);
    assert_eq!(
        bmm_only.as_deref(),
        Some("bmm_requests: BMM RPC unavailable attempt 3")
    );

    let tip = ObservedBlock::at_height(vec![0x44; 32], 200);
    store
        .record_tip_observation(
            &tip,
            None,
            CaptureMethod::Poll,
            std::time::SystemTime::now(),
        )
        .await
        .expect("record tip without changing worker errors");
    let after_tip: Option<String> = client
        .query_one(
            "SELECT last_error FROM extractor_status WHERE source = 'enforcer'",
            &[],
        )
        .await
        .expect("query extractor status after tip")
        .get(0);
    assert_eq!(after_tip, bmm_only, "tip writes must not clear BMM health");

    store
        .record_worker_failure(
            ExtractorWorker::BmmRequests,
            "BMM RPC unavailable attempt 4",
            3,
        )
        .await
        .expect("rewrite sustained BMM error");
    let worker_row = client
        .query_one(
            "SELECT consecutive_failures, last_error,
                    last_success_at IS NULL, last_failure_at IS NOT NULL
               FROM extractor_worker_status
              WHERE worker = 'bmm_requests'",
            &[],
        )
        .await
        .expect("query sustained BMM failure");
    let worker: (i32, Option<String>, bool, bool) = (
        worker_row.get(0),
        worker_row.get(1),
        worker_row.get(2),
        worker_row.get(3),
    );
    assert_eq!(worker.0, 4);
    assert_eq!(worker.1.as_deref(), Some("BMM RPC unavailable attempt 4"));
    assert!(worker.2);
    assert!(worker.3);

    store
        .record_worker_success(ExtractorWorker::BmmRequests)
        .await
        .expect("recover BMM worker");
    let cleared: Option<String> = client
        .query_one(
            "SELECT last_error FROM extractor_status WHERE source = 'enforcer'",
            &[],
        )
        .await
        .expect("query recovered aggregate status")
        .get(0);
    assert_eq!(cleared, None);
}

#[tokio::test]
async fn pre_v4_coverage_is_isolated_and_triggers_a_safe_rebackfill() {
    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL")
        .expect("BIP300_MONITOR_TEST_POSTGRES_URL must point at a test Postgres");
    let (admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test Postgres");
    tokio::spawn(connection);
    let database = "bip300_test_legacy_coverage";
    admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS {database}"))
        .await
        .expect("drop previous legacy fixture");
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .expect("create legacy fixture");

    let mut config = admin_url
        .parse::<tokio_postgres::Config>()
        .expect("parse test Postgres URL");
    config.dbname(database);
    let (legacy, connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .expect("connect to legacy fixture");
    tokio::spawn(connection);
    legacy
        .batch_execute(include_str!("../schema/0001_event.sql"))
        .await
        .expect("apply schema v1");
    legacy
        .batch_execute(include_str!("../schema/0002_event_identity_nulls.sql"))
        .await
        .expect("apply schema v2");
    legacy
        .batch_execute(include_str!("../schema/0003_history_coverage.sql"))
        .await
        .expect("apply schema v3");
    legacy
        .batch_execute(
            "CREATE TABLE schema_version (
                 version integer PRIMARY KEY,
                 applied_at timestamptz NOT NULL DEFAULT now()
             );
             INSERT INTO schema_version (version) VALUES (1), (2), (3);",
        )
        .await
        .expect("mark legacy schema versions");
    legacy
        .execute(
            "INSERT INTO history_coverage
                (source, stream, sidechain, coverage_start_height,
                 covered_tip_hash, covered_tip_height,
                 target_tip_hash, target_tip_height,
                 floor_hash, floor_height, next_hash, next_height,
                 status, rows_recorded, effective_page_blocks, completed_at)
             VALUES
                ('enforcer', 'block', 9, 101,
                 $1, 104, $1, 104,
                 NULL, 100, NULL, NULL,
                 'complete', 4, 128, now())",
            &[&vec![0x68_u8; 32]],
        )
        .await
        .expect("seed ambiguous pre-v4 coverage");
    drop(legacy);

    let store = Store::connect(&args_for(&admin_url, database), "enforcer")
        .await
        .expect("migrate the legacy record");
    let client = query_client("legacy_coverage").await;
    let row = client
        .query_one(
            "SELECT dataset_id::text, event_contract_version, sidechain_instance_id
               FROM history_coverage
              WHERE stream = 'block' AND sidechain = 9",
            &[],
        )
        .await
        .expect("query migrated legacy cursor");
    assert_eq!(
        row.get::<_, String>(0),
        "00000000-0000-0000-0000-000000000001"
    );
    assert_eq!(row.get::<_, i32>(1), 1);
    assert_eq!(row.get::<_, Option<String>>(2), None);
    assert_eq!(
        store
            .history_coverage("block", Some(9), Some(&instance_id(9)))
            .await
            .expect("look up current-contract coverage"),
        None,
        "ambiguous legacy coverage must not satisfy a current instance"
    );
}

#[tokio::test]
async fn v5_backfills_fact_hash_when_a_legacy_envelope_hash_is_null() {
    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL")
        .expect("BIP300_MONITOR_TEST_POSTGRES_URL must point at a test Postgres");
    let (admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test Postgres");
    tokio::spawn(connection);
    let database = "bip300_test_legacy_null_envelope_hash";
    admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS {database}"))
        .await
        .expect("drop previous legacy fixture");
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .expect("create legacy fixture");

    let mut config = admin_url
        .parse::<tokio_postgres::Config>()
        .expect("parse test Postgres URL");
    config.dbname(database);
    let (legacy, connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .expect("connect to legacy fixture");
    tokio::spawn(connection);
    for migration in [
        include_str!("../schema/0001_event.sql"),
        include_str!("../schema/0002_event_identity_nulls.sql"),
        include_str!("../schema/0003_history_coverage.sql"),
        include_str!("../schema/0004_observation_provenance.sql"),
    ] {
        legacy
            .batch_execute(migration)
            .await
            .expect("apply legacy migration");
    }
    legacy
        .batch_execute(
            "CREATE TABLE schema_version (
                 version integer PRIMARY KEY,
                 applied_at timestamptz NOT NULL DEFAULT now()
             );
             INSERT INTO schema_version (version) VALUES (1), (2), (3), (4);
             INSERT INTO event
                 (observed_at, source, kind, sidechain, block_hash, height,
                  envelope, payload, dataset_id, event_contract_version,
                  envelope_sha256, sidechain_instance_id)
             VALUES
                 (now(), 'enforcer', 'chain_info', NULL,
                  decode(repeat('11', 32), 'hex'), 1,
                  decode('0102', 'hex'), '{}'::jsonb,
                  '00000000-0000-0000-0000-000000000001', 3, NULL, NULL);",
        )
        .await
        .expect("seed a v4 row without an envelope hash");
    drop(legacy);

    Store::connect(&args_for(&admin_url, database), "enforcer")
        .await
        .expect("migrate the legacy record to v5");
    let client = query_client("legacy_null_envelope_hash").await;
    let row = client
        .query_one(
            "SELECT fact_sha256, sha256(envelope)
               FROM event
              WHERE kind = 'chain_info'",
            &[],
        )
        .await
        .expect("query the backfilled fact hash");
    let fact_hash: Vec<u8> = row.get(0);
    let expected_hash: Vec<u8> = row.get(1);
    assert_eq!(fact_hash, expected_hash);
}

#[tokio::test]
async fn migrating_a_record_that_already_exists_is_a_no_op() {
    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL")
        .expect("BIP300_MONITOR_TEST_POSTGRES_URL must point at a test Postgres");
    let store = store_for("migrate_twice", "enforcer").await;
    let anchor = ObservedBlock::at_height(vec![0x11; 32], 996_260);
    store
        .record(&[envelope(ctip(9, 100), Some(anchor), 1_700_000_000_000)])
        .await
        .expect("record before the second migration");

    // Every extractor restart re-runs the migration path against a record that
    // already holds rows. It must neither fail nor lose them.
    let restarted = Store::connect(
        &args_for(&admin_url, "bip300_test_migrate_twice"),
        "enforcer",
    )
    .await
    .expect("reconnect and re-migrate");

    assert_eq!(
        restarted
            .record(&[envelope(
                connected(9, 996_261, 0x61),
                Some(ObservedBlock::at_height(vec![0x61; 32], 996_261)),
                1_700_000_001_000,
            )])
            .await
            .expect("record after the second migration"),
        1
    );
}

#[tokio::test]
async fn recording_the_same_observation_twice_keeps_one_row() {
    let store = store_for("idempotent", "enforcer").await;
    let anchor = ObservedBlock::at_height(vec![0x22; 32], 996_260);
    let event = envelope(ctip(9, 100), Some(anchor.clone()), 1_700_000_000_000);

    assert_eq!(
        store
            .record(std::slice::from_ref(&event))
            .await
            .expect("record"),
        1
    );
    // The republish after a restart and the blocks replayed by a backfill are
    // both observations the record already holds.
    assert_eq!(store.record(&[event]).await.expect("replay"), 0);
}

#[tokio::test]
async fn recording_the_same_global_observation_twice_keeps_one_row() {
    // chain_info carries no slot, so its row records `sidechain = NULL`. A
    // unique constraint that treats NULLs as distinct never matches those, and
    // every restart would insert the snapshot again at the same block.
    let store = store_for("idempotent_global", "enforcer").await;
    let anchor = ObservedBlock::at_height(vec![0x22; 32], 996_260);
    let event = envelope(chain_info(), Some(anchor), 1_700_000_000_000);

    assert_eq!(
        store
            .record(std::slice::from_ref(&event))
            .await
            .expect("record"),
        1
    );
    assert_eq!(store.record(&[event]).await.expect("replay"), 0);
}

#[tokio::test]
async fn replaying_a_whole_snapshot_adds_no_rows() {
    // The shape `record_snapshot` writes on every startup: two slot-less
    // constants, two slot-less mutable snapshots, and one payload per slot, all
    // anchored to the same tip.
    let store = store_for("idempotent_snapshot", "enforcer").await;
    let anchor = ObservedBlock::at_height(vec![0x22; 32], 996_260);
    let snapshot: Vec<Event> = [
        chain_info(),
        chain_tip(0x22, 996_260),
        sidechain_proposals(),
        active_sidechains(),
        ctip(9, 100),
        ctip(98, 200),
    ]
    .into_iter()
    .map(|payload| envelope(payload, Some(anchor.clone()), 1_700_000_000_000))
    .collect();

    assert_eq!(
        store.record(&snapshot).await.expect("record the snapshot"),
        snapshot.len() as u64
    );
    assert_eq!(
        store.record(&snapshot).await.expect("republish"),
        0,
        "a restart at the same tip must not grow the record"
    );
}

#[tokio::test]
async fn a_batch_is_all_or_nothing() {
    let store = store_for("atomic_batch", "enforcer").await;
    let instance_id = instance_id(9);
    let good = envelope(
        ctip(9, 100),
        Some(ObservedBlock::at_height(vec![0x44; 32], 996_262)),
        1_700_000_000_000,
    );
    let malformed = Event {
        timestamp: 1_700_000_000_000,
        observed_at_block: None,
        monitor_event: None,
    };

    store
        .record(&[good, malformed])
        .await
        .expect_err("a malformed event must fail the batch");

    assert_eq!(
        store
            .last_recorded_height("ctip", 9, &instance_id)
            .await
            .expect("query the record"),
        None,
        "the valid event of a failed batch must not have been committed"
    );
}

#[tokio::test]
async fn the_checkpoint_reports_the_newest_recorded_block_per_slot() {
    let store = store_for("checkpoint", "enforcer").await;
    let instance_9 = instance_id(9);
    let instance_98 = instance_id(98);
    // Written out of order on purpose: the checkpoint is the highest block, not
    // the last one written.
    for (height, hash) in [(996_260_u32, 0x60_u8), (996_262, 0x62), (996_261, 0x61)] {
        store
            .record(&[envelope(
                connected(9, height, hash),
                Some(ObservedBlock::at_height(vec![hash; 32], height)),
                1_700_000_000_000 + u64::from(height),
            )])
            .await
            .expect("record a block");
    }
    store
        .record(&[envelope(
            connected(98, 996_999, 0x99),
            Some(ObservedBlock::at_height(vec![0x99; 32], 996_999)),
            1_700_000_002_000,
        )])
        .await
        .expect("record another slot");

    assert_eq!(
        store
            .last_recorded_height("block_connected", 9, &instance_9)
            .await
            .expect("query the checkpoint"),
        Some(996_262)
    );
    assert_eq!(
        store
            .last_recorded_height("block_connected", 98, &instance_98)
            .await
            .expect("query the checkpoint"),
        Some(996_999),
        "another slot must not move this slot's checkpoint"
    );
}

#[tokio::test]
async fn a_disconnect_without_a_height_does_not_move_the_checkpoint() {
    let store = store_for("disconnect_height", "enforcer").await;
    let instance_id = instance_id(9);
    let disconnect = envelope(
        events::enforcer_event::Event::BlockDisconnected(events::BlockDisconnected {
            block_hash: vec![0x55; 32],
            sidechain_number: 9,
        }),
        Some(ObservedBlock::without_height(vec![0x55; 32])),
        1_700_000_000_000,
    );

    assert_eq!(store.record(&[disconnect]).await.expect("record"), 1);
    assert_eq!(
        store
            .last_recorded_height("block_disconnected", 9, &instance_id)
            .await
            .expect("query the checkpoint"),
        None,
        "an absent height must not be recorded as zero"
    );
}

#[tokio::test]
async fn historical_pages_commit_events_and_cursor_together() {
    let store = store_for("history_pages", "enforcer").await;
    let instance_id = instance_id(9);
    let target = ObservedBlock::at_height(vec![0x68; 32], 104);
    let mut coverage = store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&instance_id),
            101,
            None,
            &target,
            None,
            Some(100),
            2,
        )
        .await
        .expect("start history");
    assert_eq!(coverage.status, HistoryStatus::Running);
    assert_eq!(coverage.next, Some(target.clone()));

    let cursor = coverage.next.clone().expect("first cursor");
    let next = ObservedBlock::at_height(vec![0x66; 32], 102);
    let first = [
        envelope(
            connected(9, 103, 0x67),
            Some(ObservedBlock::at_height(vec![0x67; 32], 103)),
            1_700_000_000_103,
        ),
        envelope(
            connected(9, 104, 0x68),
            Some(ObservedBlock::at_height(vec![0x68; 32], 104)),
            1_700_000_000_104,
        ),
    ];
    assert_eq!(
        store
            .record_history_page(
                &first,
                HistoryPage {
                    stream: "block",
                    sidechain: Some(9),
                    sidechain_instance_id: Some(&instance_id),
                    expected_next: &cursor,
                    next: Some(&next),
                },
            )
            .await
            .expect("record first page"),
        2
    );
    coverage = store
        .history_coverage("block", Some(9), Some(&instance_id))
        .await
        .expect("read coverage")
        .expect("coverage exists");
    assert_eq!(coverage.next, Some(next.clone()));
    assert_eq!(coverage.rows_recorded, 2);

    let second = [
        envelope(
            connected(9, 101, 0x65),
            Some(ObservedBlock::at_height(vec![0x65; 32], 101)),
            1_700_000_000_101,
        ),
        envelope(
            connected(9, 102, 0x66),
            Some(ObservedBlock::at_height(vec![0x66; 32], 102)),
            1_700_000_000_102,
        ),
    ];
    store
        .record_history_page(
            &second,
            HistoryPage {
                stream: "block",
                sidechain: Some(9),
                sidechain_instance_id: Some(&instance_id),
                expected_next: &next,
                next: None,
            },
        )
        .await
        .expect("record final page");
    coverage = store
        .history_coverage("block", Some(9), Some(&instance_id))
        .await
        .expect("read coverage")
        .expect("coverage exists");
    assert_eq!(coverage.status, HistoryStatus::Complete);
    assert_eq!(coverage.next, None);
    assert_eq!(coverage.covered_tip, Some(target));
    assert_eq!(coverage.rows_recorded, 4);
}

#[tokio::test]
async fn a_failed_historical_page_advances_neither_events_nor_cursor() {
    let store = store_for("atomic_history_page", "enforcer").await;
    let instance_id = instance_id(9);
    let target = ObservedBlock::at_height(vec![0x68; 32], 104);
    store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&instance_id),
            101,
            None,
            &target,
            None,
            Some(100),
            2,
        )
        .await
        .expect("start history");
    let good = envelope(
        connected(9, 104, 0x68),
        Some(target.clone()),
        1_700_000_000_104,
    );
    let malformed = Event {
        timestamp: 1_700_000_000_103,
        observed_at_block: None,
        monitor_event: None,
    };
    let next = ObservedBlock::at_height(vec![0x66; 32], 102);

    store
        .record_history_page(
            &[good, malformed],
            HistoryPage {
                stream: "block",
                sidechain: Some(9),
                sidechain_instance_id: Some(&instance_id),
                expected_next: &target,
                next: Some(&next),
            },
        )
        .await
        .expect_err("malformed page must roll back");

    let coverage = store
        .history_coverage("block", Some(9), Some(&instance_id))
        .await
        .expect("read coverage")
        .expect("coverage exists");
    assert_eq!(coverage.next, Some(target));
    assert_eq!(coverage.rows_recorded, 0);
    assert_eq!(
        store
            .last_recorded_height("block_connected", 9, &instance_id)
            .await
            .expect("read event rows"),
        None
    );
}

#[tokio::test]
async fn a_failed_history_cursor_resumes_with_its_smaller_page() {
    let store = store_for("resume_history", "enforcer").await;
    let instance_id = instance_id(9);
    let target = ObservedBlock::at_height(vec![0x68; 32], 104);
    store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&instance_id),
            101,
            None,
            &target,
            None,
            Some(100),
            128,
        )
        .await
        .expect("start history");
    store
        .resize_history_page("block", Some(9), Some(&instance_id), 64, "deadline")
        .await
        .expect("resize page");
    let status = store
        .settle_history_failure("block", Some(9), Some(&instance_id), "bad response")
        .await
        .expect("mark failed");
    assert_eq!(status, HistoryStatus::Error);
    store
        .resume_history("block", Some(9), Some(&instance_id))
        .await
        .expect("resume exact cursor");

    let coverage = store
        .history_coverage("block", Some(9), Some(&instance_id))
        .await
        .expect("read coverage")
        .expect("coverage exists");
    assert_eq!(coverage.status, HistoryStatus::Running);
    assert_eq!(coverage.next, Some(target));
    assert_eq!(coverage.effective_page_blocks, 64);
    assert_eq!(coverage.last_error, None);
}

#[tokio::test]
async fn a_failure_racing_instance_retirement_settles_as_superseded() {
    let store = store_for("retired_history_failure", "enforcer").await;
    let instance_id = instance_id(9);
    let target = ObservedBlock::at_height(vec![0x68; 32], 104);
    store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&instance_id),
            2,
            None,
            &target,
            None,
            Some(1),
            128,
        )
        .await
        .expect("start history");
    store
        .record(&[envelope(
            events::enforcer_event::Event::ActiveSidechains(events::ActiveSidechainsSnapshot {
                sidechains: Vec::new(),
            }),
            Some(ObservedBlock::at_height(vec![0x02; 32], 3)),
            1_700_000_000_001,
        )])
        .await
        .expect("record retired sidechain snapshot");

    let status = store
        .settle_history_failure("block", Some(9), Some(&instance_id), "late RPC failure")
        .await
        .expect("settle retired history");

    assert_eq!(status, HistoryStatus::Superseded);
    let coverage = store
        .history_coverage("block", Some(9), Some(&instance_id))
        .await
        .expect("read coverage")
        .expect("coverage exists");
    assert_eq!(coverage.status, HistoryStatus::Superseded);
}

#[tokio::test]
async fn an_already_superseded_failure_is_idempotent_before_state_catches_up() {
    let store = store_for("pre_durable_superseded_failure", "enforcer").await;
    let instance_id = instance_id(9);
    let target = ObservedBlock::at_height(vec![0x68; 32], 104);
    store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&instance_id),
            2,
            None,
            &target,
            None,
            Some(1),
            128,
        )
        .await
        .expect("start history");
    store
        .supersede_history("block", 9, &instance_id, "simulated pre-durable retirement")
        .await
        .expect("supersede history");
    assert_eq!(
        store
            .current_sidechain_instance_id(9)
            .await
            .expect("read current instance")
            .as_deref(),
        Some(instance_id.as_str()),
        "the test preserves the inconsistent window from the review"
    );

    let status = store
        .settle_history_failure("block", Some(9), Some(&instance_id), "late page failure")
        .await
        .expect("an already-superseded failure is not fatal");

    assert_eq!(status, HistoryStatus::Superseded);
}

#[tokio::test]
async fn revisiting_a_tip_preserves_the_full_a_b_a_occurrence_order() {
    let store = store_for("tip_a_b_a", "enforcer").await;
    let a = ObservedBlock::at_height(vec![0xaa; 32], 100);
    let b = ObservedBlock::at_height(vec![0xbb; 32], 101);
    let now = std::time::SystemTime::now();
    store
        .record_tip_observation(&a, None, CaptureMethod::Poll, now)
        .await
        .expect("record first A");
    store
        .record_tip_observation(&b, Some(&a), CaptureMethod::Poll, now)
        .await
        .expect("record B");
    store
        .record_tip_observation(&a, Some(&b), CaptureMethod::Poll, now)
        .await
        .expect("record second A");

    let client = query_client("tip_a_b_a").await;
    let rows = client
        .query(
            "SELECT tip_hash, previous_observed_hash
               FROM tip_observation
              ORDER BY tip_observation_id",
            &[],
        )
        .await
        .expect("query tip occurrences");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, Vec<u8>>(0), a.hash);
    assert_eq!(rows[0].get::<_, Option<Vec<u8>>>(1), None);
    assert_eq!(rows[1].get::<_, Vec<u8>>(0), b.hash);
    assert_eq!(rows[1].get::<_, Option<Vec<u8>>>(1), Some(a.hash.clone()));
    assert_eq!(rows[2].get::<_, Vec<u8>>(0), a.hash);
    assert_eq!(rows[2].get::<_, Option<Vec<u8>>>(1), Some(b.hash));
}

#[tokio::test]
async fn sidechain_replacement_and_reactivation_keep_distinct_instances() {
    let store = store_for("instance_replacement", "enforcer").await;
    let replacement = |description: Vec<u8>, proposal_height, activation_height| {
        events::enforcer_event::Event::ActiveSidechains(events::ActiveSidechainsSnapshot {
            sidechains: vec![replacement_sidechain(
                9,
                description,
                proposal_height,
                activation_height,
            )],
        })
    };
    store
        .record(&[envelope(
            replacement(vec![0xbb; 32], 3, 4),
            Some(ObservedBlock::at_height(vec![0x04; 32], 4)),
            1_700_000_000_004,
        )])
        .await
        .expect("record replacement B");
    store
        .record(&[envelope(
            replacement(vec![9; 32], 1, 2),
            Some(ObservedBlock::at_height(vec![0x05; 32], 5)),
            1_700_000_000_005,
        )])
        .await
        .expect("record reactivated A");

    let client = query_client("instance_replacement").await;
    let count: i64 = client
        .query_one(
            "SELECT count(*) FROM sidechain_instance WHERE sidechain = 9",
            &[],
        )
        .await
        .expect("count sidechain instances")
        .get(0);
    assert_eq!(count, 2, "A -> B -> A must retain A and B exactly once");
    let current: Vec<u8> = client
        .query_one(
            "SELECT instance.raw_description
               FROM current_sidechain_instance current
               JOIN sidechain_instance instance
                 USING (dataset_id, sidechain_instance_id)
              WHERE current.sidechain = 9",
            &[],
        )
        .await
        .expect("query current instance")
        .get(0);
    let mut expected = vec![32];
    expected.extend_from_slice(&[9; 32]);
    assert_eq!(current, expected);
}

#[tokio::test]
async fn a_new_event_contract_cannot_alias_an_older_normalized_fact() {
    let test = "event_contract_identity";
    let store = store_for(test, "enforcer").await;
    let anchor = ObservedBlock::at_height(vec![0xcc; 32], 200);
    let fact = envelope(chain_info(), Some(anchor.clone()), 1_700_000_000_200);
    assert_eq!(store.record(std::slice::from_ref(&fact)).await.unwrap(), 1);

    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL").unwrap();
    let initial_version = events::EVENT_CONTRACT_VERSION;
    let upgraded_version = initial_version + 1;
    // A newer converter never writes into an older contract's dataset.
    let manifest = DatasetManifest {
        event_contract_version: upgraded_version,
        ..DatasetManifest::default()
    };
    let error = match Store::connect_with_manifest(
        &args_for(&admin_url, &format!("bip300_test_{test}")),
        "enforcer",
        manifest,
    )
    .await
    {
        Ok(_) => panic!("a newer contract must not reuse an older dataset"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains(&format!("requires a fresh v{upgraded_version} dataset")),
        "unexpected error: {error:#}"
    );

    // The fact identity itself keeps versions apart as a second line of defence.
    let client = query_client(test).await;
    client
        .execute(
            "INSERT INTO event(dataset_id,event_contract_version,source,kind,sidechain,sidechain_instance_id,
                block_hash,height,observed_at,envelope,envelope_sha256,payload,fact_sha256)
             SELECT dataset_id,event_contract_version+1,source,kind,sidechain,sidechain_instance_id,
                block_hash,height,observed_at,envelope,envelope_sha256,payload,fact_sha256
               FROM event WHERE kind='chain_info' AND block_hash=$1",
            &[&anchor.hash],
        )
        .await
        .expect("an identical fact under another contract is a separate identity");
    let versions = client
        .query(
            "SELECT event_contract_version
               FROM event
              WHERE kind = 'chain_info' AND block_hash = $1
              ORDER BY event_contract_version",
            &[&anchor.hash],
        )
        .await
        .expect("query normalized fact versions")
        .into_iter()
        .map(|row| row.get::<_, i32>(0))
        .collect::<Vec<_>>();
    assert_eq!(
        versions,
        [
            i32::try_from(initial_version).unwrap(),
            i32::try_from(upgraded_version).unwrap()
        ]
    );
}

#[tokio::test]
async fn live_bmm_changes_share_a_parent_without_aliasing_facts() {
    let test = "bmm_fact_identity";
    let store = store_for(test, "enforcer").await;
    let anchor = ObservedBlock::at_height(vec![0xdd; 32], 967_700);
    let first = envelope(
        bmm_requests(0xdd, 10),
        Some(anchor.clone()),
        1_700_000_000_000,
    );
    let changed = envelope(
        bmm_requests(0xdd, 20),
        Some(anchor.clone()),
        1_700_000_005_000,
    );
    let repeated = envelope(
        bmm_requests(0xdd, 10),
        Some(anchor.clone()),
        1_700_000_010_000,
    );

    assert_eq!(store.record(&[first]).await.expect("first sample"), 1);
    assert_eq!(store.record(&[changed]).await.expect("changed sample"), 1);
    assert_eq!(
        store.record(&[repeated]).await.expect("repeated sample"),
        0,
        "an identical auction state should reuse its immutable fact"
    );

    let client = query_client(test).await;
    let fact_count: i64 = client
        .query_one(
            "SELECT count(*) FROM event WHERE kind = 'bmm_requests' AND block_hash = $1",
            &[&anchor.hash],
        )
        .await
        .expect("count BMM facts")
        .get(0);
    let observation_count: i64 = client
        .query_one(
            "SELECT count(*)
               FROM event_observation observation
               JOIN event fact ON fact.id = observation.event_id
              WHERE fact.kind = 'bmm_requests' AND fact.block_hash = $1",
            &[&anchor.hash],
        )
        .await
        .expect("count BMM observations")
        .get(0);
    assert_eq!(fact_count, 2);
    assert_eq!(observation_count, 3);

    let latest_row = client
        .query_one(
            "SELECT (fact.payload #>>
                        '{monitor_event,Enforcer,event,BmmRequests,requests,0,bid_sats}')::bigint,
                    (extract(epoch FROM observation.observed_at) * 1000)::bigint
               FROM event_observation observation
               JOIN event fact ON fact.id = observation.event_id
              WHERE observation.dataset_id = fact.dataset_id
                AND fact.kind = 'bmm_requests'
                AND fact.block_hash = $1
              ORDER BY observation.observed_at DESC, observation.observation_id DESC
              LIMIT 1",
            &[&anchor.hash],
        )
        .await
        .expect("query latest BMM observation");
    let latest: (i64, i64) = (latest_row.get(0), latest_row.get(1));
    assert_eq!(latest, (10, 1_700_000_010_000));
}

#[tokio::test]
async fn a_tip_matched_bmm_observation_proves_one_parent_in_payload_anchor_and_snapshot() {
    let test = "stable_bmm_parent";
    let store = store_for(test, "enforcer").await;
    let parent = ObservedBlock::at_height(vec![0xdd; 32], 967_700);
    let metadata = SnapshotMetadata {
        revision_before: None,
        revision_after: None,
        started_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        finished_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_001),
        tip_before: parent.clone(),
        tip_after: parent.clone(),
        consistency: SnapshotConsistency::TipMatched,
        attempts: 1,
    };
    store
        .record_snapshot(
            &[envelope(
                bmm_requests(0xdd, 10),
                Some(parent),
                1_700_000_000_500,
            )],
            CaptureMethod::Poll,
            &metadata,
        )
        .await
        .expect("record tip-matched BMM snapshot");

    let client = query_client(test).await;
    let verified: bool = client
        .query_one(
            "SELECT EXISTS (
                 SELECT 1
                   FROM extractor_run run
                   JOIN event_observation observation
                     ON observation.run_id = run.run_id
                    AND observation.dataset_id = run.dataset_id
                   JOIN snapshot_group snapshot
                     ON snapshot.snapshot_group_id = observation.snapshot_group_id
                    AND snapshot.run_id = run.run_id
                   JOIN event fact
                     ON fact.id = observation.event_id
                    AND fact.dataset_id = run.dataset_id
                    AND fact.event_contract_version = run.event_contract_version
                  WHERE run.source = 'enforcer' AND run.status = 'running'
                    AND observation.capture_method = 'poll'
                    AND fact.kind = 'bmm_requests'
                    AND snapshot.consistency = 'tip_matched'
                    AND snapshot.tip_before_hash = snapshot.tip_after_hash
                    AND snapshot.tip_before_hash = fact.block_hash
                    AND decode(
                        fact.payload #>> '{monitor_event,Enforcer,event,BmmRequests,previous_mainchain_block_hash}',
                        'hex'
                    ) = fact.block_hash
             )",
            &[],
        )
        .await
        .expect("verify stable BMM parent proof")
        .get(0);
    assert!(verified);
}

fn block_event(height: u32, hash: u8, parent: u8) -> Event {
    let mut payload = connected(9, height, hash);
    if let events::enforcer_event::Event::BlockConnected(block) = &mut payload {
        block.header.as_mut().unwrap().previous_hash = vec![parent; 32];
    }
    envelope(
        payload,
        Some(ObservedBlock::at_height(vec![hash; 32], height)),
        1_700_000_000_000 + u64::from(height),
    )
}

#[tokio::test]
async fn a_reorg_preserves_old_proof_and_joins_only_a_certified_prefix() {
    let store = store_for("certified_reorg", "enforcer").await;
    let id = instance_id(9);
    let old_tip = ObservedBlock::at_height(vec![104; 32], 104);
    store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&id),
            101,
            None,
            &old_tip,
            None,
            Some(100),
            128,
        )
        .await
        .unwrap();
    let events = (101..=104)
        .map(|h| block_event(h, h as u8, h as u8 - 1))
        .collect::<Vec<_>>();
    store
        .record_history_page(
            &events,
            HistoryPage {
                stream: "block",
                sidechain: Some(9),
                sidechain_instance_id: Some(&id),
                expected_next: &old_tip,
                next: None,
            },
        )
        .await
        .unwrap();
    // A live fact with no proven prefix must never serve as a repair floor.
    store.record(&[block_event(200, 200, 199)]).await.unwrap();
    let fork = ObservedBlock::at_height(vec![170; 32], 104);
    let progress = store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&id),
            101,
            None,
            &fork,
            None,
            Some(100),
            128,
        )
        .await
        .unwrap();
    assert_eq!(progress.covered_tip, Some(old_tip));
    assert_eq!(progress.status, HistoryStatus::Running);
    let floor = store
        .certified_history_floor(
            "block",
            Some(9),
            Some(&id),
            &[vec![200; 32], vec![170; 32], vec![103; 32]],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(floor.height, Some(103));
    store
        .record_history_page(
            &[block_event(104, 170, 103)],
            HistoryPage {
                stream: "block",
                sidechain: Some(9),
                sidechain_instance_id: Some(&id),
                expected_next: &fork,
                next: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .history_coverage("block", Some(9), Some(&id))
            .await
            .unwrap()
            .unwrap()
            .covered_tip,
        Some(fork)
    );
    let client = query_client("certified_reorg").await;
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM history_certified_block", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        5
    );
}

#[tokio::test]
async fn conflicting_block_payloads_survive_a_failed_history_certification() {
    let store = store_for("durable_conflict", "enforcer").await;
    let id = instance_id(9);
    let target = ObservedBlock::at_height(vec![104; 32], 104);
    let first = block_event(104, 104, 103);
    store.record(std::slice::from_ref(&first)).await.unwrap();
    let mut conflicting = first;
    if let Some(MonitorEvent::Enforcer(payload)) = &mut conflicting.monitor_event
        && let Some(events::enforcer_event::Event::BlockConnected(block)) = &mut payload.event
    {
        block.header.as_mut().unwrap().timestamp += 1;
    }
    store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&id),
            104,
            None,
            &target,
            None,
            Some(103),
            128,
        )
        .await
        .unwrap();
    let error = store
        .record_history_page(
            &[conflicting],
            HistoryPage {
                stream: "block",
                sidechain: Some(9),
                sidechain_instance_id: Some(&id),
                expected_next: &target,
                next: None,
            },
        )
        .await
        .unwrap_err();
    // A typed error: the backfill quarantines the scope instead of crashing.
    assert!(error.is::<shared::store::HistoryConflict>());
    let client = query_client("durable_conflict").await;
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM event WHERE kind='block_connected'",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM event_conflict", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        store
            .history_coverage("block", Some(9), Some(&id))
            .await
            .unwrap()
            .unwrap()
            .last_error
            .as_deref(),
        Some(shared::store::HISTORY_CONFLICT)
    );
}

#[tokio::test]
async fn an_old_conflict_does_not_fail_pages_of_other_blocks() {
    let store = store_for("conflict_scope", "enforcer").await;
    let id = instance_id(9);
    // Two different live payloads for block 103 are a retained conflict.
    let first = block_event(103, 103, 102);
    let mut conflicting = first.clone();
    if let Some(MonitorEvent::Enforcer(payload)) = &mut conflicting.monitor_event
        && let Some(events::enforcer_event::Event::BlockConnected(block)) = &mut payload.event
    {
        block.header.as_mut().unwrap().timestamp += 1;
    }
    store.record(&[first, conflicting]).await.unwrap();
    let target = ObservedBlock::at_height(vec![105; 32], 105);
    store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&id),
            104,
            None,
            &target,
            None,
            Some(103),
            128,
        )
        .await
        .unwrap();
    // Pages above it are judged on their own blocks.
    store
        .record_history_page(
            &[block_event(105, 105, 104)],
            HistoryPage {
                stream: "block",
                sidechain: Some(9),
                sidechain_instance_id: Some(&id),
                expected_next: &target,
                next: Some(&ObservedBlock::at_height(vec![104; 32], 104)),
            },
        )
        .await
        .expect("an unrelated older conflict must not suspend this page");
}

#[tokio::test]
async fn an_orphan_at_the_missing_height_does_not_certify_the_target_chain() {
    let store = store_for("orphan_height", "enforcer").await;
    let id = instance_id(9);
    store.record(&[block_event(101, 101, 100)]).await.unwrap();
    let target = ObservedBlock::at_height(vec![170; 32], 102);
    store
        .begin_history_cycle(
            "block",
            Some(9),
            Some(&id),
            101,
            None,
            &target,
            None,
            Some(100),
            128,
        )
        .await
        .unwrap();
    store
        .record_history_page(
            &[block_event(102, 170, 169)],
            HistoryPage {
                stream: "block",
                sidechain: Some(9),
                sidechain_instance_id: Some(&id),
                expected_next: &target,
                next: None,
            },
        )
        .await
        .expect_err("counting two heights cannot prove their parent linkage");
    assert_eq!(
        store
            .history_coverage("block", Some(9), Some(&id))
            .await
            .unwrap()
            .unwrap()
            .status,
        HistoryStatus::Running
    );
}

async fn fee_job(client: &tokio_postgres::Client) -> (String, i32) {
    let row = client
        .query_one("SELECT status, attempts FROM bmm_fee_job", &[])
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

/// A second recorder for another source of the same dataset.
async fn store_in(test: &str, source: &'static str) -> Store {
    let admin_url = std::env::var("BIP300_MONITOR_TEST_POSTGRES_URL")
        .expect("BIP300_MONITOR_TEST_POSTGRES_URL must point at a test Postgres");
    Store::connect(
        &args_for(&admin_url, &format!("bip300_test_{test}")),
        source,
    )
    .await
    .expect("connect another source to the test record")
}

/// A node block whose raw bytes hold a real transaction, so fees can name it.
fn node_block_with_transaction(height: u32, hash: u8, parent: u8) -> (Event, Vec<u8>) {
    let block = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
    let txid = hex::decode(block.txdata[0].compute_txid().to_string()).unwrap();
    let mut event = node_block_event(height, hash, parent);
    if let Some(MonitorEvent::Node(node)) = &mut event.monitor_event
        && let Some(shared::protobuf::event::node_event::Event::MainchainBlock(raw)) =
            &mut node.event
    {
        raw.raw_block = bitcoin::consensus::serialize(&block);
    }
    (event, txid)
}

#[tokio::test]
async fn confirmed_fees_resume_and_never_rewrite_the_block() {
    let test = "confirmed_fees";
    let enforcer = store_for(test, "enforcer").await;
    let node = store_in(test, "node").await;
    let (block, txid) = node_block_with_transaction(4, 4, 3);
    let header = match &block.monitor_event {
        Some(MonitorEvent::Node(n)) => match &n.event {
            Some(shared::protobuf::event::node_event::Event::MainchainBlock(b)) => {
                b.header.clone().unwrap()
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };
    // The official API showed the bid at the parent and committed it in the block.
    let mut bid = bmm_requests(3, 9);
    if let events::enforcer_event::Event::BmmRequests(snapshot) = &mut bid
        && let Some(request) = snapshot.requests.first_mut()
    {
        request.txid = txid.clone();
    }
    let mut committed = block_event(4, 4, 3);
    if let Some(MonitorEvent::Enforcer(payload)) = &mut committed.monitor_event
        && let Some(events::enforcer_event::Event::BlockConnected(b)) = &mut payload.event
    {
        b.bmm_commitment = Some(vec![0x33; 32]);
    }
    let parent = ObservedBlock::at_height(vec![3; 32], 3);
    let bid = envelope(bid, Some(parent.clone()), 1_700_000_000_003);
    let mut sample = SnapshotMetadata {
        started_at: SystemTime::now(),
        finished_at: SystemTime::now(),
        tip_before: parent.clone(),
        tip_after: parent,
        consistency: SnapshotConsistency::Unknown,
        attempts: 1,
        revision_before: None,
        revision_after: None,
    };
    // A sample taken while the enforcer may still be reloading its mempool.
    enforcer
        .record_snapshot(std::slice::from_ref(&bid), CaptureMethod::Poll, &sample)
        .await
        .unwrap();
    enforcer.record(&[committed]).await.unwrap();
    node.record(std::slice::from_ref(&block)).await.unwrap();
    let anchor = ObservedBlock::at_height(header.hash.clone(), header.height);
    let (source, next) = node.next_fee_block().await.unwrap().unwrap();
    assert_eq!(next, anchor);
    let make_fee = |txid: Vec<u8>, fee_sats| Event {
        timestamp: 1_700_000_000_005,
        observed_at_block: Some(anchor.clone()),
        monitor_event: Some(MonitorEvent::Node(shared::protobuf::event::NodeEvent {
            event: Some(
                shared::protobuf::event::node_event::Event::ConfirmedBmmFees(
                    events::ConfirmedBmmFees {
                        header: Some(header.clone()),
                        source: "ecash-node:getblock:3".into(),
                        fees: vec![events::ConfirmedBmmFee {
                            sidechain_number: 9,
                            txid,
                            fee_sats,
                            unavailable_reason: if fee_sats.is_some() {
                                String::new()
                            } else {
                                "historical_prevouts_unavailable".into()
                            },
                        }],
                    },
                ),
            ),
        })),
    };
    // A bid the official API never showed is not evidence.
    assert!(
        node.record_fee_enrichment(source, &make_fee(vec![8; 32], Some(42)))
            .await
            .is_err()
    );
    // A bid seen only in an unknown-consistency sample is not evidence either.
    assert!(
        node.record_fee_enrichment(source, &make_fee(txid.clone(), Some(42)))
            .await
            .is_err()
    );
    sample.consistency = SnapshotConsistency::TipMatched;
    enforcer
        .record_snapshot(std::slice::from_ref(&bid), CaptureMethod::Poll, &sample)
        .await
        .unwrap();
    // Enforcer payloads are not fee enrichments.
    assert!(
        node.record_fee_enrichment(source, &envelope(chain_info(), None, 1))
            .await
            .is_err()
    );
    node.record_fee_enrichment(source, &make_fee(txid.clone(), None))
        .await
        .unwrap();
    assert!(node.next_fee_block().await.unwrap().is_none());
    let client = query_client(test).await;
    client
        .execute(
            "UPDATE bmm_fee_job SET next_retry_at=now()-interval '1 second'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(node.next_fee_block().await.unwrap().unwrap().0, source);
    node.record_fee_enrichment(source, &make_fee(txid.clone(), Some(u64::MAX)))
        .await
        .unwrap();
    let counts = client.query_one("SELECT count(*) FILTER(WHERE kind='mainchain_block'),count(*) FILTER(WHERE kind='confirmed_bmm_fees') FROM event",&[]).await.unwrap();
    assert_eq!(counts.get::<_, i64>(0), 1);
    assert_eq!(counts.get::<_, i64>(1), 2);
    // Slot 98 has no official block fact yet, so its bids may still arrive.
    assert_eq!(fee_job(&client).await, ("pending".to_owned(), 2));
    let mut other_slot = block_event(4, 4, 3);
    if let Some(MonitorEvent::Enforcer(payload)) = &mut other_slot.monitor_event
        && let Some(events::enforcer_event::Event::BlockConnected(b)) = &mut payload.event
    {
        b.sidechain_number = 98;
    }
    enforcer.record(&[other_slot]).await.unwrap();
    client
        .execute(
            "UPDATE bmm_fee_job SET next_retry_at=now()-interval '1 second'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(node.next_fee_block().await.unwrap().unwrap().0, source);
    node.record_fee_enrichment(source, &make_fee(txid, Some(u64::MAX)))
        .await
        .unwrap();
    // Every active slot's block fact exists: the candidates are final.
    assert_eq!(fee_job(&client).await.0, "done");
    assert!(node.next_fee_block().await.unwrap().is_none());

    // A block that keeps failing is retried with backoff, then abandoned.
    client
        .execute(
            "UPDATE bmm_fee_job SET status='pending',attempts=0,next_retry_at=now()",
            &[],
        )
        .await
        .unwrap();
    for _ in 0..10 {
        node.record_fee_failure(source, "node RPC unavailable")
            .await
            .unwrap();
    }
    assert_eq!(fee_job(&client).await, ("abandoned".to_owned(), 10));
    assert!(node.next_fee_block().await.unwrap().is_none());
    // Only node recorders serve fee jobs.
    assert!(enforcer.next_fee_block().await.is_err());
}

#[tokio::test]
async fn a_stable_label_cannot_hide_a_revision_change() {
    let store = store_for("stable_revision", "enforcer").await;
    let anchor = ObservedBlock::at_height(vec![1; 32], 2);
    let mut metadata = SnapshotMetadata {
        started_at: SystemTime::now(),
        finished_at: SystemTime::now(),
        tip_before: anchor.clone(),
        tip_after: anchor.clone(),
        consistency: SnapshotConsistency::Stable,
        attempts: 1,
        revision_before: Some("test:1".into()),
        revision_after: Some("test:3".into()),
    };
    let event = envelope(ctip(9, 42), Some(anchor), 1_700_000_000_000);
    assert!(
        store
            .record_snapshot(std::slice::from_ref(&event), CaptureMethod::Poll, &metadata)
            .await
            .is_err()
    );
    metadata.consistency = SnapshotConsistency::Changed;
    store
        .record_snapshot(std::slice::from_ref(&event), CaptureMethod::Poll, &metadata)
        .await
        .unwrap();
    let client = query_client("stable_revision").await;
    let count: i64 = client
        .query_one("SELECT count(*) FROM state_snapshot_tip_matched", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    metadata.consistency = SnapshotConsistency::TipMatched;
    metadata.revision_before = None;
    metadata.revision_after = None;
    store
        .record_snapshot(&[event], CaptureMethod::Poll, &metadata)
        .await
        .unwrap();
    let count: i64 = client
        .query_one("SELECT count(*) FROM state_snapshot_tip_matched", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}

// Storage tests exercise identity and chain proof; node RPC tests validate raw bytes.
fn node_block_event(height: u32, hash: u8, parent: u8) -> Event {
    let mut event = block_event(height, hash, parent);
    let Some(MonitorEvent::Enforcer(payload)) = event.monitor_event.take() else {
        unreachable!()
    };
    let Some(events::enforcer_event::Event::BlockConnected(block)) = payload.event else {
        unreachable!()
    };
    event.monitor_event = Some(MonitorEvent::Node(shared::protobuf::event::NodeEvent {
        event: Some(shared::protobuf::event::node_event::Event::MainchainBlock(
            shared::protobuf::event::MainchainBlock {
                header: block.header,
                raw_block: vec![0],
            },
        )),
    }));
    event
}
#[tokio::test]
async fn node_reorg_preserves_proof_and_requires_certified_prefix() {
    let store = store_for("node_certified_reorg", "node").await;
    let old_tip = ObservedBlock::at_height(vec![104; 32], 104);
    store
        .begin_history_cycle(
            "mainchain_block",
            None,
            None,
            101,
            None,
            &old_tip,
            None,
            Some(100),
            128,
        )
        .await
        .unwrap();
    let events = (101..=104)
        .map(|h| node_block_event(h, h as u8, h as u8 - 1))
        .collect::<Vec<_>>();
    store
        .record_history_page(
            &events,
            HistoryPage {
                stream: "mainchain_block",
                sidechain: None,
                sidechain_instance_id: None,
                expected_next: &old_tip,
                next: None,
            },
        )
        .await
        .unwrap();
    // A live fact with no proven prefix must never serve as a repair floor.
    store
        .record(&[node_block_event(200, 200, 199)])
        .await
        .unwrap();
    let fork = ObservedBlock::at_height(vec![170; 32], 104);
    let progress = store
        .begin_history_cycle(
            "mainchain_block",
            None,
            None,
            101,
            None,
            &fork,
            None,
            Some(100),
            128,
        )
        .await
        .unwrap();
    assert_eq!(progress.covered_tip, Some(old_tip));
    assert_eq!(progress.status, HistoryStatus::Running);
    let floor = store
        .certified_history_floor(
            "mainchain_block",
            None,
            None,
            &[vec![200; 32], vec![170; 32], vec![103; 32]],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(floor.height, Some(103));
    store
        .record_history_page(
            &[node_block_event(104, 170, 103)],
            HistoryPage {
                stream: "mainchain_block",
                sidechain: None,
                sidechain_instance_id: None,
                expected_next: &fork,
                next: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .history_coverage("mainchain_block", None, None)
            .await
            .unwrap()
            .unwrap()
            .covered_tip,
        Some(fork)
    );
    let client = query_client("node_certified_reorg").await;
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM history_certified_block", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        5
    );
}

#[tokio::test]
async fn node_raw_content_conflict_survives_failed_certification() {
    let store = store_for("node_durable_conflict", "node").await;
    let target = ObservedBlock::at_height(vec![104; 32], 104);
    let first = node_block_event(104, 104, 103);
    store.record(std::slice::from_ref(&first)).await.unwrap();
    let mut conflicting = first;
    if let Some(MonitorEvent::Node(payload)) = &mut conflicting.monitor_event
        && let Some(shared::protobuf::event::node_event::Event::MainchainBlock(block)) =
            &mut payload.event
    {
        block.raw_block.push(1);
    }
    store
        .begin_history_cycle(
            "mainchain_block",
            None,
            None,
            104,
            None,
            &target,
            None,
            Some(103),
            128,
        )
        .await
        .unwrap();
    let error = store
        .record_history_page(
            &[conflicting],
            HistoryPage {
                stream: "mainchain_block",
                sidechain: None,
                sidechain_instance_id: None,
                expected_next: &target,
                next: None,
            },
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("conflicting immutable"));
    let client = query_client("node_durable_conflict").await;
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM event WHERE kind='mainchain_block'",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM event_conflict", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        store
            .history_coverage("mainchain_block", None, None)
            .await
            .unwrap()
            .unwrap()
            .status,
        HistoryStatus::Error
    );
}

fn numbered_hash(height: u32) -> Vec<u8> {
    let mut hash = vec![0xab; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    hash
}

fn numbered_node_block(height: u32) -> Event {
    Event {
        timestamp: 1_700_000_000_000 + u64::from(height),
        observed_at_block: Some(ObservedBlock::at_height(numbered_hash(height), height)),
        monitor_event: Some(MonitorEvent::Node(shared::protobuf::event::NodeEvent {
            event: Some(shared::protobuf::event::node_event::Event::MainchainBlock(
                shared::protobuf::event::MainchainBlock {
                    header: Some(events::BlockHeader {
                        hash: numbered_hash(height),
                        previous_hash: numbered_hash(height - 1),
                        height,
                        block_work: vec![1; 32],
                        cumulative_work: vec![0x44; 32],
                        timestamp: 1_750_000_000,
                    }),
                    raw_block: vec![0],
                },
            )),
        })),
    }
}

/// Wall time of the page that completes, and therefore certifies, `blocks`.
async fn certification_time(test: &str, blocks: u32) -> Duration {
    let store = store_for(test, "node").await;
    let tip = ObservedBlock::at_height(numbered_hash(blocks), blocks);
    store
        .begin_history_cycle(
            "mainchain_block",
            None,
            None,
            1,
            None,
            &tip,
            None,
            None,
            1_000,
        )
        .await
        .unwrap();
    let mut high = blocks;
    loop {
        // The final page is block 1 alone, so its time is the certification.
        let low = if high > 1 {
            high.saturating_sub(999).max(2)
        } else {
            1
        };
        let events = (low..=high).map(numbered_node_block).collect::<Vec<_>>();
        let expected = ObservedBlock::at_height(numbered_hash(high), high);
        let next = (low > 1).then(|| ObservedBlock::at_height(numbered_hash(low - 1), low - 1));
        let started = std::time::Instant::now();
        store
            .record_history_page(
                &events,
                HistoryPage {
                    stream: "mainchain_block",
                    sidechain: None,
                    sidechain_instance_id: None,
                    expected_next: &expected,
                    next: next.as_ref(),
                },
            )
            .await
            .unwrap();
        if next.is_none() {
            return started.elapsed();
        }
        high = low - 1;
    }
}

/// Certification walks a chain once; it must not rescan the proof per block.
/// Timing-based, so it is run on demand:
/// `cargo test -p shared --features postgres_integration_tests -- --ignored certification_scales`
#[tokio::test]
#[ignore = "timing measurement; run explicitly"]
async fn certification_scales_linearly_with_history_length() {
    let small = certification_time("certify_scale_2k", 2_000).await;
    let large = certification_time("certify_scale_8k", 8_000).await;
    eprintln!("certification: 2k blocks {small:?}, 8k blocks {large:?}");
    // Linear growth is ~4x; quadratic growth would be ~16x.
    assert!(
        large < small * 8,
        "certifying 4x more history took {large:?} vs {small:?}"
    );
}

fn numbered_enforcer_block(height: u32) -> Event {
    let mut payload = connected(9, height, 0);
    if let events::enforcer_event::Event::BlockConnected(block) = &mut payload {
        let header = block.header.as_mut().unwrap();
        header.hash = numbered_hash(height);
        header.previous_hash = numbered_hash(height - 1);
    }
    envelope(
        payload,
        Some(ObservedBlock::at_height(numbered_hash(height), height)),
        1_700_000_000_000 + u64::from(height),
    )
}

/// A consumer paging by `id > cursor` must never skip a row that commits
/// later than a higher id: the enforcer and node connections commit in id order.
#[tokio::test]
async fn concurrent_sources_commit_in_identity_order() {
    let test = "commit_order";
    let enforcer = store_for(test, "enforcer").await;
    let node = store_in(test, "node").await;
    let reader = query_client(test).await;
    let writes = 150_u32;
    let enforcer_task = tokio::spawn(async move {
        for height in 1..=writes {
            enforcer
                .record(&[numbered_enforcer_block(height)])
                .await
                .unwrap();
        }
    });
    let node_task = tokio::spawn(async move {
        for height in 1..=writes {
            node.record(&[numbered_node_block(height)]).await.unwrap();
        }
    });
    let mut cursor = 0_i64;
    let mut seen = std::collections::BTreeSet::new();
    loop {
        let finished = enforcer_task.is_finished() && node_task.is_finished();
        for row in reader
            .query("SELECT id FROM event WHERE id > $1 ORDER BY id", &[&cursor])
            .await
            .unwrap()
        {
            let id: i64 = row.get(0);
            seen.insert(id);
            cursor = cursor.max(id);
        }
        if finished {
            break;
        }
    }
    enforcer_task.await.unwrap();
    node_task.await.unwrap();
    let all = reader
        .query("SELECT id FROM event ORDER BY id", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<std::collections::BTreeSet<_>>();
    let skipped = all.difference(&seen).collect::<Vec<_>>();
    assert!(
        skipped.is_empty(),
        "rows committed behind the cursor: {skipped:?}"
    );
}
