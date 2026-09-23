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
    CaptureMethod, DatasetManifest, HistoryPage, HistoryStatus, PostgresArgs, SidechainInstanceRef,
    Store, sidechain_instance_ref,
};

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
            chain_work: vec![0x44; 32],
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
    events::ActiveSidechain {
        sidechain_number,
        raw_description: vec![sidechain_number as u8; 32],
        vote_count: 4,
        proposal_height: 1,
        activation_height: 2,
        declaration: None,
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
            chain_work: vec![0x44; 32],
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
            sidechains: vec![events::ActiveSidechain {
                sidechain_number: 9,
                raw_description: vec![0xbb; 32],
                proposal_height: 3,
                activation_height: 4,
                ..Default::default()
            }],
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
async fn extractor_status_reports_and_clears_operational_errors() {
    let test = "extractor_status_error";
    let store = store_for(test, "enforcer").await;
    let client = query_client(test).await;

    store
        .record_extractor_error("tip RPC unavailable")
        .await
        .expect("record operational error");
    let recorded: Option<String> = client
        .query_one(
            "SELECT last_error FROM extractor_status WHERE source = 'enforcer'",
            &[],
        )
        .await
        .expect("query extractor error")
        .get(0);
    assert_eq!(recorded.as_deref(), Some("tip RPC unavailable"));

    store
        .clear_extractor_error()
        .await
        .expect("clear recovered error");
    let cleared: Option<String> = client
        .query_one(
            "SELECT last_error FROM extractor_status WHERE source = 'enforcer'",
            &[],
        )
        .await
        .expect("query recovered extractor status")
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
    let replacement = |raw_description: Vec<u8>, proposal_height, activation_height| {
        events::enforcer_event::Event::ActiveSidechains(events::ActiveSidechainsSnapshot {
            sidechains: vec![events::ActiveSidechain {
                sidechain_number: 9,
                raw_description,
                vote_count: 4,
                proposal_height,
                activation_height,
                declaration: None,
            }],
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
    assert_eq!(current, vec![9; 32]);
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
    let manifest = DatasetManifest {
        event_contract_version: upgraded_version,
        ..DatasetManifest::default()
    };
    let upgraded = Store::connect_with_manifest(
        &args_for(&admin_url, &format!("bip300_test_{test}")),
        "enforcer",
        manifest,
    )
    .await
    .expect("connect an upgraded converter run");
    assert_eq!(
        upgraded
            .record(&[fact])
            .await
            .expect("record upgraded fact"),
        1,
        "a new normalized contract must get its own immutable fact"
    );

    let client = query_client(test).await;
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

    let dataset_version: i32 = client
        .query_one(
            "SELECT initial_event_contract_version
               FROM dataset_manifest
              WHERE dataset_id = (
                    SELECT dataset_id FROM event
                     WHERE kind = 'chain_info' AND block_hash = $1
                     LIMIT 1
              )",
            &[&anchor.hash],
        )
        .await
        .expect("query immutable dataset metadata")
        .get(0);
    assert_eq!(dataset_version, i32::try_from(initial_version).unwrap());
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
}
