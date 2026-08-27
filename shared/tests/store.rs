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

use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::{Event, ObservedBlock, event::MonitorEvent};
use shared::store::{PostgresArgs, Store};

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

    Store::connect(&args_for(&admin_url, &database), source)
        .await
        .expect("connect and migrate the record")
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

fn sidechain_proposals() -> events::enforcer_event::Event {
    events::enforcer_event::Event::SidechainProposals(events::SidechainProposalsSnapshot {
        proposals: Vec::new(),
    })
}

fn active_sidechains() -> events::enforcer_event::Event {
    events::enforcer_event::Event::ActiveSidechains(events::ActiveSidechainsSnapshot {
        sidechains: Vec::new(),
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
            .last_recorded_height("ctip", 9)
            .await
            .expect("query the record"),
        None,
        "the valid event of a failed batch must not have been committed"
    );
}

#[tokio::test]
async fn the_checkpoint_reports_the_newest_recorded_block_per_slot() {
    let store = store_for("checkpoint", "enforcer").await;
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
            .last_recorded_height("block_connected", 9)
            .await
            .expect("query the checkpoint"),
        Some(996_262)
    );
    assert_eq!(
        store
            .last_recorded_height("block_connected", 98)
            .await
            .expect("query the checkpoint"),
        Some(996_999),
        "another slot must not move this slot's checkpoint"
    );
}

#[tokio::test]
async fn a_disconnect_without_a_height_does_not_move_the_checkpoint() {
    let store = store_for("disconnect_height", "enforcer").await;
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
            .last_recorded_height("block_disconnected", 9)
            .await
            .expect("query the checkpoint"),
        None,
        "an absent height must not be recorded as zero"
    );
}
