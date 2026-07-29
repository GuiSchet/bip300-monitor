#![cfg(feature = "nats_integration_tests")]

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use shared::nats::{EventPublisher, EventSubscriber, NatsArgs};
use shared::nats_subjects::Subject;
use shared::protobuf::enforcer_extractor::{
    Bip300Constants, ChainInfo, EnforcerEvent, Network, enforcer_event,
};
use shared::protobuf::event::{Event, event::MonitorEvent};
use tokio::time::{sleep, timeout};

struct TestNatsServer {
    child: Option<Child>,
    address: String,
}

impl TestNatsServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a local port");
        let port = listener.local_addr().expect("reserved address").port();
        drop(listener);

        let binary =
            std::env::var("NATS_SERVER_BINARY").unwrap_or_else(|_| "nats-server".to_owned());
        let child = Command::new(&binary)
            .args(["--addr", "127.0.0.1", "--port", &port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|error| panic!("starting `{binary}`: {error}"));
        let server = Self {
            child: Some(child),
            address: format!("nats://127.0.0.1:{port}"),
        };

        timeout(Duration::from_secs(5), async {
            loop {
                if async_nats::connect(&server.address).await.is_ok() {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("NATS server ready within five seconds");

        server
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for TestNatsServer {
    fn drop(&mut self) {
        self.stop();
    }
}

#[tokio::test]
async fn publishes_and_decodes_the_monitor_envelope() {
    let server = TestNatsServer::start().await;
    let mut subscriber = EventSubscriber::connect(
        &NatsArgs {
            nats_url: server.address.clone(),
            ..NatsArgs::default()
        },
        Subject::Enforcer,
        "bip300-monitor-integration-test-subscriber",
    )
    .await
    .expect("event subscriber connection");

    let publisher = EventPublisher::connect(&NatsArgs {
        nats_url: server.address.clone(),
        ..NatsArgs::default()
    })
    .await
    .expect("publisher connection");
    let payload = EnforcerEvent {
        event: Some(enforcer_event::Event::ChainInfo(ChainInfo {
            network: Network::Regtest as i32,
            bip300_constants: Some(Bip300Constants {
                activation_height: 100,
                ..Bip300Constants::default()
            }),
        })),
    };
    let expected =
        Event::new(MonitorEvent::Enforcer(payload)).expect("system clock after Unix epoch");

    publisher
        .publish_and_flush(Subject::Enforcer, &expected)
        .await
        .expect("publish and flush event");

    let received = timeout(Duration::from_secs(2), subscriber.next_event())
        .await
        .expect("event received before timeout")
        .expect("valid monitor event");

    assert_eq!(received, expected);
    subscriber.close().await.expect("subscriber shutdown");
}

#[tokio::test]
async fn subscriber_rejects_an_invalid_protobuf_payload() {
    let server = TestNatsServer::start().await;
    let mut subscriber = EventSubscriber::connect(
        &NatsArgs {
            nats_url: server.address.clone(),
            ..NatsArgs::default()
        },
        Subject::Enforcer,
        "bip300-monitor-invalid-payload-test",
    )
    .await
    .expect("event subscriber connection");
    let publisher = async_nats::connect(&server.address)
        .await
        .expect("raw publisher connection");

    publisher
        .publish(Subject::Enforcer.to_string(), vec![0xff, 0x00].into())
        .await
        .expect("publish invalid payload");
    publisher.flush().await.expect("flush invalid payload");

    let error = timeout(Duration::from_secs(2), subscriber.next_event())
        .await
        .expect("payload received before timeout")
        .expect_err("invalid protobuf must fail");
    assert!(format!("{error:#}").contains("decoding event"));
}

#[tokio::test]
async fn detected_server_loss_makes_publish_and_flush_time_out() {
    let mut server = TestNatsServer::start().await;
    let publisher = EventPublisher::connect(&NatsArgs {
        nats_url: server.address.clone(),
        nats_flush_timeout_seconds: 1,
        ..NatsArgs::default()
    })
    .await
    .expect("publisher connection");
    let payload = EnforcerEvent {
        event: Some(enforcer_event::Event::ChainInfo(ChainInfo {
            network: Network::Regtest as i32,
            bip300_constants: None,
        })),
    };
    let event = Event::new(MonitorEvent::Enforcer(payload)).expect("system clock after Unix epoch");

    server.stop();
    sleep(Duration::from_millis(250)).await;
    let error = timeout(
        Duration::from_secs(2),
        publisher.publish_and_flush(Subject::Enforcer, &event),
    )
    .await
    .expect("publish and flush is bounded")
    .expect_err("server loss must make the flush fail");

    assert!(
        format!("{error:#}").contains("flushing the Core NATS connection"),
        "unexpected error: {error:#}"
    );
}
