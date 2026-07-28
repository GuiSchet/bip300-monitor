#![cfg(feature = "nats_integration_tests")]

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures_util::StreamExt;
use prost::Message;
use shared::nats::{EventPublisher, NatsArgs};
use shared::nats_subjects::Subject;
use shared::protobuf::enforcer_extractor::{
    Bip300Constants, ChainInfo, EnforcerEvent, Network, enforcer_event,
};
use shared::protobuf::event::{Event, event::MonitorEvent};
use tokio::time::{sleep, timeout};

struct TestNatsServer {
    child: Child,
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
            child,
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
}

impl Drop for TestNatsServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn publishes_and_decodes_the_monitor_envelope() {
    let server = TestNatsServer::start().await;
    let subscriber = async_nats::connect(&server.address)
        .await
        .expect("subscriber connection");
    let mut subscription = subscriber
        .subscribe(Subject::Enforcer.to_string())
        .await
        .expect("enforcer subscription");
    subscriber.flush().await.expect("flush subscription");

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
        .publish(Subject::Enforcer, &expected)
        .await
        .expect("publish event");
    publisher.flush().await.expect("flush publisher");

    let message = timeout(Duration::from_secs(2), subscription.next())
        .await
        .expect("event received before timeout")
        .expect("subscription remains open");
    let received = Event::decode(message.payload).expect("valid monitor protobuf");

    assert_eq!(received, expected);
}
