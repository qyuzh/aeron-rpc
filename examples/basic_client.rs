use aeron_rpc::{aeron::AeronBuilder, client::RpcClientBuilder};
use std::time::Duration;
use tokio::runtime::Runtime;

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .filter_module("aeron_rs", log::LevelFilter::Info)
        .init();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Build Aeron
        let mut aeron = AeronBuilder::new()
            .dir("/dev/shm/aeron-qyuzh")
            .build()
            .unwrap();

        // Add publication and subscription
        let publication = aeron
            .add_publication("aeron:udp?endpoint=localhost:40123", 1002)
            .unwrap();

        let subscription = aeron
            .add_subscription("aeron:udp?endpoint=localhost:40123", 1001)
            .unwrap();

        let client = RpcClientBuilder::new()
            .add_publication(publication)
            .add_subscription(subscription)
            .build()
            .unwrap();

        log::info!("Client is running...");

        // Send request and receive response
        let response: Vec<u8> = client
            .send_and_receive(
                &aeron_rpc::Interface::Ping,
                b"hello server, Ping",
                Duration::from_secs(10),
            )
            .await
            .expect("Failed to send/receive");

        log::info!("Received: {}", String::from_utf8_lossy(&response));

        let response: Vec<u8> = client
            .send_and_receive(
                &aeron_rpc::Interface::Echo,
                b"hello server, Echo",
                Duration::from_secs(10),
            )
            .await
            .expect("Failed to send/receive");

        log::info!("Received: {}", String::from_utf8_lossy(&response));

        let mut resp = client
            .send_and_receive_stream::<String>(
                &aeron_rpc::Interface::Stream,
                b"hello server, Stream",
            )
            .expect("Failed to send/receive");

        while let Some(data) = resp.next().await {
            match data {
                Ok(data) => log::info!("Received: {}", data),
                Err(e) => {
                    log::info!("Error: {}", e);
                    break;
                }
            }
        }
        log::info!("Stream finished");
    });
}
