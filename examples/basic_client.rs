use aeron_rpc::{RpcContext, aeron::AeronBuilder};
use std::time::Duration;
use tokio::runtime::Runtime;

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .filter_module("aeron_rs", log::LevelFilter::Info)
        .format_file(true)
        .format_line_number(true)
        .format_target(false)
        .init();

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

    let mut rpc_context = RpcContext::new(publication, subscription);

    let client = rpc_context.get_rpc_client();

    rpc_context.run();

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Build Aeron

        log::info!("Client is running...");

        // Send request and receive response
        let response: Vec<u8> = client
            .send(
                &aeron_rpc::Interface::Ping,
                b"hello server, Ping",
                Duration::from_secs(20),
            )
            .await
            .expect("Failed to send/receive");

        log::info!("Received: {}", String::from_utf8_lossy(&response));

        let response: Vec<u8> = client
            .send(
                &aeron_rpc::Interface::Echo,
                b"hello server, Echo",
                Duration::from_secs(10),
            )
            .await
            .expect("Failed to send/receive");

        log::info!("Received: {}", String::from_utf8_lossy(&response));

        let mut resp = client
            .send_stream::<String>(&aeron_rpc::Interface::Stream, b"hello server, Stream")
            .await
            .expect("Failed to send/receive");

        while let Some(data) = resp.next().await {
            match data {
                Ok(data) => log::info!("Received: {}", data),
                Err(e) => {
                    log::info!("Error: {:?}", e);
                    break;
                }
            }
        }

        log::info!("Stream finished");
    });
}
