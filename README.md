# Aeron RPC

Aeron RPC is a lightweight, high-performance RPC framework built on top of [Aeron](https://github.com/real-logic/aeron) for Rust. It provides simple abstractions for building request/response and streaming RPC services using Aeron publications and subscriptions.

## Features

- Asynchronous request/response and streaming RPC
- Builder patterns for both server and client
- Axum-like easy handler registration with flexible argument support

## Example Usage

### Prerequisites

- Install [Aeron Media Driver](https://aeron.io/docs/cookbook-content/build-ubuntu-cpp-driver/)

### Server

```rust
use aeron_rpc::{
    aeron::AeronBuilder,
    server::{RespSender, RpcServerBuilder},
};
use tokio::runtime::Runtime;

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .filter_module("aeron_rs", log::LevelFilter::Info)
        .init();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Build Aeron
        let mut aeron = AeronBuilder::new()
            .dir("/dev/shm/aeron-qyuzh")
            .build()
            .unwrap();

        let publication = aeron
            .add_publication("aeron:udp?endpoint=localhost:40123", 1001)
            .unwrap();

        log::info!(
            "Server publication session_id: {:?}",
            publication.lock().unwrap().session_id()
        );

        let subscription = aeron
            .add_subscription("aeron:udp?endpoint=localhost:40123", 1002)
            .unwrap();

        let mut server = RpcServerBuilder::new()
            .add_publication(publication)
            .add_subscription(subscription)
            .add_handler(&aeron_rpc::Interface::Ping, handle_ping)
            .add_handler(&aeron_rpc::Interface::Echo, handle_echo)
            .add_handler(&aeron_rpc::Interface::Stream, handle_stream)
            .build()
            .unwrap();

        // Run the server
        server.run().unwrap();

        log::info!("Server is running...");

        // Keep the server running
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    });
}

async fn handle_ping(req: String) -> Result<&'static str, String> {
    log::info!("Server received: {:?}", req);
    Ok("hello client, Ping")
}

async fn handle_echo(req: String) -> Result<String, String> {
    log::info!("Server received: {:?}", req);
    Ok(req)
}

async fn handle_stream(req: String, tx: RespSender) -> Result<(), String> {
    log::info!("Server received: {:?}", req);

    tokio::spawn(async move {
        for i in 0..10 {
            let data = format!("hello client, Stream {}", i);

            log::info!("Server sending: {:?}", data);

            tx.send(data).await.unwrap();

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });
    Ok(())
}
```

### Client

```rust
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
```

## License

MIT
