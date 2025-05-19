use aeron_rpc::{RpcContext, aeron::AeronBuilder, server::RespSender};
use tokio::runtime::Runtime;

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .filter_module("aeron_rs", log::LevelFilter::Info)
        .format_file(true)
        .format_line_number(true)
        .format_target(false)
        .init();

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

    let mut rpc_context = RpcContext::new(publication, subscription);

    let mut server = rpc_context
        .add_handler(&aeron_rpc::Interface::Ping, handle_ping)
        .add_handler(&aeron_rpc::Interface::Echo, handle_echo)
        .add_handler(&aeron_rpc::Interface::Stream, handle_stream)
        .get_rpc_server();

    rpc_context.run();

    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        log::info!("Server is running...");
        server.run().await.unwrap();
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
