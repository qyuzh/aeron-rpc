use std::{
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};

use tokio::sync::oneshot;

use crate::{
    FromBytes, ToBusinessId,
    err::{ReceiveError, SendError},
    protocol::{Client2MultiplexerSender, Request, Response, SendPacket},
};

pub struct RpcClient {
    sender: Client2MultiplexerSender,
    pub request_id: Arc<AtomicU32>,
}

impl RpcClient {
    pub fn new(sender: Client2MultiplexerSender) -> Self {
        Self {
            sender,
            request_id: Arc::new(AtomicU32::new(1)),
        }
    }

    fn fetch_request_id(&self) -> u64 {
        self.request_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u64
    }

    /// Send a request without waiting for a response.
    pub async fn report(
        &self,
        business_id: &impl ToBusinessId,
        data: impl Into<Vec<u8>>,
    ) -> Result<(), SendError> {
        let request_id = self.fetch_request_id();
        let req = Request::new(request_id, business_id.to_business_id(), data.into());
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(SendPacket {
                request: req,
                resp_sender: None,
                timeout: Duration::from_secs(60),
                send_signal: tx,
            })
            .await
            .expect("Channel closed unexpectedly");

        rx.await.expect("Channel closed unexpectedly")?;

        Ok(())
    }

    /// Send a request and wait for a response.
    pub async fn send<T>(
        &self,
        business_id: &impl ToBusinessId,
        data: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> Result<T, SendError>
    where
        T: FromBytes,
    {
        let request_id = self.fetch_request_id();
        let req = Request::new(request_id, business_id.to_business_id(), data.into());

        let (tx, mut rx) = tokio::sync::mpsc::channel(1 << 10);

        let (signal_tx, signal_rx) = oneshot::channel();
        self.sender
            .send(SendPacket {
                request: req,
                resp_sender: Some(tx),
                timeout,
                send_signal: signal_tx,
            })
            .await
            .expect("Channel closed unexpectedly");

        signal_rx.await.expect("Channel closed unexpectedly")?;

        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(resp) => {
                let resp = resp.expect("Channel closed unexpectedly");
                match resp {
                    Ok(r) => Ok(T::from_bytes(r.data)?),
                    Err(_) => Err(SendError::Timeout),
                }
            }
            Err(_) => Err(SendError::Timeout),
        }
    }

    /// Send a request and receive a stream of responses.
    pub async fn send_stream<T>(
        &self,
        business_id: &impl ToBusinessId,
        data: impl Into<Vec<u8>>,
    ) -> Result<Stream<T>, SendError> {
        let request_id = self.fetch_request_id();
        let req = Request::new(request_id, business_id.to_business_id(), data.into());

        let (tx, rx) = tokio::sync::mpsc::channel(1 << 10);

        let (signal_tx, signal_rx) = oneshot::channel();

        self.sender
            .send(SendPacket {
                request: req,
                timeout: Duration::from_secs(60),
                resp_sender: Some(tx),
                send_signal: signal_tx,
            })
            .await
            .expect("Channel closed unexpectedly");

        signal_rx.await.expect("Channel closed unexpectedly")?;

        Ok(Stream::new(rx))
    }
}

pub struct Stream<T> {
    rx: tokio::sync::mpsc::Receiver<Result<Response, ReceiveError>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(rx: tokio::sync::mpsc::Receiver<Result<Response, ReceiveError>>) -> Self {
        Self {
            rx,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> Stream<T>
where
    T: FromBytes,
{
    pub async fn next(&mut self) -> Option<Result<T, ReceiveError>> {
        self.rx.recv().await.map(|r| {
            log::trace!("Received response: {:?}", r);
            Ok(T::from_bytes(r?.data)?)
        })
    }
}
