use std::{
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};

use tokio::sync::mpsc;

use crate::{
    FromBytes, ToBusinessId,
    protocol::{Request, Response, Status},
};

pub struct RpcClient {
    sender: mpsc::Sender<(Request, Option<mpsc::Sender<Response>>)>,
    pub request_id: Arc<AtomicU32>,
}

impl RpcClient {
    pub fn new(sender: mpsc::Sender<(Request, Option<mpsc::Sender<Response>>)>) -> Self {
        Self {
            sender,
            request_id: Arc::new(AtomicU32::new(1)),
        }
    }

    fn fetch_request_id(&self) -> u64 {
        self.request_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u64
    }

    pub fn send(
        &self,
        business_id: &impl ToBusinessId,
        data: impl Into<Vec<u8>>,
    ) -> Result<(), String> {
        let request_id = self.fetch_request_id();
        let req = Request::new(request_id, business_id.to_business_id(), data.into());
        self.sender
            .blocking_send((req, None))
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn send_and_receive<T>(
        &self,
        business_id: &impl ToBusinessId,
        data: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> Result<T, String>
    where
        T: FromBytes,
    {
        let request_id = self.fetch_request_id();
        let req = Request::new(request_id, business_id.to_business_id(), data.into());

        let (tx, mut rx) = tokio::sync::mpsc::channel(1 << 10);

        self.sender
            .send((req, Some(tx)))
            .await
            .map_err(|e| e.to_string())?;

        tokio::select! {
            _ = tokio::time::sleep(timeout) => {
                Err("timeout".to_string())
            }
            result = rx.recv() => {
                if result.is_none() {
                    Err("failed to receive response".to_string())
                } else {
                    let resp = result.unwrap();
                    if resp.status != Status::Ok {
                        return Err(format!("Error with {:?}, {}", resp.status, String::from_utf8_lossy(&resp.data)));
                    }
                    T::from_bytes(resp.data)
                }
            }
        }
    }

    pub async fn send_and_receive_stream<T>(
        &self,
        business_id: &impl ToBusinessId,
        data: impl Into<Vec<u8>>,
    ) -> Result<Stream<T>, String> {
        let request_id = self.fetch_request_id();
        let req = Request::new(request_id, business_id.to_business_id(), data.into());

        let (tx, rx) = tokio::sync::mpsc::channel(1 << 10);

        self.sender
            .send((req, Some(tx)))
            .await
            .map_err(|e| e.to_string())?;

        Ok(Stream::new(rx))
    }
}

pub struct Stream<T> {
    rx: tokio::sync::mpsc::Receiver<Response>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(rx: tokio::sync::mpsc::Receiver<Response>) -> Self {
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
    pub async fn next(&mut self) -> Option<Result<T, String>> {
        self.rx.recv().await.map(|r: Response| {
            log::trace!("Received response: {:?}", r);
            if r.status != Status::Ok {
                Err(format!(
                    "Error with {:?}, {}",
                    r.status,
                    String::from_utf8_lossy(&r.data)
                ))
            } else {
                T::from_bytes(r.data)
            }
        })
    }
}

pub struct RpcClientBuilder {}

impl Default for RpcClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcClientBuilder {
    pub fn new() -> Self {
        Self {}
    }
}
