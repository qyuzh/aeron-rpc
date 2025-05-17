use std::time::Duration;

use crate::{FromBytes, ToBusinessId, publisher::Publisher, subscriber::Subscriber};

pub struct RpcClient {
    publication: Publisher,   // Aeron publication for sending messages
    subscription: Subscriber, // Aeron subscription for receiving messages
}

impl RpcClient {
    pub fn new(publication: Publisher, subscription: Subscriber) -> Self {
        let mut subscription = subscription;
        subscription.run();
        Self {
            publication,
            subscription,
        }
    }
}

impl RpcClient {
    pub fn send(
        &self,
        business_id: &impl ToBusinessId,
        data: impl Into<Vec<u8>>,
    ) -> Result<(), String> {
        let _ = self.publication.request(business_id, data);
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
        let request_id = self.publication.request(business_id, data)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(1 << 10);
        self.subscription.add_client_sender(request_id, tx);
        tokio::select! {
            _ = tokio::time::sleep(timeout) => {
                Err("timeout".to_string())
            }
            result = rx.recv() => {
                if result.is_none() {
                    Err("failed to receive response".to_string())
                } else {
                    let data = result.unwrap();
                    T::from_bytes(data)
                }
            }
        }
    }

    pub fn send_and_receive_stream<T>(
        &self,
        business_id: &impl ToBusinessId,
        data: impl Into<Vec<u8>>,
    ) -> Result<Stream<T>, String> {
        let request_id = self.publication.request(business_id, data)?;
        let (tx, rx) = tokio::sync::mpsc::channel(1 << 10);
        self.subscription.add_client_sender(request_id, tx);
        Ok(Stream::new(rx))
    }
}

pub struct Stream<T> {
    rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(rx: tokio::sync::mpsc::Receiver<Vec<u8>>) -> Self {
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
        self.rx.recv().await.map(T::from_bytes)
    }
}

pub struct RpcClientBuilder {
    publication: Option<Publisher>,
    subscription: Option<Subscriber>,
}

impl Default for RpcClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcClientBuilder {
    pub fn new() -> Self {
        Self {
            publication: None,
            subscription: None,
        }
    }

    pub fn add_publication(
        mut self,
        publication: std::sync::Arc<std::sync::Mutex<aeron_rs::publication::Publication>>,
    ) -> Self {
        self.publication = Some(Publisher::new(publication));
        self
    }

    pub fn add_subscription(
        mut self,
        subscription: std::sync::Arc<std::sync::Mutex<aeron_rs::subscription::Subscription>>,
    ) -> Self {
        self.subscription = Some(Subscriber::new(subscription));
        self
    }

    pub fn build(self) -> Result<RpcClient, String> {
        let publication = self.publication.ok_or("publication is required")?;
        let subscription = self.subscription.ok_or("subscription is required")?;
        Ok(RpcClient::new(publication, subscription))
    }
}
