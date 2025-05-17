use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use aeron_rs::publication::Publication;
use aeron_rs::subscription::Subscription;

use crate::ToBytes;
use crate::{ToBusinessId, publisher::Publisher, subscriber::Subscriber};

pub struct RpcServerBuilder {
    publisher: Option<Publisher>,
    subscriber: Option<Subscriber>,
    handlers: HashMap<u64, Arc<dyn Handler + Send + Sync>>,
}

impl Default for RpcServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcServerBuilder {
    pub fn new() -> Self {
        Self {
            publisher: None,
            subscriber: None,
            handlers: HashMap::new(),
        }
    }

    pub fn add_publication(mut self, publication: Arc<Mutex<Publication>>) -> Self {
        self.publisher = Some(Publisher::new(publication));
        self
    }

    pub fn add_subscription(mut self, publication: Arc<Mutex<Subscription>>) -> Self {
        self.subscriber = Some(Subscriber::new(publication));
        self
    }

    pub fn add_handler<F, Args, Res>(mut self, business_id: &impl ToBusinessId, func: F) -> Self
    where
        F: IntoHandlerWrapper<Args, Res> + 'static,
        HandlerWrapper<F, Args, Res>: Handler + 'static,
        Res: ToBytes + 'static,
    {
        let business_id = business_id.to_business_id();
        let wrapper = func.into_handler_wrapper();
        self.handlers.insert(business_id, Arc::new(wrapper));
        self
    }

    pub fn build(self) -> Result<RpcServer, String> {
        let publisher = self.publisher.ok_or("publisher is required")?;
        let subscriber = self.subscriber.ok_or("subscriber is required")?;
        if self.handlers.is_empty() {
            return Err("at least one handler is required".to_string());
        }
        Ok(RpcServer::new(
            publisher,
            subscriber,
            Arc::new(self.handlers),
        ))
    }
}

pub struct RpcServer {
    handlers: Arc<HashMap<u64, Arc<dyn Handler + Send + Sync>>>,
    subscriber: Subscriber,
    publisher: Publisher,
}

impl RpcServer {
    pub fn new(
        publisher: Publisher,
        subscriber: Subscriber,
        handlers: Arc<HashMap<u64, Arc<dyn Handler + Send + Sync>>>,
    ) -> Self {
        Self {
            handlers,
            subscriber,
            publisher,
        }
    }
}

impl RpcServer {
    pub fn run(&mut self) -> Result<(), String> {
        let (tx, rx) = tokio::sync::mpsc::channel(1 << 12);

        self.subscriber.add_server_sender(tx);
        self.subscriber.run();

        let handlers = self.handlers.clone();
        let publisher = self.publisher.clone();

        tokio::spawn(async move {
            let mut rx = rx;
            while let Some(payload) = rx.recv().await {
                if let Some(handler) = handlers.get(&payload.business_id).cloned() {
                    let publisher = publisher.clone();
                    tokio::spawn(async move {
                        let (tx, rc) = tokio::sync::mpsc::channel(1 << 10);
                        let mut ctx = Context {
                            sender: Some(tx),
                            request_id: payload.request_id,
                            session_id: 0,
                            data: payload.data.clone(),
                        };

                        let t = handler.handle(&mut ctx).await;

                        if ctx.sender.is_none() {
                            let mut rx = rc;

                            let mut last_data = None;

                            while let Some(data) = rx.recv().await {
                                if let Some(last_data) = last_data {
                                    publisher.response(payload.request_id, false, last_data);
                                }
                                last_data = Some(data);
                            }

                            if let Some(last_data) = last_data {
                                publisher.response(payload.request_id, true, last_data);
                            }
                        } else {
                            match t {
                                Ok(data) => publisher.response(payload.request_id, true, data),
                                Err(t) => {
                                    publisher.response(payload.request_id, false, t.to_bytes())
                                }
                            }
                        }
                    });
                }
            }

            log::info!("server thread exit");
        });
        Ok(())
    }
}

pub struct HandlerWrapper<F, Req, Res> {
    func: F,
    _phantom: std::marker::PhantomData<(Req, Res)>,
}

impl<F, Req, Res> HandlerWrapper<F, Req, Res> {
    pub fn new(func: F) -> Self {
        HandlerWrapper {
            func,
            _phantom: std::marker::PhantomData,
        }
    }
}

pub struct Context {
    sender: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
    pub request_id: u64,
    pub session_id: u64,
    pub data: Vec<u8>,
}

pub trait FromContext {
    fn from_context(ctx: &mut Context) -> Self;
}

#[async_trait::async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, ctx: &mut Context) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl<F, Fut, F1, Res> Handler for HandlerWrapper<F, F1, Res>
where
    F: Fn(F1) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Res, String>> + Send + 'static,
    F1: FromContext + Send + Sync + 'static,
    Res: ToBytes + Send + Sync + 'static,
{
    async fn handle(&self, ctx: &mut Context) -> Result<Vec<u8>, String> {
        let req1 = F1::from_context(ctx);
        let res = (self.func)(req1).await?;
        Ok(res.to_bytes())
    }
}

#[async_trait::async_trait]
impl<F, Fut, F1, F2, Res> Handler for HandlerWrapper<F, (F1, F2), Res>
where
    F: Fn(F1, F2) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Res, String>> + Send + 'static,
    F1: FromContext + Send + Sync + 'static,
    F2: FromContext + Send + Sync + 'static,
    Res: ToBytes + Send + Sync + 'static,
{
    async fn handle(&self, ctx: &mut Context) -> Result<Vec<u8>, String> {
        let req1 = F1::from_context(ctx);
        let req2 = F2::from_context(ctx);
        let res = (self.func)(req1, req2).await?;
        Ok(res.to_bytes())
    }
}

#[async_trait::async_trait]
impl<F, Fut, F1, F2, F3, Res> Handler for HandlerWrapper<F, (F1, F2, F3), Res>
where
    F: Fn(F1, F2, F3) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Res, String>> + Send + 'static,
    F1: FromContext + Send + Sync + 'static,
    F2: FromContext + Send + Sync + 'static,
    F3: FromContext + Send + Sync + 'static,
    Res: ToBytes + Send + Sync + 'static,
{
    async fn handle(&self, ctx: &mut Context) -> Result<Vec<u8>, String> {
        let req1 = F1::from_context(ctx);
        let req2 = F2::from_context(ctx);
        let req3 = F3::from_context(ctx);
        let res = (self.func)(req1, req2, req3).await?;
        Ok(res.to_bytes())
    }
}

/// Helper trait for converting a function into a HandlerWrapper.
pub trait IntoHandlerWrapper<F1, Res>: Sized {
    fn into_handler_wrapper(self) -> HandlerWrapper<Self, F1, Res>;
}

impl<F, Fut, F1, F2, Res> IntoHandlerWrapper<(F1, F2), Res> for F
where
    F: Fn(F1, F2) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Res, String>> + Send + 'static,
    F1: FromContext + Send + Sync + 'static,
    F2: FromContext + Send + Sync + 'static,
    Res: ToBytes + Send + Sync + 'static,
{
    fn into_handler_wrapper(self) -> HandlerWrapper<Self, (F1, F2), Res> {
        HandlerWrapper::new(self)
    }
}

impl<F, Fut, F1, Res> IntoHandlerWrapper<F1, Res> for F
where
    F: Fn(F1) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Res, String>> + Send + 'static,
    F1: FromContext + Send + Sync + 'static,
    Res: ToBytes + Send + Sync + 'static,
{
    fn into_handler_wrapper(self) -> HandlerWrapper<Self, F1, Res> {
        HandlerWrapper::new(self)
    }
}

/// The result of a handler will be ignored if using RespSender.
/// Direct return data from the handler if just one data needs to be sent.
pub struct RespSender(tokio::sync::mpsc::Sender<Vec<u8>>);

impl RespSender {
    pub fn blocking_send(&self, data: impl ToBytes) -> Result<(), String> {
        self.0
            .blocking_send(data.to_bytes())
            .map_err(|_| "failed to send".to_string())
    }

    pub async fn send(&self, data: impl ToBytes) -> Result<(), String> {
        self.0
            .send(data.to_bytes())
            .await
            .map_err(|_| "failed to send".to_string())
    }

    pub fn try_send(&self, data: impl ToBytes) -> Result<(), String> {
        self.0
            .try_send(data.to_bytes())
            .map_err(|_| "failed to send".to_string())
    }
}

impl FromContext for RespSender {
    fn from_context(ctx: &mut Context) -> Self {
        let sender = ctx.sender.take().unwrap();
        Self(sender)
    }
}

impl FromContext for String {
    fn from_context(ctx: &mut Context) -> Self {
        String::from_utf8(ctx.data.clone()).unwrap()
    }
}

impl FromContext for Vec<u8> {
    fn from_context(ctx: &mut Context) -> Self {
        ctx.data.clone()
    }
}
