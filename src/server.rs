use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::ToBusinessId;
use crate::ToBytes;
use crate::protocol::{Request, Response, Status};

pub struct RpcServerBuilder {
    receiver: Option<tokio::sync::mpsc::Receiver<Request>>,
    sender: Option<tokio::sync::mpsc::Sender<Response>>,
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
            receiver: None,
            sender: None,
            handlers: HashMap::new(),
        }
    }

    pub fn add_receiver(mut self, receiver: tokio::sync::mpsc::Receiver<Request>) -> Self {
        self.receiver = Some(receiver);
        self
    }

    pub fn add_sender(mut self, sender: tokio::sync::mpsc::Sender<Response>) -> Self {
        self.sender = Some(sender);
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
        let receiver = self.receiver.ok_or("receiver is required")?;
        let sender = self.sender.ok_or("Sender is required")?;
        if self.handlers.is_empty() {
            return Err("at least one handler is required".to_string());
        }
        Ok(RpcServer::new(Arc::new(self.handlers), receiver, sender))
    }
}

pub struct RpcServer {
    handlers: Arc<HashMap<u64, Arc<dyn Handler + Send + Sync>>>,
    receiver: Option<tokio::sync::mpsc::Receiver<Request>>,
    sender: Option<tokio::sync::mpsc::Sender<Response>>,
}

impl RpcServer {
    pub fn new(
        handlers: Arc<HashMap<u64, Arc<dyn Handler + Send + Sync>>>,
        receiver: tokio::sync::mpsc::Receiver<Request>,
        sender: tokio::sync::mpsc::Sender<Response>,
    ) -> Self {
        Self {
            handlers,
            receiver: Some(receiver),
            sender: Some(sender),
        }
    }
}

impl RpcServer {
    pub async fn run(&mut self) -> Result<(), String> {
        let receiver = self.receiver.take();
        let handlers = self.handlers.clone();
        let sender = self.sender.take().expect("sender is required");

        tokio::spawn(async move {
            let mut rx = receiver.expect("receiver is required");

            while let Some(req) = rx.recv().await {
                if let Some(handler) = handlers.get(&req.business_id).cloned() {
                    let sender = sender.clone();
                    tokio::spawn(async move {
                        let (tx, rc) = tokio::sync::mpsc::channel(1 << 10);
                        let mut ctx = Context {
                            sender: Some(tx),
                            request_id: req.request_id,
                            session_id: 0,
                            data: req.data.clone(),
                        };

                        let t = handler.handle(&mut ctx).await;

                        if ctx.sender.is_none() {
                            let mut rx = rc;

                            let mut last_data = None;

                            while let Some(data) = rx.recv().await {
                                if let Some(last_data) = last_data {
                                    sender
                                        .send(Response::new(
                                            req.request_id,
                                            false,
                                            Status::Ok,
                                            last_data,
                                        ))
                                        .await
                                        .unwrap();
                                }
                                last_data = Some(data);
                            }

                            if let Some(last_data) = last_data {
                                sender
                                    .send(Response::new(
                                        req.request_id,
                                        true,
                                        Status::Ok,
                                        last_data,
                                    ))
                                    .await
                                    .unwrap();
                            }
                        } else {
                            match t {
                                Ok(data) => {
                                    sender
                                        .send(Response::new(req.request_id, true, Status::Ok, data))
                                        .await
                                        .unwrap();
                                }
                                Err(t) => {
                                    log::error!("handler error: {}", t);
                                    sender
                                        .send(Response::new(
                                            req.request_id,
                                            true,
                                            Status::Other,
                                            t.to_bytes(),
                                        ))
                                        .await
                                        .unwrap();
                                }
                            }
                        }
                    });
                }
            }

            log::info!("server thread exit");
        })
        .await
        .map_err(|e| e.to_string())?;
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

pub enum FromContextError {
    ParseError(String),
    Other(String),
}

impl std::fmt::Display for FromContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FromContextError::ParseError(e) => write!(f, "Parse error: {}", e),
            FromContextError::Other(e) => write!(f, "Other error: {}", e),
        }
    }
}

pub trait FromContext {
    fn from_context(ctx: &mut Context) -> Result<Self, FromContextError>
    where
        Self: Sized;
}

#[async_trait::async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, ctx: &mut Context) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl<F, Fut, F1, Res> Handler for HandlerWrapper<F, F1, Res>
where
    F: Fn(F1) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Res> + Send + 'static,
    F1: FromContext + Send + Sync + 'static,
    Res: ToBytes + Send + Sync + 'static,
{
    async fn handle(&self, ctx: &mut Context) -> Result<Vec<u8>, String> {
        let req1 = F1::from_context(ctx).map_err(|e| e.to_string())?;
        let res = (self.func)(req1).await;
        Ok(res.to_bytes())
    }
}

#[async_trait::async_trait]
impl<F, Fut, F1, F2, Res> Handler for HandlerWrapper<F, (F1, F2), Res>
where
    F: Fn(F1, F2) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Res> + Send + 'static,
    F1: FromContext + Send + Sync + 'static,
    F2: FromContext + Send + Sync + 'static,
    Res: ToBytes + Send + Sync + 'static,
{
    async fn handle(&self, ctx: &mut Context) -> Result<Vec<u8>, String> {
        let req1 = F1::from_context(ctx).map_err(|e| e.to_string())?;
        let req2 = F2::from_context(ctx).map_err(|e| e.to_string())?;
        let res = (self.func)(req1, req2).await;
        Ok(res.to_bytes())
    }
}

#[async_trait::async_trait]
impl<F, Fut, F1, F2, F3, Res> Handler for HandlerWrapper<F, (F1, F2, F3), Res>
where
    F: Fn(F1, F2, F3) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Res> + Send + 'static,
    F1: FromContext + Send + Sync + 'static,
    F2: FromContext + Send + Sync + 'static,
    F3: FromContext + Send + Sync + 'static,
    Res: ToBytes + Send + Sync + 'static,
{
    async fn handle(&self, ctx: &mut Context) -> Result<Vec<u8>, String> {
        let req1 = F1::from_context(ctx).map_err(|e| e.to_string())?;
        let req2 = F2::from_context(ctx).map_err(|e| e.to_string())?;
        let req3 = F3::from_context(ctx).map_err(|e| e.to_string())?;
        let res = (self.func)(req1, req2, req3).await;
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
    Fut: Future<Output = Res> + Send + 'static,
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
    Fut: Future<Output = Res> + Send + 'static,
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
    fn from_context(ctx: &mut Context) -> Result<Self, FromContextError> {
        let sender = ctx
            .sender
            .take()
            .ok_or_else(|| FromContextError::Other("sender is not available".to_string()))?;
        Ok(Self(sender))
    }
}

impl FromContext for String {
    fn from_context(ctx: &mut Context) -> Result<Self, FromContextError> {
        String::from_utf8(ctx.data.clone()).map_err(|e| FromContextError::ParseError(e.to_string()))
    }
}

impl FromContext for Vec<u8> {
    fn from_context(ctx: &mut Context) -> Result<Self, FromContextError> {
        Ok(ctx.data.clone())
    }
}
