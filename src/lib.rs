pub mod aeron;
pub mod client;
pub mod multiplexer;
mod protocol;
pub mod server;

type RequestId = u64;
type BusinessId = u64;

pub trait ToBusinessId {
    fn to_business_id(&self) -> BusinessId;
}

pub enum Interface {
    Ping = 1,
    Echo = 2,
    Stream = 3,
}

impl ToBusinessId for Interface {
    fn to_business_id(&self) -> u64 {
        match self {
            Interface::Ping => 1,
            Interface::Echo => 2,
            Interface::Stream => 3,
        }
    }
}

pub trait FromBytes {
    fn from_bytes(data: Vec<u8>) -> Result<Self, String>
    where
        Self: Sized;
}

impl FromBytes for String {
    fn from_bytes(data: Vec<u8>) -> Result<Self, String> {
        String::from_utf8(data).map_err(|e| e.to_string())
    }
}

impl FromBytes for Vec<u8> {
    fn from_bytes(data: Vec<u8>) -> Result<Self, String> {
        Ok(data)
    }
}

pub trait ToBytes {
    fn to_bytes(self) -> Vec<u8>;
}

impl<T, E> ToBytes for Result<T, E>
where
    T: ToBytes,
    E: ToBytes,
{
    fn to_bytes(self) -> Vec<u8> {
        match self {
            Ok(data) => data.to_bytes(),
            Err(err) => err.to_bytes(),
        }
    }
}

impl ToBytes for () {
    fn to_bytes(self) -> Vec<u8> {
        Vec::new()
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(self) -> Vec<u8> {
        self
    }
}

impl ToBytes for String {
    fn to_bytes(self) -> Vec<u8> {
        self.into_bytes()
    }
}

impl ToBytes for &str {
    fn to_bytes(self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

macro_rules! impl_to_bytes_for_numbers {
    ($($t:ty),*) => {
        $(
            impl ToBytes for $t {
                fn to_bytes(self) -> Vec<u8> {
                    self.to_le_bytes().to_vec()
                }
            }
        )*
    };
}

impl_to_bytes_for_numbers!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aeron_rs::publication::Publication;
use aeron_rs::subscription::Subscription;
use client::RpcClient;
use multiplexer::Multiplexer;
use protocol::{Request, Response};
use server::{Handler, HandlerWrapper, IntoHandlerWrapper, RpcServer};
use tokio::sync::mpsc;

pub struct RpcContext {
    multiplexer: Multiplexer,
    rx: Option<mpsc::Receiver<Request>>,
    tx2: Option<mpsc::Sender<Response>>,
    tx3: Option<mpsc::Sender<(Request, Option<mpsc::Sender<Response>>)>>,
    handlers: Option<HashMap<u64, Arc<dyn Handler + Send + Sync>>>,
    is_taken_client: bool,
    is_taken_server: bool,
}

impl RpcContext {
    pub fn new(
        publication: Arc<Mutex<Publication>>,
        subscription: Arc<Mutex<Subscription>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1 << 10);
        let (tx2, rx2) = mpsc::channel(1 << 10);
        let (tx3, rx3) = mpsc::channel(1 << 10);

        Self {
            multiplexer: Multiplexer::new(publication, subscription, tx, rx2, rx3),
            rx: Some(rx),
            tx2: Some(tx2),
            tx3: Some(tx3),
            handlers: Some(HashMap::new()),
            is_taken_client: false,
            is_taken_server: false,
        }
    }

    pub fn add_handler<F, Args, Res>(
        &mut self,
        business_id: &impl ToBusinessId,
        func: F,
    ) -> &mut Self
    where
        F: IntoHandlerWrapper<Args, Res> + 'static,
        HandlerWrapper<F, Args, Res>: Handler + 'static,
        Res: ToBytes + 'static,
    {
        let business_id = business_id.to_business_id();
        let wrapper = func.into_handler_wrapper();
        self.handlers
            .as_mut()
            .unwrap()
            .insert(business_id, Arc::new(wrapper));
        self
    }

    pub fn get_rpc_client(&mut self) -> RpcClient {
        if self.is_taken_client {
            panic!("RpcClient has been taken");
        }
        self.is_taken_client = true;
        RpcClient::new(self.tx3.take().unwrap())
    }

    pub fn get_rpc_server(&mut self) -> RpcServer {
        if self.is_taken_server {
            panic!("RpcServer has been taken");
        }
        self.is_taken_server = true;
        RpcServer::new(
            self.handlers.take().unwrap().into(),
            self.rx.take().unwrap(),
            self.tx2.take().unwrap(),
        )
    }

    pub fn run(&mut self) {
        self.multiplexer.run();
    }
}
