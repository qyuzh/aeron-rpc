use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::AtomicBool};

use aeron_rs::publication::Publication;
use aeron_rs::{
    concurrent::{atomic_buffer::AtomicBuffer, logbuffer::header::Header},
    fragment_assembler::FragmentAssembler,
    subscription::Subscription,
};
use tokio::sync::mpsc;

use crate::protocol::{
    Client2MultiplexerReceiver, Multiplexer2ServerSender, ResponsePacket, SendPacket,
    Server2MultiplexerReceiver,
};
use crate::{
    RequestId,
    protocol::{Request, Response},
};

pub struct Multiplexer {
    publication: Arc<Mutex<Publication>>,
    subscription: Arc<Mutex<Subscription>>,
    send_req_to_server: Option<Multiplexer2ServerSender>,
    recv_res_from_server: Option<Server2MultiplexerReceiver>,
    recv_req_from_client: Option<Client2MultiplexerReceiver>,
    stop: Arc<AtomicBool>,
}

impl Multiplexer {
    pub fn new(
        publication: Arc<Mutex<Publication>>,
        subscription: Arc<Mutex<Subscription>>,
        send_req_to_server: Multiplexer2ServerSender,
        recv_res_from_server: Server2MultiplexerReceiver,
        recv_req_from_server: Client2MultiplexerReceiver,
    ) -> Self {
        Self {
            publication,
            subscription,
            send_req_to_server: Some(send_req_to_server),
            recv_res_from_server: Some(recv_res_from_server),
            recv_req_from_client: Some(recv_req_from_server),
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn run2(&mut self) -> Result<(), MultiplexerError> {
        todo!()
    }

    pub fn run(&mut self) {
        std::thread::spawn({
            let mut recv_res_from_server = self
                .recv_res_from_server
                .take()
                .expect("server receiver not found");
            let mut recv_req_from_client = self
                .recv_req_from_client
                .take()
                .expect("client receiver not found");
            let send_req_to_server = self
                .send_req_to_server
                .take()
                .expect("server sender not found");
            let publication = self.publication.clone();
            let subscription = self.subscription.clone();

            let stop = self.stop.clone();

            move || {
                const CHANNEL_SIZE: usize = 20;
                let mut ht = HashMap::<RequestId, mpsc::Sender<Response>>::new();
                let (tx, mut rx) = mpsc::channel::<Vec<u8>>(CHANNEL_SIZE);

                let mut handler =
                    |buffer: &AtomicBuffer, offset: i32, length: i32, header: &Header| {
                        let slice_msg = unsafe {
                            std::slice::from_raw_parts(
                                buffer.buffer().offset(offset as isize),
                                length as usize,
                            )
                        } as &[u8];

                        log::trace!(
                            "Received Response from: {}, len: {}, pre_10: {:?}",
                            header.session_id(),
                            slice_msg.len(),
                            &slice_msg[..10]
                        );

                        tx.blocking_send(slice_msg.to_vec()).expect("send failed");
                    };
                let mut fragment_assembler = FragmentAssembler::new(&mut handler, None);
                let handler_ref = &mut fragment_assembler.handler();

                log::info!("Multiplexer is running...");
                loop {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        log::info!("Stopping the multiplexer");
                        break;
                    }

                    log::trace!("Send request from client...");
                    loop {
                        match recv_req_from_client.try_recv() {
                            Ok(SendPacket {
                                request,
                                resp_sender,
                                send_signal,
                            }) => {
                                if let Some(sender) = resp_sender {
                                    ht.insert(request.request_id, sender);
                                }
                                let request_id = request.request_id;
                                let mut buffer: Vec<u8> = request.into();
                                let atomic_buffer = AtomicBuffer::wrap_slice(&mut buffer);
                                publication
                                    .lock()
                                    .unwrap()
                                    .offer(atomic_buffer)
                                    .unwrap_or_else(|e| {
                                        let msg = format!("send failed: {:?}", e);
                                        log::error!("{}", msg);
                                        let _ = ht.remove(&request_id).unwrap().blocking_send(
                                            Response {
                                                response_id: request_id,
                                                is_last: true,
                                                data: msg.into_bytes(),
                                            },
                                        );
                                        0
                                    });
                            }
                            Err(e) => {
                                if e == mpsc::error::TryRecvError::Empty {
                                    break;
                                }
                            }
                        }
                    }

                    log::trace!("Poll message...");
                    subscription
                        .lock()
                        .unwrap()
                        .poll(handler_ref, (CHANNEL_SIZE >> 1) as i32);

                    log::trace!("Process messages from the handler...");
                    loop {
                        match rx.try_recv() {
                            Ok(msg) => {
                                if let Ok(payload) = Response::try_from(msg.as_slice()) {
                                    if payload.is_last {
                                        if let Some(sender) = ht.remove(&payload.response_id) {
                                            sender.blocking_send(payload).unwrap();
                                        }
                                    } else if let Some(sender) = ht.get(&payload.response_id) {
                                        sender.blocking_send(payload).unwrap();
                                    }
                                } else if let Ok(payload) = Request::try_from(msg.as_slice()) {
                                    send_req_to_server
                                        .blocking_send(payload)
                                        .expect("Server Handler Not Found");
                                } else {
                                    // TODO
                                }
                            }
                            Err(e) => {
                                if e == mpsc::error::TryRecvError::Empty {
                                    break;
                                }
                            }
                        }
                    }

                    log::trace!("Send response from server...");
                    loop {
                        match recv_res_from_server.try_recv() {
                            Ok(ResponsePacket {
                                response,
                                resp_signal,
                            }) => {
                                let mut buffer: Vec<u8> = response.into();
                                let atomic_buffer = AtomicBuffer::wrap_slice(&mut buffer);
                                publication
                                    .lock()
                                    .unwrap()
                                    .offer(atomic_buffer)
                                    .unwrap_or_else(|e| {
                                        log::error!("send failed: {:?}", e);
                                        0
                                    });
                            }
                            Err(e) => {
                                if e == mpsc::error::TryRecvError::Empty {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}

pub enum MultiplexerError {
    SendFailed(String),
}
