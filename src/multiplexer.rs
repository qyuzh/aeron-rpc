use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::AtomicBool};

use aeron_rs::publication::Publication;
use aeron_rs::{
    concurrent::{atomic_buffer::AtomicBuffer, logbuffer::header::Header},
    fragment_assembler::FragmentAssembler,
    subscription::Subscription,
};
use tokio::sync::mpsc;

use crate::protocol::Status;
use crate::{
    RequestId,
    protocol::{Request, Response},
};

pub struct Multiplexer {
    publication: Arc<Mutex<Publication>>,
    subscription: Arc<Mutex<Subscription>>,
    server_sender: Option<mpsc::Sender<Request>>,
    server_receiver: Option<mpsc::Receiver<Response>>,
    client_receiver: Option<mpsc::Receiver<(Request, Option<mpsc::Sender<Response>>)>>,
    stop: Arc<AtomicBool>,
}

impl Multiplexer {
    pub fn new(
        publication: Arc<Mutex<Publication>>,
        subscription: Arc<Mutex<Subscription>>,
        server_sender: mpsc::Sender<Request>,
        server_receiver: mpsc::Receiver<Response>,
        client_receiver: mpsc::Receiver<(Request, Option<mpsc::Sender<Response>>)>,
    ) -> Self {
        Self {
            publication,
            subscription,
            server_sender: Some(server_sender),
            server_receiver: Some(server_receiver),
            client_receiver: Some(client_receiver),
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn run(&mut self) {
        std::thread::spawn({
            let mut resp_receiver = self
                .server_receiver
                .take()
                .expect("server receiver not found");
            let mut client_receiver = self
                .client_receiver
                .take()
                .expect("client receiver not found");
            let server_sender = self.server_sender.take().expect("server sender not found");
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
                        match client_receiver.try_recv() {
                            Ok((request, response_sender)) => {
                                if let Some(sender) = response_sender {
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
                                                status: Status::Other,
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
                                    if payload.status != Status::Ok {
                                        log::error!("Internal Error: {:?}", payload.status);
                                        continue;
                                    }

                                    if payload.is_last {
                                        if let Some(sender) = ht.remove(&payload.response_id) {
                                            sender.blocking_send(payload).unwrap();
                                        }
                                    } else if let Some(sender) = ht.get(&payload.response_id) {
                                        sender.blocking_send(payload).unwrap();
                                    }
                                } else if let Ok(payload) = Request::try_from(msg.as_slice()) {
                                    server_sender
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
                        match resp_receiver.try_recv() {
                            Ok(resp) => {
                                let mut buffer: Vec<u8> = resp.into();
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
