use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

use aeron_rs::{
    concurrent::atomic_buffer::AtomicBuffer, fragment_assembler::FragmentAssembler,
    subscription::Subscription,
};

use crate::{
    RequestId,
    protocol::{Request, Response},
};

pub struct Subscriber {
    subscription: Arc<Mutex<Subscription>>, // Aeron subscription for receiving messages
    client_senders: Arc<Mutex<HashMap<RequestId, tokio::sync::mpsc::Sender<Vec<u8>>>>>,
    server_senders: Arc<Mutex<Option<tokio::sync::mpsc::Sender<Request>>>>,
    join_handler: Option<JoinHandle<()>>,
}

impl Subscriber {
    pub fn new(subscription: Arc<Mutex<Subscription>>) -> Self {
        Subscriber {
            subscription,
            client_senders: Arc::new(Mutex::new(HashMap::new())),
            server_senders: Arc::new(Mutex::new(None)),
            join_handler: None,
        }
    }

    pub fn add_client_sender(
        &self,
        request_id: RequestId,
        sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) {
        self.client_senders
            .lock()
            .unwrap()
            .insert(request_id, sender);
    }

    pub fn add_server_sender(&mut self, sender: tokio::sync::mpsc::Sender<Request>) {
        self.server_senders.lock().unwrap().replace(sender);
    }

    pub fn run(&mut self) {
        let subscription = self.subscription.clone();
        let client_senders = self.client_senders.clone();
        let server_senders = self.server_senders.clone();

        self.join_handler = Some(std::thread::spawn(move || {
            let mut handler =
                |buffer: &AtomicBuffer,
                 offset: i32,
                 length: i32,
                 header: &aeron_rs::concurrent::logbuffer::header::Header| {
                    let slice_msg = unsafe {
                        std::slice::from_raw_parts(
                            buffer.buffer().offset(offset as isize),
                            length as usize,
                        )
                    } as &[u8];

                    if let Ok(payload) = Response::try_from(slice_msg) {
                        log::trace!(
                            "Received Response from: {}, {:?}",
                            header.session_id(),
                            payload
                        );
                        if payload.is_last {
                            if let Some(sender) =
                                client_senders.lock().unwrap().remove(&payload.response_id)
                            {
                                sender.blocking_send(payload.data).unwrap();
                            }
                        } else if let Some(sender) =
                            client_senders.lock().unwrap().get(&payload.response_id)
                        {
                            sender.blocking_send(payload.data).unwrap();
                        }
                    } else if let Ok(payload) = Request::try_from(slice_msg) {
                        log::trace!(
                            "Received Request from: {}, {:?}",
                            header.session_id(),
                            payload
                        );
                        server_senders
                            .lock()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .blocking_send(payload)
                            .unwrap();
                    } else {
                        log::trace!(
                            "Received message from: {}, msg: {:?}",
                            header.session_id(),
                            slice_msg
                        );
                    }
                };
            let mut fragment_assembler = FragmentAssembler::new(&mut handler, None);
            let handler_ref = &mut fragment_assembler.handler();
            loop {
                subscription.lock().unwrap().poll(handler_ref, 10);
            }
        }));
    }
}
