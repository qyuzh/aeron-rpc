use std::sync::{Arc, Mutex, atomic::AtomicU32};

use aeron_rs::publication::Publication;

use crate::{
    RequestId, ToBusinessId,
    protocol::{Request, Response},
};

#[derive(Clone)]
pub struct Publisher {
    pub publication: Arc<Mutex<Publication>>, // Aeron publication for sending messages
    pub request_id: Arc<AtomicU32>,
}

impl Publisher {
    pub fn new(publication: Arc<Mutex<Publication>>) -> Self {
        Self {
            publication,
            request_id: Arc::new(AtomicU32::new(1)),
        }
    }
}

impl Publisher {
    fn fetch_request_id(&self) -> u64 {
        let mut request_id = self
            .request_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u64;

        request_id <<= 32;
        request_id |= self.publication.lock().unwrap().session_id() as u32 as u64; // keep session_id in low 32 bits
        request_id
    }

    pub fn request(
        &self,
        business_id: &impl ToBusinessId,
        data: impl Into<Vec<u8>>,
    ) -> Result<u64, String> {
        let request_id = self.fetch_request_id();

        let mut buffer: Vec<u8> =
            Request::new(request_id, business_id.to_business_id(), data.into()).into();

        let atomic_buffer =
            aeron_rs::concurrent::atomic_buffer::AtomicBuffer::wrap_slice(&mut buffer);

        self.publication
            .lock()
            .unwrap()
            .offer(atomic_buffer)
            .map_err(|e| format!("send failed: {:?}", e))?;

        Ok(request_id)
    }

    pub fn response(&self, request_id: RequestId, is_last: bool, data: Vec<u8>) {
        let mut buffer: Vec<u8> = Response::new(request_id, is_last, data).into();
        let atomic_buffer =
            aeron_rs::concurrent::atomic_buffer::AtomicBuffer::wrap_slice(&mut buffer);
        self.publication
            .lock()
            .unwrap()
            .offer(atomic_buffer)
            .map_err(|e| format!("send failed: {:?}", e))
            .unwrap();
    }
}
