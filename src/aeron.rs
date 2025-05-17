use std::sync::{Arc, Mutex};

use aeron_rs::publication::Publication;
use aeron_rs::subscription::Subscription;

pub struct AeronProxy {
    pub context: aeron_rs::context::Context,
    pub aeron: aeron_rs::aeron::Aeron,
}

pub struct AeronBuilder {
    dir: Option<String>,
    driver_timeout_ms: Option<i64>,
    client_timeout_ms: Option<i64>,
}

impl Default for AeronBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AeronBuilder {
    pub fn new() -> Self {
        AeronBuilder {
            dir: None,
            driver_timeout_ms: None,
            client_timeout_ms: None,
        }
    }

    pub fn dir(mut self, dir: impl Into<String>) -> Self {
        self.dir = Some(dir.into());
        self
    }

    pub fn driver_timeout_ms(mut self, timeout: i64) -> Self {
        self.driver_timeout_ms = Some(timeout);
        self
    }

    pub fn client_timeout_ms(mut self, timeout: i64) -> Self {
        self.client_timeout_ms = Some(timeout);
        self
    }

    pub fn build(self) -> Result<AeronProxy, String> {
        let mut context = aeron_rs::context::Context::new();
        if let Some(dir) = self.dir {
            context.set_aeron_dir(dir);
        }
        context.set_pre_touch_mapped_memory(true);
        let aeron = aeron_rs::aeron::Aeron::new(context.clone())
            .map_err(|e| format!("Failed to create Aeron: {:?}", e))?;
        Ok(AeronProxy { context, aeron })
    }
}

impl AeronProxy {
    pub fn add_subscription(
        &mut self,
        channel: &str,
        stream_id: i32,
    ) -> Result<Arc<Mutex<Subscription>>, String> {
        use std::ffi::CString;
        let c_channel =
            CString::new(channel).map_err(|e| format!("Invalid channel string: {:?}", e))?;
        let registration_id = self
            .aeron
            .add_subscription(c_channel, stream_id)
            .map_err(|e| format!("Failed to add subscription: {:?}", e))?;
        loop {
            if let Ok(subscription) = self.aeron.find_subscription(registration_id) {
                return Ok(subscription);
            }
            std::thread::yield_now();
        }
    }

    pub fn add_publication(
        &mut self,
        channel: &str,
        stream_id: i32,
    ) -> Result<Arc<Mutex<Publication>>, String> {
        use std::ffi::CString;
        let c_channel =
            CString::new(channel).map_err(|e| format!("Invalid channel string: {:?}", e))?;
        let publication_id = self
            .aeron
            .add_publication(c_channel, stream_id)
            .map_err(|e| format!("Failed to add publication: {:?}", e))?;

        let publication = loop {
            if let Ok(publication) = self.aeron.find_publication(publication_id) {
                break publication;
            }
            std::thread::yield_now();
        };
        Ok(publication)
    }
}
