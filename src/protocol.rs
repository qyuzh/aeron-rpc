//! Request/Response Protocol
//!
//! Request:   |flags:1Byte|request_id:8Byte|business_id:8Byte|data|
//! Response:  |flags:1Byte|response_id:8Byte|data|
//!
//! flags
//! 0000'0000
//! - b1
//!   - 0: request
//!   - 1: response
//! - b2
//!   - 0: last data in stream
//!   - 1: not last data in stream

#[derive(Debug)]
pub struct Request {
    pub request_id: u64,
    pub business_id: u64,
    pub data: Vec<u8>,
}

impl Request {
    pub fn new(request_id: u64, business_id: u64, data: Vec<u8>) -> Self {
        Self {
            request_id,
            business_id,
            data,
        }
    }
}

impl From<Request> for Vec<u8> {
    fn from(req: Request) -> Self {
        let mut buf = Vec::with_capacity(1 + 8 + 8 + req.data.len());
        buf.push(0); // flags = 0 for request
        buf.extend(&req.request_id.to_le_bytes());
        buf.extend(&req.business_id.to_le_bytes());
        buf.extend(&req.data);
        buf
    }
}

impl TryFrom<&[u8]> for Request {
    type Error = String;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        if buf.len() < 1 + 8 + 8 {
            return Err("Request buffer too short".to_string());
        }

        let flags = buf[0];
        if flags & 1 != 0 {
            return Err("Not a request packet".to_string());
        }

        let request_id = u64::from_le_bytes(buf[1..9].try_into().unwrap());
        let business_id = u64::from_le_bytes(buf[9..17].try_into().unwrap());
        let data = buf[17..].to_vec();

        Ok(Self {
            request_id,
            business_id,
            data,
        })
    }
}

#[derive(Debug)]
pub struct Response {
    pub response_id: u64,
    pub is_last: bool,
    pub data: Vec<u8>,
}

impl Response {
    pub fn new(response_id: u64, is_last: bool, data: Vec<u8>) -> Self {
        Self {
            response_id,
            is_last,
            data,
        }
    }
}

impl From<Response> for Vec<u8> {
    fn from(resp: Response) -> Self {
        let mut buf = Vec::with_capacity(1 + 8 + resp.data.len());
        let flags: u8 = if resp.is_last { 4 } else { 0 };
        buf.push(1 | flags); // flags = 1 for response
        buf.extend(&resp.response_id.to_le_bytes());
        buf.extend(&resp.data);
        buf
    }
}

impl TryFrom<&[u8]> for Response {
    type Error = String;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        if buf.len() < 1 + 8 {
            return Err("Response buffer too short".to_string());
        }

        let flags = buf[0];
        if flags & 1 != 1 {
            return Err("Not a response packet".to_string());
        }

        let is_last = flags & 4 != 0;

        let response_id = u64::from_le_bytes(buf[1..9].try_into().unwrap());
        let data = buf[9..].to_vec();

        Ok(Self {
            response_id,
            is_last,
            data,
        })
    }
}
