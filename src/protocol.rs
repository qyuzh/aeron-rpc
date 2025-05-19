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
//! - b3-b6
//!   - 0: response without internal error
//!   - 1: response with internal error
//!

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Status {
    Ok = 0,
    ParseError = 1,
    Timeout = 2,
    Other = 15,
}

impl From<u8> for Status {
    fn from(value: u8) -> Self {
        match value {
            0 => Status::Ok,
            1 => Status::ParseError,
            2 => Status::Timeout,
            15 => Status::Other,
            _ => Status::Other,
        }
    }
}

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

        let request_id = u64::from_le_bytes(buf[1..9].try_into().expect("Invalid request ID"));
        let business_id = u64::from_le_bytes(buf[9..17].try_into().expect("Invalid business ID"));
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
    pub status: Status,
    pub data: Vec<u8>,
}

impl Response {
    pub fn new(response_id: u64, is_last: bool, status: Status, data: Vec<u8>) -> Self {
        Self {
            response_id,
            is_last,
            status,
            data,
        }
    }
}

impl From<Response> for Vec<u8> {
    fn from(resp: Response) -> Self {
        let mut buf = Vec::with_capacity(1 + 8 + resp.data.len());
        let flags: u8 = if resp.is_last { 2 } else { 0 };
        let flags = flags | ((resp.status as u8) << 2);
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

        let is_last = flags & 2 != 0;
        let status = ((flags >> 2) & 15).into();

        let response_id = u64::from_le_bytes(buf[1..9].try_into().expect("Invalid response ID"));
        let data = buf[9..].to_vec();

        Ok(Self {
            response_id,
            is_last,
            status,
            data,
        })
    }
}
