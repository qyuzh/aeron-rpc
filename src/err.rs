#[derive(Debug)]
pub enum SendError {
    Timeout,
    SendFailed(String),
    ParseError(String),
    Custom(String),
}

#[derive(Debug)]
pub enum ReceiveError {
    ParseError(String),
    Custom(String),
}

#[derive(Debug)]
pub enum FromBytesError {
    ParseError(String),
    Custom(String),
}

impl From<FromBytesError> for SendError {
    fn from(err: FromBytesError) -> Self {
        match err {
            FromBytesError::ParseError(e) => SendError::ParseError(e),
            FromBytesError::Custom(e) => SendError::Custom(e),
        }
    }
}

impl From<FromBytesError> for ReceiveError {
    fn from(err: FromBytesError) -> Self {
        match err {
            FromBytesError::ParseError(e) => ReceiveError::ParseError(e),
            FromBytesError::Custom(e) => ReceiveError::Custom(e),
        }
    }
}
