pub mod aeron;
pub mod client;
mod protocol;
pub mod publisher;
pub mod server;
pub mod subscriber;

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
