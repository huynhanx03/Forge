use bytes::Buf;
use crate::protocol::types::Type;

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

impl RequestHeader {
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        let api_key = i16::decode(buf)?;
        let api_version = i16::decode(buf)?;
        let correlation_id = i32::decode(buf)?;
        let client_id = if buf.remaining() >= 2 {
            let mut temp_buf = buf.check();

            if temp_buf.len() >= 2 {
                let len = i16::from_be_bytes(temp_buf[0], temp_buf[1]);
                
                if len < 0 {
                    buf.advance(2);
                    None
                } else {
                    String::decode(buf).ok()
                }
            }
        } else {
            None
        };
        
        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}