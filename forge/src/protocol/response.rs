use bytes::BufMut;
use crate::protocol::types::Type;

#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    pub fn encode<B: BufMut>(self, buf: &mut B) {
        self.correlation_id.encode(buf);
    }
}