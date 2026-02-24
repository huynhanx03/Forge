use crate::protocol::types::{Type, Varint, Varlong};
use crate::shared::byte::{decode_nullable_bytes, encode_nullable_bytes};
use bytes::{Buf, BufMut};

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub length: Varint,
    pub attributes: i8,
    pub timestamp_delta: Varlong,
    pub offset_delta: Varint,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

impl Record {
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        let length = Varint::decode(buf)?;

        if buf.remaining() < 1 {
            return Err("Not enough data for Record attributes".to_string());
        }

        let attributes = i8::decode(buf)?;
        let timestamp_delta = Varlong::decode(buf)?;
        let offset_delta = Varint::decode(buf)?;

        let key = decode_nullable_bytes(buf)?;
        let value = decode_nullable_bytes(buf)?;

        let headers_count = Varint::decode(buf)?;
        let mut headers = Vec::new();
        for _ in 0..headers_count.0 {
            let h_key_len = Varint::decode(buf)?;
            if h_key_len.0 < 0 {
                return Err("Not enough data for Header key length".to_string());
            }

            if buf.remaining() < h_key_len.0 as usize {
                return Err("Not enough data for Header key".to_string());
            }
            let mut hk_bytes = vec![0; h_key_len.0 as usize];
            buf.copy_to_slice(&mut hk_bytes);
            let h_key = String::from_utf8(hk_bytes).map_err(|_| "Invalid UTF-8 for Header key")?;

            let h_value = decode_nullable_bytes(buf)?;

            headers.push(Header {
                key: h_key,
                value: h_value,
            });
        }

        Ok(Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        self.length.encode(buf);
        self.attributes.encode(buf);
        self.timestamp_delta.encode(buf);
        self.offset_delta.encode(buf);

        encode_nullable_bytes(buf, &self.key);
        encode_nullable_bytes(buf, &self.value);

        Varint(self.headers.len() as i32).encode(buf);
        for header in &self.headers {
            Varint(header.key.len() as i32).encode(buf);
            buf.put_slice(header.key.as_bytes());
            encode_nullable_bytes(buf, &header.value);
        }
    }
}
