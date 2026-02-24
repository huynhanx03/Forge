use crate::protocol::types::{Type, Varint};
use bytes::{Buf, BufMut};

pub fn decode_nullable_bytes<B: Buf>(buf: &mut B) -> Result<Option<Vec<u8>>, String> {
    let len = Varint::decode(buf)?;
    if len.0 < 0 {
        return Ok(None);
    }
    if buf.remaining() < len.0 as usize {
        return Err("Not enough data for nullable bytes".to_string());
    }
    let mut bytes = vec![0; len.0 as usize];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(bytes))
}

pub fn encode_nullable_bytes<B: BufMut>(buf: &mut B, bytes: &Option<Vec<u8>>) {
    match bytes {
        Some(bytes) => {
            Varint(bytes.len() as i32).encode(buf);
            buf.put_slice(bytes);
        }
        None => Varint(-1).encode(buf),
    }
}
