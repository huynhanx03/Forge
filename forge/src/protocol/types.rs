use bytes::{Buf, BufMut, BytesMut};
use uuid::Uuid;

pub trait Type {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String>;
    fn encode<B: BufMut>(&self, buf: &mut B);
}

impl Type for bool {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        if buf.remaining() < 1 {
            return Err("Not enough data for bool".to_string());
        }
        Ok(buf.get_u8() != 0)
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(*self as u8);
    }
}

macro_rules! impl_primitive {
    ($ty:ty, $size:expr, $read:ident, $write:ident) => {
        impl Type for $ty {
            fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
                if buf.remaining() < $size {
                    return Err(format!("Not enough data for {}", stringify!($ty)));
                }
                Ok(buf.$read())
            }

            fn encode<B: BufMut>(&self, buf: &mut B) {
                buf.$write(*self);
            }
        }
    };
}

impl_primitive!(i8, 1, get_i8, put_i8);
impl_primitive!(i16, 2, get_i16, put_i16);
impl_primitive!(i32, 4, get_i32, put_i32);
impl_primitive!(i64, 8, get_i64, put_i64);

macro_rules! impl_unsigned_varint_trait {
    ($name:ident, $inner:ty, $max_bytes:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name(pub $inner);

        impl Type for $name {
            fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
                let mut value: $inner = 0;
                let mut shift = 0;

                loop {
                    if buf.remaining() < 1 {
                        return Err(format!("Not enough data for {}", stringify!($name)));
                    }
                    let byte = buf.get_u8();
                    value |= ((byte & 0x7F) as $inner) << shift;

                    if byte & 0x80 == 0 {
                        break;
                    }

                    shift += 7;
                    if shift >= ($max_bytes * 7) {
                        return Err(format!("{} too long", stringify!($name)));
                    }
                }

                Ok($name(value))
            }

            fn encode<B: BufMut>(&self, buf: &mut B) {
                let mut value = self.0;

                loop {
                    if (value & !0x7F) == 0 {
                        buf.put_u8(value as u8);
                        break;
                    } else {
                        buf.put_u8(((value & 0x7F) | 0x80) as u8);
                        value >>= 7;
                    }
                }
            }
        }
    };
}

macro_rules! impl_signed_varint_trait {
    ($name:ident, $inner:ty, $unsigned:ty, $max_bytes:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name(pub $inner);

        impl Type for $name {
            fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
                let mut value: $unsigned = 0;
                let mut shift = 0;

                loop {
                    if buf.remaining() < 1 {
                        return Err(format!("Not enough data for {}", stringify!($name)));
                    }
                    let byte = buf.get_u8();
                    value |= ((byte & 0x7F) as $unsigned) << shift;

                    if byte & 0x80 == 0 {
                        break;
                    }

                    shift += 7;
                    if shift >= ($max_bytes * 7) {
                        return Err(format!("{} too long", stringify!($name)));
                    }
                }

                let zigzag = (value >> 1) as $inner ^ -((value & 1) as $inner);
                Ok($name(zigzag))
            }

            fn encode<B: BufMut>(&self, buf: &mut B) {
                let mut value =
                    ((self.0 as $unsigned) << 1) ^ ((self.0 >> (<$inner>::BITS - 1)) as $unsigned);

                loop {
                    if (value & !0x7F) == 0 {
                        buf.put_u8(value as u8);
                        break;
                    } else {
                        buf.put_u8(((value & 0x7F) | 0x80) as u8);
                        value >>= 7;
                    }
                }
            }
        }
    };
}

impl_signed_varint_trait!(Varint, i32, u32, 5);
impl_signed_varint_trait!(Varlong, i64, u64, 10);
impl_unsigned_varint_trait!(UnsignedVarint, u32, 5);
impl_unsigned_varint_trait!(UnsignedVarlong, u64, 10);

impl Type for String {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        if buf.remaining() < 2 {
            return Err("Not enough data for String length".to_string());
        }
        let len = buf.get_i16();
        if len < 0 {
            return Ok("".to_string());
        }
        let len = len as usize;

        if buf.remaining() < len {
            return Err("Not enough data for String".to_string());
        }
        let bytes = buf.split_to(len);
        String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put_i16(self.len() as i16);
        buf.put_slice(self.as_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactString(pub String);
impl Type for CompactString {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        let n = UnsignedVarint::decode(buf)?.0;

        if n == 0 {
            return Ok(CompactString("".to_string()));
        }

        let len = (n - 1) as usize;

        if buf.remaining() < len {
            return Err("Not enough data for CompactString".to_string());
        }

        let bytes = buf.split_to(len);
        String::from_utf8(bytes.to_vec())
            .map_err(|e| e.to_string())
            .map(CompactString)
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        let n = self.0.len() as u32 + 1;
        UnsignedVarint(n).encode(buf);
        buf.put_slice(self.0.as_bytes());
    }
}

impl Type for uuid::Uuid {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        if buf.remaining() < 16 {
            return Err("Not enough data for UUID".to_string());
        }
        let mut bytes = [0u8; 16];
        buf.copy_to_slice(&mut bytes);
        Ok(uuid::Uuid::from_bytes(bytes))
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put_slice(self.as_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactArray<T>(pub Vec<T>);
impl<T: Type> Type for CompactArray<T> {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        let n = UnsignedVarint::decode(buf)?.0;

        if n == 0 {
            return Ok(CompactArray(Vec::new()));
        }

        let len = (n - 1) as usize;

        if buf.remaining() < len {
            return Err("Not enough data for CompactArray".to_string());
        }

        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(T::decode(buf)?);
        }

        Ok(CompactArray(vec))
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        let n = self.0.len() as u32 + 1;
        UnsignedVarint(n).encode(buf);
        for item in &self.0 {
            item.encode(buf);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactBytes(pub Vec<u8>);
impl Type for CompactBytes {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        let n = UnsignedVarint::decode(buf)?.0;

        if n == 0 {
            return Ok(CompactBytes(Vec::new()));
        }

        let len = (n - 1) as usize;

        if buf.remaining() < len {
            return Err("Not enough data for CompactBytes".to_string());
        }

        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(CompactBytes(bytes))
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        let n = self.0.len() as u32 + 1;
        UnsignedVarint(n).encode(buf);
        buf.put_slice(&self.0);
    }
}
