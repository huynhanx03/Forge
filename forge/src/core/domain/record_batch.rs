use crate::core::domain::record::Record;
use crate::protocol::types::Type;
use bytes::{Buf, BufMut};
use crc32fast::Hasher;

#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records_count: i32,
    pub records: Vec<Record>,
}

const PARTITION_LEADER_EPOCH_SIZE: usize = 4;
const MAGIC_SIZE: usize = 1;
const CRC_SIZE: usize = 4;
const HEADER_SIZE: usize = PARTITION_LEADER_EPOCH_SIZE + MAGIC_SIZE + CRC_SIZE;

pub const BATCH_HEADER_SIZE: usize = 8 + 4;
pub const BATCH_LENGTH_OFFSET: usize = 8;

impl RecordBatch {
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        let base_offset = i64::decode(buf)?;
        let batch_length = i32::decode(buf)?;
        let partition_leader_epoch = i32::decode(buf)?;
        let magic = i8::decode(buf)?;
        let crc = u32::decode(buf)?;

        let buf_bytes = buf.chunk();
        let expected_payload_len = batch_length as usize - HEADER_SIZE;
        if buf_bytes.len() < expected_payload_len {
            return Err("Not enough data for record batch payload".to_string());
        }

        let mut hasher = Hasher::new();
        hasher.update(&buf_bytes[..expected_payload_len]);
        let calculated_crc = hasher.finalize();
        if calculated_crc != crc {
            return Err("CRC check failed".to_string());
        }

        let attributes = i16::decode(buf)?;
        let last_offset_delta = i32::decode(buf)?;
        let base_timestamp = i64::decode(buf)?;
        let max_timestamp = i64::decode(buf)?;
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let base_sequence = i32::decode(buf)?;
        let records_count = i32::decode(buf)?;

        let mut records = Vec::with_capacity(records_count as usize);
        for _ in 0..records_count {
            records.push(Record::decode(buf)?);
        }

        Ok(RecordBatch {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
            records,
        })
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        let mut temp_buf = Vec::new();

        self.attributes.encode(&mut temp_buf);
        self.last_offset_delta.encode(&mut temp_buf);
        self.base_timestamp.encode(&mut temp_buf);
        self.max_timestamp.encode(&mut temp_buf);
        self.producer_id.encode(&mut temp_buf);
        self.producer_epoch.encode(&mut temp_buf);
        self.base_sequence.encode(&mut temp_buf);
        self.records_count.encode(&mut temp_buf);

        for record in &self.records {
            record.encode(&mut temp_buf);
        }

        let batch_length = (HEADER_SIZE + temp_buf.len()) as i32;
        let mut hasher = Hasher::new();
        hasher.update(&temp_buf);
        let crc = hasher.finalize();

        self.base_offset.encode(buf);
        batch_length.encode(buf);
        self.partition_leader_epoch.encode(buf);
        self.magic.encode(buf);
        crc.encode(buf);

        buf.put_slice(&temp_buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::record::Header;
    use crate::protocol::types::{Varint, Varlong};
    use bytes::BytesMut;

    #[test]
    fn test_record_batch_roundtrip() {
        // Record 1: Full payload with Key, Value, and Headers
        let record1 = Record {
            length: Varint(0),
            attributes: 0,
            timestamp_delta: Varlong(100),
            offset_delta: Varint(0),
            key: Some(b"hello_key".to_vec()),
            value: Some(b"world_value".to_vec()),
            headers: vec![Header {
                key: "header1".to_string(),
                value: Some(b"header_val".to_vec()),
            }],
        };

        // Record 2: Tombstone message (Null value, has key)
        let record2 = Record {
            length: Varint(0),
            attributes: 0,
            timestamp_delta: Varlong(150),
            offset_delta: Varint(1),
            key: Some(b"tombstone_key".to_vec()),
            value: None,
            headers: vec![],
        };

        // Record 3: Null Key, Empty Value, and Null Header Value
        let record3 = Record {
            length: Varint(0),
            attributes: 0,
            timestamp_delta: Varlong(200),
            offset_delta: Varint(2),
            key: None,
            value: Some(vec![]),
            headers: vec![Header {
                key: "empty_header".to_string(),
                value: None,
            }],
        };

        // Construct the parent RecordBatch containing the test records
        let original_batch = RecordBatch {
            base_offset: 12345,
            batch_length: 0, // Will be overwritten during encoding
            partition_leader_epoch: 42,
            magic: 2,
            crc: 0,        // Will be overwritten during encoding
            attributes: 1, // Suppose data is compressed
            last_offset_delta: 2,
            base_timestamp: 1670000000000,
            max_timestamp: 1670000000200,
            producer_id: 1001,
            producer_epoch: 5,
            base_sequence: 10,
            records_count: 3,
            records: vec![record1, record2, record3],
        };

        // Encode the batch into a buffer
        let mut buffer = BytesMut::new();
        original_batch.encode(&mut buffer);

        // Decode the buffer back into a RecordBatch object
        let mut read_buffer = std::io::Cursor::new(buffer.freeze());
        let decoded_batch =
            RecordBatch::decode(&mut read_buffer).expect("Failed to decode RecordBatch");

        // Verify batch-level header fields
        assert_eq!(original_batch.base_offset, decoded_batch.base_offset);
        assert_eq!(
            original_batch.partition_leader_epoch,
            decoded_batch.partition_leader_epoch
        );
        assert_eq!(original_batch.magic, decoded_batch.magic);
        assert_eq!(original_batch.attributes, decoded_batch.attributes);
        assert_eq!(
            original_batch.last_offset_delta,
            decoded_batch.last_offset_delta
        );
        assert_eq!(original_batch.base_timestamp, decoded_batch.base_timestamp);
        assert_eq!(original_batch.max_timestamp, decoded_batch.max_timestamp);
        assert_eq!(original_batch.producer_id, decoded_batch.producer_id);
        assert_eq!(original_batch.producer_epoch, decoded_batch.producer_epoch);
        assert_eq!(original_batch.base_sequence, decoded_batch.base_sequence);
        assert_eq!(original_batch.records_count, decoded_batch.records_count);
        assert_eq!(original_batch.records.len(), decoded_batch.records.len());

        // Verify Back-patched fields (Length & CRC32C)
        assert!(
            decoded_batch.batch_length > 0,
            "Batch length should be computed and > 0"
        );
        assert!(
            decoded_batch.crc != 0,
            "CRC32C checksum should be computed and strictly != 0"
        );

        // Verify Record 1 (Full payload)
        let decoded_record1 = &decoded_batch.records[0];
        let original_record1 = &original_batch.records[0];
        assert_eq!(original_record1.key, decoded_record1.key);
        assert_eq!(original_record1.value, decoded_record1.value);
        assert_eq!(
            original_record1.headers[0].key,
            decoded_record1.headers[0].key
        );
        assert_eq!(
            original_record1.headers[0].value,
            decoded_record1.headers[0].value
        );

        // Verify Record 2 (Tombstone Payload)
        let decoded_record2 = &decoded_batch.records[1];
        let original_record2 = &original_batch.records[1];
        assert_eq!(original_record2.key, decoded_record2.key);
        assert_eq!(original_record2.value, decoded_record2.value); // Should be None

        // Verify Record 3 (Null keys, empty values)
        let decoded_record3 = &decoded_batch.records[2];
        let original_record3 = &original_batch.records[2];
        assert_eq!(original_record3.key, decoded_record3.key); // Should be None
        assert_eq!(original_record3.value, decoded_record3.value); // Should be Some([])
        assert_eq!(
            original_record3.headers[0].value,
            decoded_record3.headers[0].value
        ); // Should be None
    }
}
