use bytes::BytesMut;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::consensus::node::Node;
use crate::core::domain::metadata_records::{
    MetadataRecord, PartitionRecord, RegisterBrokerRecord, TopicRecord,
};
use crate::core::domain::record::Record;
use crate::core::domain::record_batch::RecordBatch;
use crate::protocol::types::{Type, Varint, Varlong};

pub struct QuorumController {
    pub raft_node: Node,
}

impl QuorumController {
    pub fn new(raft_node: Node) -> Self {
        Self { raft_node }
    }

    pub async fn register_broker(
        &mut self,
        broker_id: i32,
        host: String,
        port: i32,
    ) -> Result<i64, String> {
        let record = MetadataRecord::RegisterBroker(RegisterBrokerRecord {
            broker_id,
            host,
            port,
        });

        self.append_metadata_record(record).await
    }

    pub async fn create_topic(
        &mut self,
        topic_name: String,
        partitions: Vec<PartitionRecord>,
    ) -> Result<i64, String> {
        let record = MetadataRecord::Topic(TopicRecord {
            topic_name,
            partitions,
        });

        self.append_metadata_record(record).await
    }

    async fn append_metadata_record(
        &mut self,
        metadata_record: MetadataRecord,
    ) -> Result<i64, String> {
        let mut value_buf = BytesMut::new();
        metadata_record.encode(&mut value_buf);

        let data_record = Record {
            length: Varint(0),
            attributes: 0,
            timestamp_delta: Varlong(0),
            offset_delta: Varint(0),
            key: None,
            value: Some(value_buf.to_vec()),
            headers: vec![],
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis() as i64;

        let batch = RecordBatch {
            base_offset: 0,
            batch_length: 0,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp: now,
            max_timestamp: now,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records_count: 1,
            records: vec![data_record],
        };

        self.raft_node.client_append_local(batch).await
    }
}
