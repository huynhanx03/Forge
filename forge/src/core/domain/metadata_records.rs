use bytes::{Buf, BufMut};

use crate::protocol::types::Type;

#[derive(Debug, Clone, PartialEq)]
pub enum MetadataRecord {
    RegisterBroker(RegisterBrokerRecord),
    Topic(TopicRecord),
    Partition(PartitionRecord),
}

impl MetadataRecord {
    pub fn record_type(&self) -> i16 {
        match self {
            Self::RegisterBroker(_) => 27,
            Self::Topic(_) => 2,
            Self::Partition(_) => 3,
        }
    }
}

impl Type for MetadataRecord {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.record_type().encode(buf);
        match self {
            Self::RegisterBroker(r) => r.encode(buf),
            Self::Topic(r) => r.encode(buf),
            Self::Partition(r) => r.encode(buf),
        }
    }

    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        let record_type = i16::decode(buf)?;
        match record_type {
            27 => Ok(Self::RegisterBroker(RegisterBrokerRecord::decode(buf)?)),
            2 => Ok(Self::Topic(TopicRecord::decode(buf)?)),
            3 => Ok(Self::Partition(PartitionRecord::decode(buf)?)),
            _ => Err(format!("Unknown metadata record type: {}", record_type)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegisterBrokerRecord {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
}

impl Type for RegisterBrokerRecord {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.broker_id.encode(buf);
        self.host.encode(buf);
        self.port.encode(buf);
    }

    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        Ok(Self {
            broker_id: i32::decode(buf)?,
            host: String::decode(buf)?,
            port: i32::decode(buf)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicRecord {
    pub topic_name: String,
    pub partitions: Vec<PartitionRecord>,
}

impl Type for TopicRecord {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.topic_name.encode(buf);
        (self.partitions.len() as i32).encode(buf);
        for partition in &self.partitions {
            partition.encode(buf);
        }
    }

    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        let topic_name = String::decode(buf)?;
        let partitions_len = i32::decode(buf)?;
        let mut partitions = Vec::with_capacity(partitions_len as usize);
        for _ in 0..partitions_len {
            partitions.push(PartitionRecord::decode(buf)?);
        }
        Ok(Self {
            topic_name,
            partitions,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionRecord {
    pub topic_name: String,
    pub partition_index: i32,
    pub leader: String,
    pub replicas: Vec<String>,
}

impl Type for PartitionRecord {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.topic_name.encode(buf);
        self.partition_index.encode(buf);
        self.leader.encode(buf);
        (self.replicas.len() as i32).encode(buf);
        for replica in &self.replicas {
            replica.encode(buf);
        }
    }

    fn decode<B: Buf>(buf: &mut B) -> Result<Self, String> {
        let topic_name = String::decode(buf)?;
        let partition_index = i32::decode(buf)?;
        let leader = String::decode(buf)?;

        let replicas_len = i32::decode(buf)?;
        let mut replicas = Vec::with_capacity(replicas_len as usize);
        for _ in 0..replicas_len {
            replicas.push(String::decode(buf)?);
        }

        Ok(Self {
            topic_name,
            partition_index,
            leader,
            replicas,
        })
    }
}
