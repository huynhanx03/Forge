use crate::core::domain::metadata_records::{
    MetadataRecord, PartitionRecord, RegisterBrokerRecord,
};
use crate::shared::collections::FlatMap;

#[derive(Debug, Clone)]
pub struct ClusterMetadataCache {
    /// Maps broker_id to its registration details
    pub brokers: FlatMap<i32, RegisterBrokerRecord>,
    /// Maps topic_name to its metadata and partitions
    pub topics: FlatMap<String, TopicMetadata>,
    /// The offset of the highest metadata record applied to this cache
    pub last_applied_offset: i64,
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub name: String,
    /// Maps partition_index to its replicas and leader state
    pub partitions: FlatMap<i32, PartitionRecord>,
}

impl ClusterMetadataCache {
    pub fn new() -> Self {
        Self {
            brokers: FlatMap::new(),
            topics: FlatMap::new(),
            last_applied_offset: 0,
        }
    }

    pub fn apply_record(&mut self, offset: i64, record: &MetadataRecord) {
        match record {
            MetadataRecord::RegisterBroker(broker) => {
                self.brokers.insert(broker.broker_id, broker.clone());
            }
            MetadataRecord::Topic(topic) => {
                let mut partitions_map = FlatMap::new();
                for p in &topic.partitions {
                    partitions_map.insert(p.partition_index, p.clone());
                }

                self.topics.insert(
                    topic.topic_name.clone(),
                    TopicMetadata {
                        name: topic.topic_name.clone(),
                        partitions: partitions_map,
                    },
                );
            }
            MetadataRecord::Partition(partition) => {
                if let Some(topic_meta) = self.topics.get_mut(&partition.topic_name) {
                    topic_meta
                        .partitions
                        .insert(partition.partition_index, partition.clone());
                } else {
                    let mut partitions_map = FlatMap::new();
                    partitions_map.insert(partition.partition_index, partition.clone());
                    self.topics.insert(
                        partition.topic_name.clone(),
                        TopicMetadata {
                            name: partition.topic_name.clone(),
                            partitions: partitions_map,
                        },
                    );
                }
            }
        }
        self.last_applied_offset = offset;
    }

    pub fn replay_records(&mut self, offset: i64, records: &[MetadataRecord]) {
        for record in records {
            self.apply_record(offset, record);
        }
    }

    pub fn generate_snapshot_records(&self) -> Vec<MetadataRecord> {
        let mut snapshot = Vec::new();

        for broker in self.brokers.values() {
            snapshot.push(MetadataRecord::RegisterBroker(broker.clone()));
        }

        for topic_meta in self.topics.values() {
            let mut partitions_vec = Vec::new();
            for partition in topic_meta.partitions.values() {
                partitions_vec.push(partition.clone());
            }

            let topic_record = crate::core::domain::metadata_records::TopicRecord {
                topic_name: topic_meta.name.clone(),
                partitions: partitions_vec,
            };
            snapshot.push(MetadataRecord::Topic(topic_record));
        }

        snapshot
    }
}
