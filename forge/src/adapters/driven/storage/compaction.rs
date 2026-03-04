use crate::adapters::driven::storage::log::PartitionLog;
use crate::adapters::driven::storage::segment::Segment;
use crate::protocol::types::Type;
use crate::shared::constants::CLEANED_DIR_NAME;
use std::collections::HashMap;

pub struct LogCleaner;

impl LogCleaner {
    pub async fn compact(log: &mut PartitionLog) -> Result<(), String> {
        if log.segments.len() <= 1 {
            return Ok(());
        }

        let num_closed_segments = log.segments.len() - 1;
        let mut key_offsets: HashMap<Vec<u8>, i64> = HashMap::new();

        for i in 0..num_closed_segments {
            let segment = &mut log.segments[i];
            let mut current_offset = segment.base_offset;

            loop {
                match segment.read(current_offset).await {
                    Ok(Some(batch)) => {
                        for record in &batch.records {
                            if let Some(key) = &record.key {
                                let absolute_offset =
                                    batch.base_offset + record.offset_delta.0 as i64;
                                key_offsets.insert(key.clone(), absolute_offset);
                            }
                        }
                        current_offset = batch.base_offset + batch.last_offset_delta as i64 + 1;
                    }
                    _ => break,
                }
            }
        }

        if key_offsets.is_empty() {
            return Ok(());
        }

        let base_offset = log.segments[0].base_offset;
        let temp_dir = log.dir.join(CLEANED_DIR_NAME);
        tokio::fs::create_dir_all(&temp_dir)
            .await
            .map_err(|e| e.to_string())?;

        let mut compacted_segments = Vec::new();
        let mut current_compacted_segment = Segment::new(&temp_dir, base_offset)
            .await
            .map_err(|e| e.to_string())?;

        for i in 0..num_closed_segments {
            let segment = &mut log.segments[i];
            let mut current_offset = segment.base_offset;

            loop {
                match segment.read(current_offset).await {
                    Ok(Some(batch)) => {
                        let mut keep_records = Vec::new();

                        for record in &batch.records {
                            let keep = match &record.key {
                                Some(key) => {
                                    if let Some(&latest_offset) = key_offsets.get(key) {
                                        let absolute_offset =
                                            batch.base_offset + record.offset_delta.0 as i64;
                                        absolute_offset == latest_offset
                                    } else {
                                        true
                                    }
                                }
                                None => true,
                            };

                            if keep {
                                keep_records.push(record.clone());
                            }
                        }

                        if !keep_records.is_empty() {
                            let mut new_batch = batch.clone();
                            new_batch.records = keep_records;
                            new_batch.records_count = new_batch.records.len() as i32;

                            let mut temp_buf = Vec::new();
                            new_batch.attributes.encode(&mut temp_buf);
                            new_batch.last_offset_delta.encode(&mut temp_buf);
                            new_batch.base_timestamp.encode(&mut temp_buf);
                            new_batch.max_timestamp.encode(&mut temp_buf);
                            new_batch.producer_id.encode(&mut temp_buf);
                            new_batch.producer_epoch.encode(&mut temp_buf);
                            new_batch.base_sequence.encode(&mut temp_buf);
                            new_batch.records_count.encode(&mut temp_buf);
                            for rec in &new_batch.records {
                                rec.encode(&mut temp_buf);
                            }
                            let batch_size = crate::core::domain::record_batch::BATCH_HEADER_SIZE
                                + temp_buf.len();

                            if current_compacted_segment.current_size > 0
                                && current_compacted_segment.current_size + batch_size as u32
                                    > log.max_segment_size
                            {
                                current_compacted_segment
                                    .flush()
                                    .await
                                    .map_err(|e| e.to_string())?;
                                compacted_segments.push(current_compacted_segment);

                                let next_offset = new_batch.base_offset;
                                current_compacted_segment = Segment::new(&temp_dir, next_offset)
                                    .await
                                    .map_err(|e| e.to_string())?;
                            }

                            current_compacted_segment.append(&new_batch).await?;
                        }

                        current_offset = batch.base_offset + batch.last_offset_delta as i64 + 1;
                    }
                    _ => break,
                }
            }
        }

        current_compacted_segment
            .flush()
            .await
            .map_err(|e| e.to_string())?;
        compacted_segments.push(current_compacted_segment);

        log.swap_compacted_segments(num_closed_segments, compacted_segments)
            .await?;

        Ok(())
    }
}
