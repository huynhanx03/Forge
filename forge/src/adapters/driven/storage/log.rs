use crate::adapters::driven::storage::segment::LOG_EXTENSION;
use crate::{adapters::driven::storage::segment::Segment, shared::fs::segment_file_path};
use crate::core::domain::record_batch::RecordBatch;
use std::path::{Path, PathBuf};

pub struct PartitionLog {
    pub dir: PathBuf,
    pub max_segment_size: u32,
    pub segments: Vec<Segment>,
    pub retention_bytes: u64,
    pub retention_ms: u64,
}

impl PartitionLog {
    pub async fn new(
        dir: impl AsRef<Path>,
        max_segment_size: u32,
        retention_bytes: u64,
        retention_ms: u64,
    ) -> std::io::Result<Self> {
        let dir_path = PathBuf::from(dir.as_ref());
        tokio::fs::create_dir_all(&dir_path).await?;

        let initial_segment = Segment::new(&dir_path, 0).await?;

        Ok(Self {
            dir: dir_path,
            max_segment_size,
            segments: vec![initial_segment],
            retention_bytes,
            retention_ms,
        })
    }

    pub async fn append(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let active_segment = self.segments.last_mut().ok_or("No active segment found")?;
        active_segment.append(batch).await?;

        if active_segment.current_size >= self.max_segment_size {
            let next_offset = batch.base_offset + batch.records_count as i64;
            let new_segment = Segment::new(&self.dir, next_offset)
                .await
                .map_err(|e| e.to_string())?;
            self.segments.push(new_segment);
        }

        Ok(())
    }

    fn find_segment_index(&self, offset: i64) -> Option<usize> {
        if self.segments.is_empty() {
            return None;
        }

        match self
            .segments
            .binary_search_by_key(&offset, |s| s.base_offset)
        {
            Ok(index) => Some(index),
            Err(index) => {
                if index == 0 {
                    None
                } else {
                    Some(index - 1)
                }
            }
        }
    }

    pub async fn read(&mut self, offset: i64) -> Result<Option<RecordBatch>, String> {
        let segment_index = match self.find_segment_index(offset) {
            Some(index) => index,
            None => return Ok(None),
        };

        let active_segment = &mut self.segments[segment_index];
        active_segment.read(offset).await
    }

    pub async fn read_sequential(
        &mut self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<RecordBatch>, String> {
        let segment_index = match self.find_segment_index(offset) {
            Some(index) => index,
            None => return Ok(vec![]),
        };

        let active_segment = &mut self.segments[segment_index];
        active_segment.read_sequential(offset, max_bytes).await
    }

    pub async fn remove_segment(&mut self, index: usize) -> Result<(), String> {
        if self.segments.len() == 1 {
            return Err("Cannot remove the last segment".to_string());
        }

        if index >= self.segments.len() {
            return Err("Segment index out of bounds".to_string());
        }

        let segment = self.segments.remove(index);
        segment.delete().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn enforce_retention(&mut self) -> Result<(), String> {
        if self.retention_bytes > 0 {
            self.enforce_retention_by_bytes().await?;
        }

        if self.retention_ms > 0 {
            self.enforce_retention_by_time().await?;
        }
        
        Ok(())   
    }

    pub async fn enforce_retention_by_bytes(&mut self) -> Result<(), String> {
        loop {
            if self.segments.len() <= 1 {
                break;
            }

            let total_size: u64 = self.segments.iter().map(|s| s.current_size as u64).sum();
            if total_size <= self.retention_bytes {
                break;
            }

            self.remove_segment(0).await?;
            tracing::info!("Removed old segment due to size limit");
        }

        Ok(())
    }

    pub async fn enforce_retention_by_time(&mut self) -> Result<(), String> {
        loop {
            if self.segments.len() <= 1 {
                break;
            }

            let old_segment = &self.segments[0];
            let file_path = segment_file_path(&self.dir, old_segment.base_offset, LOG_EXTENSION);
            let is_expired = match tokio::fs::metadata(&file_path).await {
                Ok(metadata) => {
                    let Ok(modified_time) = metadata.modified() else {
                        return Err("Failed to get modified time".to_string());
                    };

                    let Ok(duration) = modified_time.elapsed() else {
                        return Err("Failed to get duration".to_string());
                    };

                    duration.as_millis() as u64 > self.retention_ms
                }
                Err(_) => false,
            };

            if !is_expired {
                break;
            }

            self.remove_segment(0).await?;
            tracing::info!("Removed old segment due to time limit");
        }

        Ok(())
    }
}
