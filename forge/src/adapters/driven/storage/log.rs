use crate::core::domain::record_batch::RecordBatch;
use crate::shared::constants::{INDEX_EXTENSION, LOG_EXTENSION, TIMEINDEX_EXTENSION};
use crate::{adapters::driven::storage::segment::Segment, shared::fs::segment_file_path};
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

    pub fn get_last_log_index(&self) -> i64 {
        if let Some(active_segment) = self.segments.last() {
            active_segment.last_offset
        } else {
            -1
        }
    }

    pub fn get_last_log_term(&self) -> u64 {
        if let Some(active_segment) = self.segments.last() {
            active_segment.last_term
        } else {
            0
        }
    }

    pub async fn get_term_at_index(&mut self, offset: i64) -> Result<Option<u64>, String> {
        let segment_index = match self.find_segment_index(offset) {
            Some(index) => index,
            None => return Ok(None),
        };
        let active_segment = &mut self.segments[segment_index];
        active_segment.get_term_at_index(offset).await
    }

    pub async fn truncate_from_index(&mut self, offset: i64) -> Result<(), String> {
        let start_segment_index = match self.find_segment_index(offset) {
            Some(index) => index,
            None => return Ok(()),
        };

        while self.segments.len() > start_segment_index + 1 {
            let _ = self.remove_segment(start_segment_index + 1).await;
        }

        let active_segment = &mut self.segments[start_segment_index];
        active_segment.truncate(offset).await?;

        Ok(())
    }

    pub fn get_first_log_index(&self) -> i64 {
        if let Some(first_segment) = self.segments.first() {
            first_segment.base_offset
        } else {
            0
        }
    }

    pub async fn truncate_prefix(&mut self, last_included_index: i64) -> Result<(), String> {
        loop {
            if self.segments.len() <= 1 {
                break;
            }

            if self.segments[1].base_offset <= last_included_index {
                self.remove_segment(0).await?;
                tracing::info!("Removed old segment due to snapshot prefix truncation");
            } else {
                break;
            }
        }
        Ok(())
    }

    pub async fn swap_compacted_segments(
        &mut self,
        num_closed_segments: usize,
        compacted_segments: Vec<Segment>,
    ) -> Result<(), String> {
        if num_closed_segments > self.segments.len() {
            return Err("Invalid number of segments to swap".to_string());
        }

        let old_segments: Vec<Segment> = self.segments.drain(0..num_closed_segments).collect();
        for old in old_segments {
            let _ = old.delete().await;
        }

        let mut new_segments = Vec::with_capacity(compacted_segments.len());
        let mut temp_dir = PathBuf::new();

        for compacted_segment in compacted_segments {
            let base_offset = compacted_segment.base_offset;
            temp_dir = compacted_segment.dir.clone();

            let extensions = [LOG_EXTENSION, INDEX_EXTENSION, TIMEINDEX_EXTENSION];
            for ext in extensions {
                let temp_file = segment_file_path(&temp_dir, base_offset, ext);
                let final_file = segment_file_path(&self.dir, base_offset, ext);
                tokio::fs::rename(temp_file, final_file)
                    .await
                    .map_err(|e| e.to_string())?;
            }

            let new_seg = Segment::new(&self.dir, base_offset)
                .await
                .map_err(|e| e.to_string())?;
            new_segments.push(new_seg);
        }

        if temp_dir.exists() {
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        }

        self.segments.splice(0..0, new_segments);

        Ok(())
    }
}
