use crate::{
    core::domain::record_batch::{BATCH_HEADER_SIZE, BATCH_LENGTH_OFFSET, RecordBatch},
    protocol::types::Type,
    shared::fs::{open_append_file, write_encoded_structure, delete_file},
};
use bytes::{BufMut, BytesMut};
use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

pub struct IndexEntry {
    pub relative_offset: i32,
    pub physical_position: u32,
}

impl IndexEntry {
    pub const SIZE: usize = 8;
    pub const RELATIVE_OFFSET_OFFSET: usize = 4;
    pub const PHYSICAL_POSITION_OFFSET: usize = 4;

    pub fn decode(buf: &[u8]) -> Self {
        Self {
            relative_offset: i32::from_be_bytes(
                buf[0..Self::RELATIVE_OFFSET_OFFSET].try_into().unwrap(),
            ),
            physical_position: u32::from_be_bytes(
                buf[Self::RELATIVE_OFFSET_OFFSET
                    ..Self::RELATIVE_OFFSET_OFFSET + Self::PHYSICAL_POSITION_OFFSET]
                    .try_into()
                    .unwrap(),
            ),
        }
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        self.relative_offset.encode(buf);
        self.physical_position.encode(buf);
    }
}

pub struct TimeIndexEntry {
    pub timestamp: i64,
    pub relative_offset: i32,
}

impl TimeIndexEntry {
    pub const SIZE: usize = 12;
    pub const TIMESTAMP_OFFSET: usize = 8;
    pub const RELATIVE_OFFSET_OFFSET: usize = 4;

    pub fn decode(buf: &[u8]) -> Self {
        Self {
            timestamp: i64::from_be_bytes(buf[0..Self::TIMESTAMP_OFFSET].try_into().unwrap()),
            relative_offset: i32::from_be_bytes(
                buf[Self::TIMESTAMP_OFFSET..Self::TIMESTAMP_OFFSET + Self::RELATIVE_OFFSET_OFFSET]
                    .try_into()
                    .unwrap(),
            ),
        }
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        self.timestamp.encode(buf);
        self.relative_offset.encode(buf);
    }
}

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const TIMEINDEX_EXTENSION: &str = "timeindex";

pub struct Segment {
    pub base_offset: i64,
    pub dir: PathBuf,
    pub log_file: File,
    pub index_file: File,
    pub timeindex_file: File,
    pub current_size: u32,
}

impl Segment {
    pub async fn new(dir: impl AsRef<Path>, base_offset: i64) -> std::io::Result<Self> {

        let log_file = open_append_file(&dir, base_offset, LOG_EXTENSION).await?;
        let index_file = open_append_file(&dir, base_offset, INDEX_EXTENSION).await?;
        let timeindex_file = open_append_file(&dir, base_offset, TIMEINDEX_EXTENSION).await?;

        let metadata = log_file.metadata().await?;
        let current_size = metadata.len() as u32;

        Ok(Self {
            base_offset,
            dir: PathBuf::from(dir.as_ref()),
            log_file,
            index_file,
            timeindex_file,
            current_size,
        })
    }

    pub async fn append(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let mut buffer = BytesMut::new();
        batch.encode(&mut buffer);

        self.log_file
            .write_all(&buffer)
            .await
            .map_err(|e| format!("IO error when writing log file: {}", e))?;

        let relative_offset = (batch.base_offset - self.base_offset) as i32;
        let physical_position = self.current_size;

        write_encoded_structure(
            &mut self.index_file,
            IndexEntry::SIZE,
            |buf| {
                IndexEntry {
                    relative_offset,
                    physical_position,
                }
                .encode(buf);
            },
            "index",
        )
        .await?;

        write_encoded_structure(
            &mut self.timeindex_file,
            TimeIndexEntry::SIZE,
            |buf| {
                TimeIndexEntry {
                    timestamp: batch.base_timestamp,
                    relative_offset,
                }
                .encode(buf);
            },
            "timeindex",
        )
        .await?;

        self.current_size += buffer.len() as u32;

        Ok(())
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.log_file.sync_data().await?;
        self.index_file.sync_data().await?;
        self.timeindex_file.sync_data().await?;
        Ok(())
    }

    async fn find_physical_position(&mut self, offset: i64) -> Result<Option<u32>, String> {
        if offset < self.base_offset {
            return Ok(None);
        }

        let relative_offset = (offset - self.base_offset) as i32;
        let metadata = self
            .index_file
            .metadata()
            .await
            .map_err(|e| format!("IO error when getting index file metadata: {}", e))?;
        let file_size = metadata.len() as usize;

        if file_size == 0 {
            return Ok(None);
        }

        let entries_count = file_size / IndexEntry::SIZE;
        let mut low = 0u64;
        let mut high = (entries_count - 1) as u64;

        let mut physical_position = 0u32;
        let mut index_buf = [0u8; IndexEntry::SIZE];

        while low <= high {
            let mid = low + ((high - low) >> 1);

            self.index_file
                .seek(SeekFrom::Start(mid * IndexEntry::SIZE as u64))
                .await
                .map_err(|e| format!("IO error when seeking index file: {}", e))?;
            self.index_file
                .read_exact(&mut index_buf)
                .await
                .map_err(|e| format!("IO error when reading index file: {}", e))?;

            let entry = IndexEntry::decode(&index_buf);

            if entry.relative_offset == relative_offset {
                return Ok(Some(entry.physical_position));
            }

            if entry.relative_offset < relative_offset {
                low = mid + 1;
                physical_position = entry.physical_position;
            } else {
                high = mid - 1;
            }
        }
        Ok(Some(physical_position))
    }

    pub async fn read(&mut self, offset: i64) -> Result<Option<RecordBatch>, String> {
        let physical_position = match self.find_physical_position(offset).await? {
            Some(pos) => pos,
            None => return Ok(None),
        };

        self.log_file
            .seek(SeekFrom::Start(physical_position as u64))
            .await
            .map_err(|e| format!("IO error when seeking log file: {}", e))?;

        let result = self.read_next_batch().await?;
        Ok(result.map(|(batch, _)| batch))
    }

    pub async fn read_sequential(
        &mut self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<RecordBatch>, String> {
        let physical_position = match self.find_physical_position(offset).await? {
            Some(pos) => pos,
            None => return Ok(vec![]),
        };

        self.log_file
            .seek(SeekFrom::Start(physical_position as u64))
            .await
            .map_err(|e| format!("IO error when seeking log file: {}", e))?;

        let mut batches = Vec::new();
        let mut bytes_read_total = 0;

        loop {
            if bytes_read_total >= max_bytes {
                break;
            }

            match self.read_next_batch().await {
                Ok(Some((batch, size))) => {
                    if bytes_read_total > 0 && bytes_read_total + size > max_bytes {
                        let _ = self.log_file.seek(SeekFrom::Current(-(size as i64))).await;
                        break;
                    }

                    if batch.base_offset + batch.records_count as i64 > offset {
                        batches.push(batch);
                    }
                    bytes_read_total += size;
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        Ok(batches)
    }

    async fn read_next_batch(&mut self) -> Result<Option<(RecordBatch, usize)>, String> {
        let mut header_buf = vec![0u8; BATCH_HEADER_SIZE];
        let bytes_read = self
            .log_file
            .read(&mut header_buf)
            .await
            .map_err(|e| format!("IO error when reading record batch header: {}", e))?;

        if bytes_read == 0 {
            return Ok(None);
        }

        if bytes_read < BATCH_HEADER_SIZE {
            return Err("Corrupted file: Lacking header size".to_string());
        }

        let batch_length = i32::from_be_bytes(
            header_buf[BATCH_LENGTH_OFFSET..BATCH_HEADER_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        let total_size = BATCH_HEADER_SIZE + batch_length;

        let mut full_batch_buf = BytesMut::zeroed(total_size);
        full_batch_buf[0..BATCH_HEADER_SIZE].copy_from_slice(&header_buf);

        self.log_file
            .read_exact(&mut full_batch_buf[BATCH_HEADER_SIZE..])
            .await
            .map_err(|e| format!("IO error when reading record batch payload: {}", e))?;

        let batch = RecordBatch::decode(&mut full_batch_buf)
            .map_err(|e| format!("Failed to decode record batch: {}", e))?;

        Ok(Some((batch, total_size)))
    }

    pub async fn delete(self) -> Result<(), String> {
        let _ = delete_file(&self.dir, self.base_offset, LOG_EXTENSION).await;
        let _ = delete_file(&self.dir, self.base_offset, INDEX_EXTENSION).await;
        let _ = delete_file(&self.dir, self.base_offset, TIMEINDEX_EXTENSION).await;

        Ok(())
    }
}
