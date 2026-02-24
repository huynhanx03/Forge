use bytes::BytesMut;
use std::path::{Path, PathBuf};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};

pub fn segment_file_path(dir: impl AsRef<Path>, base_offset: i64, extension: &str) -> PathBuf {
    let filename = format!("{:020}", base_offset);
    let mut file_path = PathBuf::from(dir.as_ref());
    file_path.push(format!("{}.{}", filename, extension));
    file_path
}

pub async fn open_append_file(
    dir: impl AsRef<Path>,
    base_offset: i64,
    extension: &str,
) -> std::io::Result<File> {
    let file_path = segment_file_path(dir, base_offset, extension);
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
        .await
}

pub async fn delete_file(
    dir: impl AsRef<Path>,
    base_offset: i64,
    extension: &str,
) -> std::io::Result<()> {
    let file_path = segment_file_path(dir, base_offset, extension);
    tokio::fs::remove_file(file_path).await
}

pub async fn write_encoded_structure(
    file: &mut File,
    size: usize,
    encoder: impl FnOnce(&mut BytesMut),
    file_label: &str,
) -> Result<(), String> {
    let mut buffer = BytesMut::with_capacity(size);
    encoder(&mut buffer);
    file.write_all(&buffer)
        .await
        .map_err(|e| format!("IO error when writing to {} file: {}", file_label, e))?;
    Ok(())
}
