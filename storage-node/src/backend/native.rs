// T026: Native 로컬 파일시스템 I/O 백엔드
// Linux: tokio-uring (io_uring) — 그 외: tokio::fs fallback

use std::path::{Path, PathBuf};
use anyhow::Result;
use bytes::Bytes;
use tracing::debug;

// ─── 공개 인터페이스 ──────────────────────────────────────────────────────────

/// 로컬 스토리지 백엔드 (Native LSM)
pub struct NativeBackend {
    root: PathBuf,
}

impl NativeBackend {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self { root: root.as_ref().to_path_buf() }
    }

    /// 파일 전체 읽기
    pub async fn read(&self, rel_path: &str) -> Result<Bytes> {
        let full = self.root.join(rel_path);
        debug!(path = %full.display(), "NativeBackend::read");
        let data = read_file(&full).await?;
        Ok(Bytes::from(data))
    }

    /// 파일 전체 쓰기 (부모 디렉토리 자동 생성)
    pub async fn write(&self, rel_path: &str, data: &[u8]) -> Result<()> {
        let full = self.root.join(rel_path);
        debug!(path = %full.display(), bytes = data.len(), "NativeBackend::write");
        write_file(&full, data).await
    }

    /// 파일 삭제
    pub async fn delete(&self, rel_path: &str) -> Result<()> {
        let full = self.root.join(rel_path);
        tokio::fs::remove_file(&full).await?;
        Ok(())
    }

    /// 파일 존재 여부
    pub async fn exists(&self, rel_path: &str) -> bool {
        self.root.join(rel_path).exists()
    }

    /// 디렉토리 아래 파일 목록 (재귀 없음)
    pub async fn list(&self, rel_dir: &str) -> Result<Vec<String>> {
        let full = self.root.join(rel_dir);
        let mut result = Vec::new();
        let mut rd = tokio::fs::read_dir(&full).await?;
        while let Some(entry) = rd.next_entry().await? {
            if entry.file_type().await?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    result.push(format!("{}/{}", rel_dir, name));
                }
            }
        }
        Ok(result)
    }
}

// ─── 플랫폼별 I/O 구현 ────────────────────────────────────────────────────────

/// Linux + io-uring feature: tokio-uring 비동기 DIO
#[cfg(all(target_os = "linux", feature = "io-uring"))]
async fn read_file(path: &Path) -> Result<Vec<u8>> {
    use tokio_uring::fs::File;
    let file = File::open(path).await?;
    let size = file.metadata().await?.len() as usize;
    let buf  = vec![0u8; size];
    let (res, buf) = file.read_at(buf, 0).await;
    res?;
    Ok(buf)
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
async fn write_file(path: &Path, data: &[u8]) -> Result<()> {
    use tokio_uring::fs::{File, OpenOptions};
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let file  = OpenOptions::new().write(true).create(true).truncate(true).open(path).await?;
    let owned = data.to_vec();
    let (res, _) = file.write_at(owned, 0).await;
    res?;
    file.sync_all().await?;
    Ok(())
}

/// fallback: tokio::fs
#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
async fn read_file(path: &Path) -> Result<Vec<u8>> {
    let data = tokio::fs::read(path).await?;
    Ok(data)
}

#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
async fn write_file(path: &Path, data: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, data).await?;
    Ok(())
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_write_and_read() {
        let tmp     = TempDir::new().unwrap();
        let backend = NativeBackend::new(tmp.path());
        let data    = b"hello-wowdb";

        backend.write("col/seg-0001.col", data).await.unwrap();
        let read = backend.read("col/seg-0001.col").await.unwrap();
        assert_eq!(read.as_ref(), data);
    }

    #[tokio::test]
    async fn test_exists() {
        let tmp     = TempDir::new().unwrap();
        let backend = NativeBackend::new(tmp.path());
        assert!(!backend.exists("missing.col").await);
        backend.write("present.col", b"x").await.unwrap();
        assert!(backend.exists("present.col").await);
    }
}
