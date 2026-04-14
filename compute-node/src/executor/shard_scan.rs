// T132: ShardScanStage — Operator 구현체
// CN이 SN gRPC ShardScanRequest를 호출하여 Arrow IPC RecordBatch를 받아
// Pipeline 채널로 내보내는 스테이지. 복수 Shard는 tokio::spawn으로 병렬 실행.
//
// NOTE: SN gRPC 호출은 ShardTransport 트레이트를 통해 추상화되어 있다.
// 실제 gRPC 클라이언트(scan_shard 메서드)는 proto 재생성 후 GrpcShardTransport로 구현된다.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use arrow2::io::ipc::read::{read_stream_metadata, StreamReader, StreamState};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{debug, warn};
use uuid::Uuid;

use shared::types::{LsmScanRange, ShardPredicate, ShardPredicateOp};

use crate::executor::pipeline::{Batch, BatchSender, BatchReceiver, Operator};

// ─── ShardSpec ───────────────────────────────────────────────────────────────

/// 단일 Shard 스캔 명세 (Fragment로부터 전달)
#[derive(Debug, Clone)]
pub struct ShardSpec {
    pub shard_id:         Uuid,
    pub sn_endpoint:      SocketAddr,
    pub shard_dir:        PathBuf,
    pub columns:          Vec<String>,
    pub predicates:       Vec<ShardPredicate>,
    pub scan_range:       LsmScanRange,
    pub bloom_probe_keys: Vec<Vec<u8>>,
}

// ─── ShardTransport 트레이트 ─────────────────────────────────────────────────

/// SN 스캔 호출 추상화
/// - 실 환경: GrpcShardTransport (tonic gRPC)
/// - 테스트: MockShardTransport
#[async_trait]
pub trait ShardTransport: Send + Sync + 'static {
    /// Shard 스캔을 실행하고 Arrow IPC 청크 바이트 목록을 반환
    async fn scan(&self, spec: &ShardSpec) -> Result<Vec<Vec<u8>>>;
}

/// gRPC ShardTransport — scan_shard RPC 사용 (proto 재생성 후 구현)
///
/// TODO: `cargo build` 후 아래 TODO 블록을 실제 gRPC 호출로 교체
pub struct GrpcShardTransport;

#[async_trait]
impl ShardTransport for GrpcShardTransport {
    async fn scan(&self, spec: &ShardSpec) -> Result<Vec<Vec<u8>>> {
        // TODO: tonic StorageServiceClient::scan_shard() 호출
        // storage.proto 업데이트 후 cargo build 시 자동 생성되는
        // StorageServiceClient::scan_shard(ShardScanRequest) → Stream<ShardScanResponse>
        // 현재는 빈 결과 반환 (stub)
        warn!(
            shard_id  = %spec.shard_id,
            endpoint  = %spec.sn_endpoint,
            "GrpcShardTransport::scan — proto 재생성 필요 (stub 반환)"
        );
        Ok(Vec::new())
    }
}

// ─── ShardScanStage ──────────────────────────────────────────────────────────

/// 복수 Shard를 병렬로 스캔하는 Operator
pub struct ShardScanStage<T: ShardTransport = GrpcShardTransport> {
    pub shards:    Vec<ShardSpec>,
    pub transport: T,
}

impl ShardScanStage<GrpcShardTransport> {
    pub fn new(shards: Vec<ShardSpec>) -> Self {
        Self { shards, transport: GrpcShardTransport }
    }
}

impl<T: ShardTransport> ShardScanStage<T> {
    pub fn with_transport(shards: Vec<ShardSpec>, transport: T) -> Self {
        Self { shards, transport }
    }
}

#[async_trait::async_trait]
impl<T: ShardTransport> Operator for ShardScanStage<T> {
    fn name(&self) -> &str { "ShardScan" }

    async fn execute(&self, _input: BatchReceiver, output: BatchSender) -> Result<()> {
        self.scan_all(output).await
    }
}

impl<T: ShardTransport> ShardScanStage<T> {
    /// 모든 Shard를 병렬 스캔하여 BatchSender로 내보냄
    pub async fn scan_all(&self, output: BatchSender) -> Result<()> {
        if self.shards.is_empty() {
            return Ok(());
        }

        // 직접 각 Shard를 순차적으로 처리하거나 병렬로 처리
        // 실제 tokio::spawn 병렬화는 Arc<T>로 transport 공유 시 가능
        for spec in &self.shards {
            let shard_id = spec.shard_id;
            debug!(shard_id = %shard_id, endpoint = %spec.sn_endpoint, "ShardScan 시작");

            match self.transport.scan(spec).await {
                Ok(chunks) => {
                    for ipc_bytes in chunks {
                        if ipc_bytes.is_empty() {
                            continue;
                        }
                        match decode_ipc_batch(&ipc_bytes) {
                            Ok(batch) => {
                                if output.send(Ok(batch)).await.is_err() {
                                    return Ok(()); // 다운스트림 종료
                                }
                            }
                            Err(e) => {
                                warn!(shard_id = %shard_id, err = %e, "IPC 디코딩 실패");
                                let _ = output.send(Err(e)).await;
                                return Ok(());
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(shard_id = %shard_id, err = %e, "ShardScan transport 오류");
                    let _ = output.send(Err(e)).await;
                    return Ok(());
                }
            }

            debug!(shard_id = %shard_id, "ShardScan 완료");
        }

        Ok(())
    }
}

// ─── Arrow IPC 디코딩 ─────────────────────────────────────────────────────────

fn decode_ipc_batch(ipc_bytes: &[u8]) -> Result<Batch> {
    let mut cursor = std::io::Cursor::new(ipc_bytes);
    let metadata   = read_stream_metadata(&mut cursor)
        .map_err(|e| anyhow!("IPC 메타데이터 읽기 실패: {}", e))?;
    let mut reader = StreamReader::new(cursor, metadata, None);

    match reader.next() {
        Some(Ok(StreamState::Some(batch))) => Ok(batch),
        Some(Ok(StreamState::Waiting))     => Ok(arrow2::chunk::Chunk::new(Vec::new())),
        Some(Err(e))                       => Err(anyhow!("IPC 배치 디코딩 실패: {}", e)),
        None                               => Ok(arrow2::chunk::Chunk::new(Vec::new())),
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::array::Int64Array;
    use arrow2::chunk::Chunk;
    use arrow2::io::ipc::write::{StreamWriter, WriteOptions};
    use arrow2::datatypes::{Schema, Field, DataType as ArrowDataType};
    use tokio::sync::mpsc;

    struct MockTransport {
        chunks: Vec<Vec<u8>>,
    }

    #[async_trait]
    impl ShardTransport for MockTransport {
        async fn scan(&self, _spec: &ShardSpec) -> Result<Vec<Vec<u8>>> {
            Ok(self.chunks.clone())
        }
    }

    fn make_ipc_batch() -> Vec<u8> {
        let schema = Schema::from(vec![
            Field::new("val", ArrowDataType::Int64, false),
        ]);
        let arr: Box<dyn arrow2::array::Array> = Box::new(Int64Array::from_vec(vec![1, 2, 3]));
        let chunk = Chunk::new(vec![arr]);
        let mut buf = Vec::new();
        let mut writer = StreamWriter::new(&mut buf, WriteOptions::default());
        writer.start(&schema, None).unwrap();
        writer.write(&chunk, None).unwrap();
        writer.finish().unwrap();
        buf
    }

    fn make_spec() -> ShardSpec {
        ShardSpec {
            shard_id:         Uuid::new_v4(),
            sn_endpoint:      "127.0.0.1:9060".parse().unwrap(),
            shard_dir:        PathBuf::from("/data/shard"),
            columns:          vec!["val".to_string()],
            predicates:       vec![],
            scan_range:       LsmScanRange::all_levels(),
            bloom_probe_keys: vec![],
        }
    }

    #[tokio::test]
    async fn test_mock_transport_returns_batches() {
        let ipc = make_ipc_batch();
        let transport = MockTransport { chunks: vec![ipc] };
        let stage = ShardScanStage::with_transport(vec![make_spec()], transport);

        let (tx, mut rx) = mpsc::channel(8);
        stage.scan_all(tx).await.unwrap();

        let batch = rx.recv().await.unwrap().unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn test_empty_shards_no_output() {
        let transport = MockTransport { chunks: vec![] };
        let stage = ShardScanStage::with_transport(vec![], transport);
        let (tx, mut rx) = mpsc::channel(4);
        stage.scan_all(tx).await.unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_multiple_shards_merged() {
        let ipc1 = make_ipc_batch();
        let ipc2 = make_ipc_batch();
        let transport = MockTransport { chunks: vec![ipc1, ipc2] };

        let spec1 = make_spec();
        let spec2 = make_spec();
        let stage = ShardScanStage::with_transport(vec![spec1, spec2], transport);

        let (tx, mut rx) = mpsc::channel(8);
        stage.scan_all(tx).await.unwrap();

        // MockTransport returns same 2 chunks for each of the 2 specs → 4 batches total
        let mut count = 0;
        while rx.recv().await.is_some() { count += 1; }
        assert_eq!(count, 4);
    }
}
