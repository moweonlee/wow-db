// T028: 비동기 파이프라인 실행기 스켈레톤
// Operator chain, backpressure 채널, tokio task 기반 실행

use anyhow::Result;
use bytes::Bytes;
use arrow2::chunk::Chunk;
use arrow2::array::Array;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

// ─── 파이프라인 채널 설정 ─────────────────────────────────────────────────────

/// 파이프라인 채널 버퍼 크기 (backpressure 제어)
const PIPELINE_BUFFER: usize = 8;

// ─── 타입 별칭 ────────────────────────────────────────────────────────────────

pub type Batch = Chunk<Box<dyn Array>>;
pub type BatchSender   = mpsc::Sender<Result<Batch>>;
pub type BatchReceiver = mpsc::Receiver<Result<Batch>>;

// ─── Operator 트레이트 ────────────────────────────────────────────────────────

/// 모든 실행 연산자가 구현해야 하는 트레이트
#[async_trait::async_trait]
pub trait Operator: Send + Sync + 'static {
    fn name(&self) -> &str;
    /// 입력 Receiver에서 배치를 읽어 Sender로 변환 결과를 내보냄
    async fn execute(&self, input: BatchReceiver, output: BatchSender) -> Result<()>;
}

// ─── 파이프라인 빌더 ──────────────────────────────────────────────────────────

/// 연산자를 체인으로 연결하는 Pipeline
pub struct Pipeline {
    operators: Vec<Box<dyn Operator>>,
    query_id:  String,
}

impl Pipeline {
    pub fn new(query_id: impl Into<String>) -> Self {
        Self { operators: Vec::new(), query_id: query_id.into() }
    }

    pub fn add_operator(mut self, op: Box<dyn Operator>) -> Self {
        self.operators.push(op);
        self
    }

    /// 소스 채널을 첫 연산자에 연결하여 파이프라인 실행
    /// source: 최초 입력 배치 스트림 (예: SN 스캔 결과)
    /// 반환: 최종 결과 Receiver
    pub async fn run(self, source: BatchReceiver) -> Result<BatchReceiver> {
        if self.operators.is_empty() {
            return Ok(source);
        }

        let query_id = self.query_id.clone();
        let mut current_rx = source;

        for op in self.operators {
            let (tx, rx) = mpsc::channel(PIPELINE_BUFFER);
            let op_name  = op.name().to_string();
            let qid      = query_id.clone();

            tokio::spawn(async move {
                debug!(query_id = %qid, op = %op_name, "operator 시작");
                if let Err(e) = op.execute(current_rx, tx).await {
                    warn!(query_id = %qid, op = %op_name, err = %e, "operator 오류");
                }
                debug!(query_id = %qid, op = %op_name, "operator 완료");
            });

            current_rx = rx;
        }

        Ok(current_rx)
    }
}

// ─── 기본 연산자 구현 ─────────────────────────────────────────────────────────

/// Pass-through: 입력을 그대로 내보내는 no-op 연산자 (테스트용)
pub struct PassThroughOp;

#[async_trait::async_trait]
impl Operator for PassThroughOp {
    fn name(&self) -> &str { "PassThrough" }

    async fn execute(&self, mut input: BatchReceiver, output: BatchSender) -> Result<()> {
        while let Some(batch) = input.recv().await {
            if output.send(batch).await.is_err() {
                break; // 다운스트림 종료
            }
        }
        Ok(())
    }
}

/// Limit: 최대 N 행 반환 연산자
pub struct LimitOp {
    pub limit: usize,
}

#[async_trait::async_trait]
impl Operator for LimitOp {
    fn name(&self) -> &str { "Limit" }

    async fn execute(&self, mut input: BatchReceiver, output: BatchSender) -> Result<()> {
        let mut remaining = self.limit;
        while let Some(batch_result) = input.recv().await {
            match batch_result {
                Err(e) => {
                    let _ = output.send(Err(e)).await;
                    break;
                }
                Ok(batch) => {
                    let rows = batch.len();
                    if remaining == 0 {
                        break;
                    }
                    if rows <= remaining {
                        remaining -= rows;
                        if output.send(Ok(batch)).await.is_err() { break; }
                    } else {
                        // 마지막 배치 자르기
                        // TODO (Phase C): Arrow2 slice 구현
                        if output.send(Ok(batch)).await.is_err() { break; }
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

// ─── Fragment 실행 컨텍스트 ───────────────────────────────────────────────────

/// Fragment 하나의 실행 결과
pub struct FragmentResult {
    pub query_id:    String,
    pub fragment_id: String,
    pub rows_output: u64,
    pub elapsed_ms:  u64,
}

/// Fragment 실행 진입점 (Phase C에서 실제 구현)
pub async fn execute_fragment(
    query_id:    String,
    fragment_id: String,
    _plan_bytes: Bytes,
) -> Result<BatchReceiver> {
    info!(query_id = %query_id, fragment_id = %fragment_id, "Fragment 실행 시작");

    let (tx, rx) = mpsc::channel(PIPELINE_BUFFER);
    tokio::spawn(async move {
        // TODO (Phase C): plan_bytes를 역직렬화하여 실제 연산자 체인 구성
        // 현재는 빈 결과 반환
        drop(tx);
    });

    Ok(rx)
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::array::Int64Array;

    fn make_batch(n: i64) -> Batch {
        let arr: Box<dyn Array> = Box::new(Int64Array::from_vec(vec![n]));
        Chunk::new(vec![arr])
    }

    #[tokio::test]
    async fn test_passthrough_pipeline() {
        let (src_tx, src_rx) = mpsc::channel(8);
        for i in 0i64..3 {
            src_tx.send(Ok(make_batch(i))).await.unwrap();
        }
        drop(src_tx);

        let pipeline = Pipeline::new("q1").add_operator(Box::new(PassThroughOp));
        let mut out  = pipeline.run(src_rx).await.unwrap();

        let mut count = 0;
        while let Some(Ok(_)) = out.recv().await {
            count += 1;
        }
        assert_eq!(count, 3);
    }
}
