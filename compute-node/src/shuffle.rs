// T050: CN 간 데이터 교환 — Shuffle / Broadcast / Gather, Arrow IPC 포맷

use anyhow::Result;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use bytes::Bytes;
use shared::codec::{encode_batch, decode_batch};
use arrow2::datatypes::{Field, DataType, Schema};

// ─── 교환 모드 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeMode {
    /// Hash Shuffle: 키 컬럼 기반 파티셔닝
    HashShuffle { key_col: usize, n_partitions: usize },
    /// Broadcast: 모든 CN에 동일 데이터 전송
    Broadcast,
    /// Gather: 모든 CN의 결과를 단일 CN에 수집
    Gather,
    /// Passthrough: CN 간 이동 없음 (co-located)
    Passthrough,
}

// ─── Shuffle 파티셔너 ─────────────────────────────────────────────────────────

/// Arrow2 배치를 파티션별로 분할
pub fn partition_batch(
    batch:        &Chunk<Box<dyn Array>>,
    key_col:      usize,
    n_partitions: usize,
) -> Result<Vec<Chunk<Box<dyn Array>>>> {
    use arrow2::array::PrimitiveArray;

    if n_partitions == 0 {
        return Ok(vec![batch.clone()]);
    }

    let n = batch.len();
    let mut partition_indices: Vec<Vec<usize>> = vec![Vec::new(); n_partitions];

    // 키 컬럼에서 파티션 계산
    let key_arr = batch.arrays().get(key_col);
    for row in 0..n {
        let partition = if let Some(arr) = key_arr {
            if let Some(prim) = arr.as_any().downcast_ref::<PrimitiveArray<i64>>() {
                if prim.is_valid(row) {
                    (prim.value(row).unsigned_abs() as usize) % n_partitions
                } else {
                    0
                }
            } else {
                0
            }
        } else {
            0
        };
        partition_indices[partition].push(row);
    }

    // 각 파티션에서 배치 선택
    let result: Result<Vec<_>> = partition_indices.iter()
        .map(|indices| {
            let arrays: Vec<Box<dyn Array>> = batch.arrays().iter().map(|arr| {
                use arrow2::compute::take::take;
                let idx_arr: arrow2::array::PrimitiveArray<u32> =
                    arrow2::array::PrimitiveArray::from_vec(
                        indices.iter().map(|&i| i as u32).collect()
                    );
                take(arr.as_ref(), &idx_arr)
                    .unwrap_or_else(|_| arrow2::array::new_empty_array(arr.data_type().clone()))
            }).collect();
            Ok(Chunk::new(arrays))
        })
        .collect();

    result
}

// ─── Arrow IPC 직렬화 ─────────────────────────────────────────────────────────

/// 배치를 Arrow IPC 바이트로 직렬화 (네트워크 전송용)
pub fn serialize_batch(
    batch:  &Chunk<Box<dyn Array>>,
    schema: &Schema,
) -> Result<Bytes> {
    Ok(Bytes::from(encode_batch(schema, batch)?))
}

/// Arrow IPC 바이트에서 배치 역직렬화
pub fn deserialize_batch(data: &[u8]) -> Result<(Schema, Vec<Chunk<Box<dyn Array>>>)> {
    decode_batch(data).map_err(anyhow::Error::from)
}

// ─── Shuffle 메시지 ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct ShuffleMessage {
    pub query_id:     String,
    pub src_fragment: String,
    pub dst_fragment: String,
    pub partition:    usize,
    pub data:         Bytes,  // Arrow IPC
    pub is_last:      bool,
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::array::PrimitiveArray;

    fn make_i64_batch(vals: Vec<i64>) -> Chunk<Box<dyn Array>> {
        let arr: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vals));
        Chunk::new(vec![arr])
    }

    #[test]
    fn test_partition_batch() {
        let batch = make_i64_batch((0i64..100).collect());
        let partitions = partition_batch(&batch, 0, 4).unwrap();
        assert_eq!(partitions.len(), 4);

        let total: usize = partitions.iter().map(|p| p.len()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn test_single_partition() {
        let batch = make_i64_batch(vec![1, 2, 3]);
        let partitions = partition_batch(&batch, 0, 1).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 3);
    }
}
