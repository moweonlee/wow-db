// T021: Arrow2 IPC RecordBatch 직렬화/역직렬화

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::ipc;
use arrow2::io::ipc::read::{read_stream_metadata, StreamState};

use crate::error::{Result, WowDbError};

/// Arrow RecordBatch를 IPC 스트림 바이너리로 직렬화
pub fn encode_batch(schema: &Schema, batch: &Chunk<Box<dyn Array>>) -> Result<Vec<u8>> {
    let mut buf    = Vec::new();
    let options    = ipc::write::WriteOptions { compression: None };
    let mut writer = ipc::write::StreamWriter::new(&mut buf, options);

    writer
        .start(schema, None)
        .map_err(|e| WowDbError::ArrowError(e.to_string()))?;
    writer
        .write(batch, None)
        .map_err(|e| WowDbError::ArrowError(e.to_string()))?;
    writer
        .finish()
        .map_err(|e| WowDbError::ArrowError(e.to_string()))?;

    Ok(buf)
}

/// IPC 스트림 바이너리에서 Arrow RecordBatch 역직렬화
pub fn decode_batch(data: &[u8]) -> Result<(Schema, Vec<Chunk<Box<dyn Array>>>)> {
    let mut cursor = std::io::Cursor::new(data);
    let metadata   = read_stream_metadata(&mut cursor)
        .map_err(|e| WowDbError::ArrowError(e.to_string()))?;
    let schema     = metadata.schema.clone();
    let reader     = ipc::read::StreamReader::new(cursor, metadata, None);

    let mut chunks = Vec::new();
    for maybe_state in reader {
        match maybe_state.map_err(|e| WowDbError::ArrowError(e.to_string()))? {
            StreamState::Some(chunk) => chunks.push(chunk),
            StreamState::Waiting     => {} // 추가 데이터 없음
        }
    }

    Ok((schema, chunks))
}

/// 빈 배치 생성 (스키마만 가진 0행 배치)
pub fn empty_batch(schema: &Schema) -> Chunk<Box<dyn Array>> {
    let arrays: Vec<Box<dyn Array>> = schema
        .fields
        .iter()
        .map(|f| arrow2::array::new_empty_array(f.data_type().clone()))
        .collect();
    Chunk::new(arrays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::array::Int64Array;
    use arrow2::datatypes::{DataType, Field};

    #[test]
    fn test_roundtrip_encode_decode() {
        let schema = Schema::from(vec![Field::new("id", DataType::Int64, false)]);
        let array: Box<dyn Array> = Box::new(Int64Array::from_vec(vec![1, 2, 3]));
        let chunk  = Chunk::new(vec![array]);

        let encoded = encode_batch(&schema, &chunk).unwrap();
        let (decoded_schema, decoded_chunks) = decode_batch(&encoded).unwrap();

        assert_eq!(decoded_schema, schema);
        assert_eq!(decoded_chunks.len(), 1);
        assert_eq!(decoded_chunks[0].len(), 3);
    }
}
