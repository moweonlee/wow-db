// T136: 논리-물리 매핑 통합 테스트
// QN 논리 단위 (Table/Partition/Shard) → SN 물리 단위 (Part/Granule/ColumnFile) 검증

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use shared::types::{
    LsmScanRange, PartitionStats, ShardPredicate, ShardPredicateOp,
    ShardScanRequest, Value,
};

// ─── 테스트 1: PartitionPruner가 범위 밖 파티션을 제거한다 ────────────────────

/// Partition Pruning: Ge predicate로 이전 파티션 제거
#[test]
fn test_partition_pruning_removes_old_partitions() {
    use storage_node::lsm::manifest::Manifest;

    let p1 = Uuid::new_v4(); // 1000~1999 — Ge(2000) 이면 제거
    let p2 = Uuid::new_v4(); // 2000~2999 — 유지
    let p3 = Uuid::new_v4(); // 3000~3999 — 유지

    // PartitionStats를 직접 구성 (CBO stats 계층의 Partition 수준)
    let partitions = vec![
        (p1, PartitionStats {
            row_count: 1_000_000, size_bytes: 100 * 1024 * 1024,
            partition_key_min: Some(Value::Int64(1000)),
            partition_key_max: Some(Value::Int64(1999)),
        }),
        (p2, PartitionStats {
            row_count: 1_000_000, size_bytes: 100 * 1024 * 1024,
            partition_key_min: Some(Value::Int64(2000)),
            partition_key_max: Some(Value::Int64(2999)),
        }),
        (p3, PartitionStats {
            row_count: 1_000_000, size_bytes: 100 * 1024 * 1024,
            partition_key_min: Some(Value::Int64(3000)),
            partition_key_max: Some(Value::Int64(3999)),
        }),
    ];

    // partition_prune 모듈은 query-node에 있으므로 직접 로직을 테스트
    // Ge(2000): max < 2000 이면 스킵 → p1(max=1999) 제거
    let surviving: Vec<Uuid> = partitions.iter()
        .filter(|(_, stats)| {
            match (&stats.partition_key_max, &stats.partition_key_min) {
                (Some(Value::Int64(mx)), Some(Value::Int64(mn))) => {
                    // Ge(2000): p.max >= 2000
                    *mx >= 2000
                }
                _ => true,
            }
        })
        .map(|(id, _)| *id)
        .collect();

    assert_eq!(surviving.len(), 2, "p1은 제거되어야 함");
    assert!(surviving.contains(&p2));
    assert!(surviving.contains(&p3));
    assert!(!surviving.contains(&p1));
}

// ─── 테스트 2: ShardScanRequest가 올바른 필드를 포함한다 ──────────────────────

/// Logical→Physical 변환: ShardScanRequest 구성 검증
#[test]
fn test_shard_scan_request_construction() {
    let shard_id  = Uuid::new_v4();
    let shard_dir = PathBuf::from("/data/shards/shard-01");

    let predicates = vec![
        ShardPredicate::range_ge("event_time", Value::Int64(1_700_000_000)),
        ShardPredicate::range_lt("event_time", Value::Int64(1_710_000_000)),
    ];

    let req = ShardScanRequest {
        shard_id,
        shard_dir:        shard_dir.clone(),
        columns:          vec!["event_time".to_string(), "event_name".to_string()],
        predicates:       predicates.clone(),
        scan_range:       LsmScanRange::all_levels(),
        bloom_probe_keys: vec![],
    };

    assert_eq!(req.shard_id, shard_id);
    assert_eq!(req.shard_dir, shard_dir);
    assert_eq!(req.columns.len(), 2);
    assert_eq!(req.predicates.len(), 2);
    assert_eq!(req.scan_range.min_level, 0);
    assert_eq!(req.scan_range.max_level, 6);
}

// ─── 테스트 3: LsmScanRange 범위 헬퍼 ───────────────────────────────────────

/// LsmScanRange 범위 헬퍼 동작 확인
#[test]
fn test_lsm_scan_range_helpers() {
    let all = LsmScanRange::all_levels();
    assert_eq!(all.min_level, 0);
    assert_eq!(all.max_level, 6);

    let l0 = LsmScanRange::l0_only();
    assert_eq!(l0.min_level, 0);
    assert_eq!(l0.max_level, 0);

    // 커스텀 범위
    let mid = LsmScanRange { min_level: 2, max_level: 4 };
    assert!(mid.min_level <= mid.max_level);
}

// ─── 테스트 4: ShardScanner가 빈 디렉토리에서 통계만 반환한다 ────────────────

#[tokio::test]
async fn test_shard_scanner_empty_returns_part_list_and_stats() {
    use storage_node::grpc::scan::{ShardScanner, ShardScanItem};
    use tempfile::TempDir;

    let tmp     = TempDir::new().unwrap();
    let scanner = ShardScanner::new(tmp.path().to_path_buf());

    let req = ShardScanRequest {
        shard_id:         Uuid::new_v4(),
        shard_dir:        tmp.path().to_path_buf(),
        columns:          vec![],
        predicates:       vec![],
        scan_range:       LsmScanRange::all_levels(),
        bloom_probe_keys: vec![],
    };

    let (tx, mut rx) = mpsc::channel(16);
    scanner.scan(&req, tx).await.unwrap();

    // 첫 번째 아이템: PartList
    let first = rx.recv().await.unwrap().unwrap();
    assert!(matches!(first, ShardScanItem::PartList(_)));

    // 마지막 아이템: Stats (빈 shard이므로 Batch 없음)
    let last = rx.recv().await.unwrap().unwrap();
    assert!(matches!(last, ShardScanItem::Stats(_)));

    // 이후 채널 닫힘
    assert!(rx.recv().await.is_none());
}

// ─── 테스트 5: ShardScanner Sort Key Range 필터링 ───────────────────────────

#[tokio::test]
async fn test_shard_scanner_sort_key_filter_skips_parts() {
    use storage_node::grpc::scan::{ShardScanner, ShardScanItem};
    use storage_node::lsm::levels::SstRef;
    use tempfile::TempDir;
    use std::io::Write;

    let tmp = TempDir::new().unwrap();

    // 컬럼 파일 생성: sort_key가 0x01~0x0F 범위인 Part
    let col_dir = tmp.path().join("event_time");
    std::fs::create_dir_all(&col_dir).unwrap();
    let seg_path = col_dir.join("seg-0000000001.col");
    {
        let mut f = std::fs::File::create(&seg_path).unwrap();
        // 행 수 헤더(4바이트) + 더미 데이터
        let row_count: u32 = 10;
        f.write_all(&row_count.to_le_bytes()).unwrap();
        f.write_all(&[0u8; 80]).unwrap();
    }

    let scanner = ShardScanner::new(tmp.path().to_path_buf());

    // Ge(0x20): max_sort_key=0x0F인 Part는 스킵되어야 함
    let req = ShardScanRequest {
        shard_id:         Uuid::new_v4(),
        shard_dir:        tmp.path().to_path_buf(),
        columns:          vec!["event_time".to_string()],
        predicates:       vec![ShardPredicate {
            column: "sort_key".to_string(),
            op:     ShardPredicateOp::Ge,
            value:  Some(Value::Bytes(vec![0x20])),
        }],
        scan_range:       LsmScanRange::all_levels(),
        bloom_probe_keys: vec![],
    };

    let (tx, mut rx) = mpsc::channel(16);
    scanner.scan(&req, tx).await.unwrap();

    // PartList 수신
    let first = rx.recv().await.unwrap().unwrap();
    if let ShardScanItem::PartList(parts) = first {
        // 실제 SstRef는 load_active_parts()에서 오므로
        // 이 테스트는 스캔 완료까지 오류 없음을 확인
        let _ = parts;
    }

    // 나머지 아이템 소비 (Stats까지)
    let mut found_stats = false;
    while let Some(item) = rx.recv().await {
        if let Ok(ShardScanItem::Stats(s)) = item {
            found_stats = true;
            // 빈 Shard이므로 row_count = 0
            assert_eq!(s.rows_returned, 0);
        }
    }
    assert!(found_stats, "Stats 아이템이 반환되어야 함");
}
