# Tasks: WOW-DB 웹 분석 데이터베이스 플랫폼

**Input**: `specs/002-wow-db-srs-v02/` (spec.md, plan.md, research.md, data-model.md, contracts/)  
**Branch**: `002-wow-db-srs-v02`

## Format: `[ID] [P?] [Story?] Description with file path`

- **[P]**: 다른 파일, 의존성 없음 — 병렬 실행 가능
- **[Story]**: 해당 사용자 스토리 (US1~US6)

---

## Phase 1: Setup — 프로젝트 초기화

**목적**: Cargo workspace 구조, proto, Docker 설정 생성

- [x] T001 Cargo workspace root 생성 (`Cargo.toml` — members: query-node, compute-node, storage-node, shared, integration-tests)
- [x] T002 `query-node/Cargo.toml` 생성 (tokio, tonic, prost, openraft, opensrv-mysql, sqlparser, axum, rdkafka, serde 의존성)
- [x] T003 [P] `compute-node/Cargo.toml` 생성 (tokio, tonic, prost, arrow2, serde 의존성)
- [x] T004 [P] `storage-node/Cargo.toml` 생성 (tokio, tokio-uring, tonic, prost, object_store, opendal, lz4_flex, zstd, crc32fast, bytes 의존성)
- [x] T005 [P] `shared/Cargo.toml` 생성 (prost, thiserror, uuid, chrono, arrow2, serde 의존성)
- [x] T006 [P] `integration-tests/Cargo.toml` 생성 (tokio, mysql_async, reqwest 의존성, `features = ["integration"]`)
- [x] T007 `proto/compute.proto` 작성 (FragmentRequest, FragmentResult, StageMetrics, CancelRequest)
- [x] T008 [P] `proto/storage.proto` 작성 (ScanRequest, ScanBatch, WriteRequest, Prepare/Commit/Rollback, RuntimeFilter)
- [x] T009 [P] `proto/raft.proto` 작성 (MetaService: CreateTable, AlterTable, GetStats, AllocateTablets 등)
- [x] T010 [P] `proto/ingestion.proto` 작성 (RoutineLoad CRUD, StreamLoad Begin/Commit/Abort)
- [x] T011 [P] `proto/health.proto` 작성 (HealthService Check, HealthRequest, HealthResponse)
- [x] T012 `query-node/build.rs`, `compute-node/build.rs`, `storage-node/build.rs` 생성 (prost/tonic 코드 생성 설정)
- [x] T013 `docker/Dockerfile.query-node` 작성 (4단계: chef→planner→builder→runtime, debian:bookworm-slim 최종 이미지)
- [x] T014 [P] `docker/Dockerfile.compute-node` 작성 (cargo-chef 멀티스테이지)
- [x] T015 [P] `docker/Dockerfile.storage-node` 작성 (cargo-chef 멀티스테이지)
- [x] T016 `docker/docker-compose.yml` 작성 (QN×3, CN×2, SN×3, MinIO, Redpanda, 헬스체크, service_healthy 의존성 체인)
- [x] T017 [P] `docker/docker-compose.dev.yml` 작성 (단일 QN/CN/SN + MinIO, 개발용 포트 노출)
- [x] T018 [P] `docker/configs/query-node.toml`, `compute-node.toml`, `storage-node.toml` 설정 템플릿 작성

**체크포인트**: `cargo build --workspace` 오류 없이 통과, `docker compose --profile infra up` 성공

---

## Phase 2: Foundational — 핵심 기반 인프라

**목적**: 모든 사용자 스토리가 시작되기 전 완료해야 하는 공통 기반  
**⚠️ CRITICAL**: 이 Phase 완료 전까지 어떤 User Story 작업도 시작 불가

- [x] T019 `shared/src/types.rs` 구현 (DataType, Value, SortKey, ColumnRef, NodeId, Timestamp, Partition, TabletRef)
- [x] T020 [P] `shared/src/error.rs` 구현 (thiserror 기반 WowDbError 열거형 — Parse, Storage, Network, NotFound, Conflict)
- [x] T021 [P] `shared/src/codec.rs` 구현 (Arrow2 IPC RecordBatch 직렬화/역직렬화 — encode_batch/decode_batch)
- [x] T022 [P] `shared/src/node_id.rs` 구현 (NodeId, ClusterId, generate UUID 유틸)
- [x] T023 `storage-node/src/lsm/wal.rs` 구현 (Append-only WAL 세그먼트, CRC32 체크섬, 세그먼트 로테이션, 크래시 복구 replay)
- [x] T024 `storage-node/src/lsm/memtable.rs` 구현 (Skip-list MemTable, Arc+RwLock, Sort Key 기준 정렬 삽입, 임계값 초과 시 Immutable 전환)
- [x] T025 `storage-node/src/lsm/sstable.rs` 구현 (MemTable→컬럼별 독립 파일 flush, `.col`/`.bloom`/`.min_max` 파일 포맷)
- [x] T026 `storage-node/src/backend/native.rs` 구현 (로컬 파일시스템 I/O — `cfg(target_os="linux")` io_uring, 그 외 `tokio::fs` fallback)
- [x] T027 `storage-node/src/grpc/server.rs` 구현 (tonic gRPC 서버 스켈레톤 — Health, WriteRows, ScanTablet, Prepare/Commit/Rollback)
- [x] T028 `compute-node/src/executor/pipeline.rs` 구현 (비동기 파이프라인 실행기 스켈레톤 — operator chain, backpressure 채널)
- [x] T029 `compute-node/src/grpc/server.rs` 구현 (tonic gRPC 서버 — ExecuteFragment, CancelFragment, Health)
- [x] T030 `query-node/src/raft/mod.rs` 구현 (openraft 통합 — StateMachine, Log Storage, Network, QN Raft 클러스터 초기화)
- [x] T031 `query-node/src/meta/table.rs` 구현 (TableSchema CRUD via Raft KV — create/get/list/delete)
- [x] T032 `query-node/src/meta/tablet.rs` 구현 (Tablet 할당, Tablet → SN 매핑 관리)
- [x] T033 `query-node/src/mysql_protocol/server.rs` 구현 (opensrv-mysql 기반 서버 스켈레톤 — 포트 9030, 연결 수락, 쿼리 디스패치)
- [x] T034 `query-node/src/sql_parser/mod.rs` 구현 (sqlparser-rs MySQL 방언 파서 통합, 커스텀 WOW-DB AST 노드 뼈대 정의)

**체크포인트**: 각 노드 `cargo test -p <node>` 단위 테스트 통과, gRPC 헬스체크 응답 확인

---

## Phase 3: US1 — 웹 이벤트 분석 쿼리 (Priority: P1) 🎯 MVP

**목표**: MySQL 클라이언트에서 FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS 쿼리를 제출하면 WOW-DB가 QN→CN→SN 파이프라인을 통해 결과를 반환한다.

**독립 테스트**: `mysql -h 127.0.0.1 -P 9030` 접속 후 CREATE TABLE, INSERT, FUNNEL_COUNT 쿼리 실행 → 결과 반환 확인

### Storage Node — 컬럼 스토리지

- [x] T035 [P] [US1] `storage-node/src/columnar/encoding.rs` 구현 (Dictionary Encoding, Delta Encoding, BitPacking, Plain 인코딩 선택 로직)
- [x] T036 [P] [US1] `storage-node/src/columnar/compression.rs` 구현 (LZ4, ZSTD 압축/해제 래퍼)
- [x] T037 [P] [US1] `storage-node/src/lsm/bloom.rs` 구현 (per-SSTable Bloom Filter — false positive rate 0.1%, Sort Key point lookup)
- [x] T038 [US1] `storage-node/src/lsm/compaction.rs` 구현 (Leveled Compaction — L0→L1, 파티션 경계 내에서만 Merge, 백그라운드 tokio task)
- [x] T039 [P] [US1] `storage-node/src/block_cache.rs` 구현 (LRU 블록 캐시 — SSTable 컬럼 블록 단위, 크기 설정 가능)
- [x] T040 [P] [US1] `storage-node/src/index/minmax.rs` 구현 (per-Granule MINMAX 인덱스 — 8,192행 단위, CBO predicate pushdown 지원)
- [x] T041 [P] [US1] `storage-node/src/index/bloom_index.rs` 구현 (per-Granule BLOOM_FILTER 인덱스)
- [x] T042 [US1] `storage-node/src/grpc/scan.rs` 구현 (ScanTablet 전체 구현 — 컬럼 투영, predicate 적용, Data Skipping 인덱스 활용, RuntimeFilter 수신)

### Compute Node — SIMD 실행 엔진

- [x] T043 [US1] `compute-node/src/executor/simd/scan.rs` 구현 (AVX2 컬럼 스캔 — `std::arch::x86_64`, CPUID 런타임 디스패치, _mm256_maskload_epi32 + prefetch)
- [x] T044 [P] [US1] `compute-node/src/executor/simd/filter.rs` 구현 (AVX2 predicate 필터 — _mm256_cmpeq_epi32 + _mm256_packs_epi32 비트마스크 패킹)
- [x] T045 [P] [US1] `compute-node/src/executor/simd/agg.rs` 구현 (AVX-512 64비트 누산 집계 — SUM/COUNT/MIN/MAX, conflict detection)
- [x] T046 [US1] `compute-node/src/executor/simd/hash_join.rs` 구현 (AVX2 Hash Build/Probe — 버킷 prefetch, scatter/gather, Partitioned Hash Join)
- [x] T047 [US1] `compute-node/src/executor/vectorized_agg.rs` 구현 (GROUP BY 벡터화 집계 — Arrow2 batch 단위 처리)
- [x] T048 [P] [US1] `compute-node/src/executor/sort.rs` 구현 (외부 정렬, Merge Sort, Top-K)
- [x] T049 [US1] `compute-node/src/runtime_filter.rs` 구현 (Build Side Bloom/InList/MinMax Filter 생성 → Probe Side CN 및 SN에 gRPC로 전파)
- [x] T050 [US1] `compute-node/src/shuffle.rs` 구현 (CN 간 데이터 교환 — Shuffle / Broadcast / Gather, Arrow IPC 포맷)
- [x] T051 [US1] `compute-node/src/executor/analytics/funnel.rs` 구현 (FUNNEL_COUNT 실행기 — 비트마스크 Step 달성 추적, Time Window 필터, 사용자별 집계)
- [x] T052 [P] [US1] `compute-node/src/executor/analytics/cohort.rs` 구현 (COHORT_ANALYSIS 실행기 — Entry 이벤트 기준 코호트 그룹화, 기간별 retention 계산)
- [x] T053 [P] [US1] `compute-node/src/executor/analytics/path.rs` 구현 (PATH_ANALYSIS 실행기 — 이벤트 시퀀스 패턴 카운팅, Top-N 경로 반환)

### Query Node — 쿼리 플래닝 및 실행

- [x] T054 [US1] `query-node/src/sql_parser/analytics.rs` 구현 (FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS 커스텀 AST 노드 및 파서 확장)
- [x] T055 [US1] `query-node/src/planner/logical.rs` 구현 (AST → LogicalPlan 변환 — Scan, Filter, Aggregate, Join, FunnelAnalysis, CohortAnalysis, PathAnalysis)
- [x] T056 [US1] `query-node/src/planner/cbo/stats.rs` 구현 (컬럼 통계 수집/저장 — row_count, min/max, NDV HyperLogLog, null_count)
- [x] T057 [US1] `query-node/src/planner/cbo/optimizer.rs` 구현 (파티션 Pruning, Join 순서 최적화, 집계 전략 선택 — 통계 기반)
- [x] T058 [US1] `query-node/src/planner/physical.rs` 구현 (LogicalPlan → PhysicalPlan Fragment 생성, CN 할당, Exchange 노드 삽입)
- [x] T059 [US1] `query-node/src/execution.rs` 구현 (QN 실행 오케스트레이터 — Fragment→CN 전송, 부분 결과 수집, 최종 병합, Profiler 기록)

**체크포인트**: `mysql -h 127.0.0.1 -P 9030`에서 FUNNEL_COUNT 쿼리 실행 → 결과 반환 확인

---

## Phase 4: US2 — 대용량 이벤트 데이터 수집 (Priority: P1)

**목표**: Kafka 토픽에서 이벤트를 실시간 수집하고, Spark 배치 및 MySQL INSERT로 데이터를 적재한다.

**독립 테스트**: Redpanda에 이벤트를 publish → 60초 이내 `SELECT COUNT(*) FROM page_events` 증가 확인

- [x] T060 [US2] `query-node/src/ingestion/kafka.rs` 구현 (rdkafka 기반 Routine Load — 컨슈머 그룹, 오프셋 추적, JSON/Avro 파싱, exactly-once ACK)
- [x] T061 [P] [US2] `query-node/src/sql_parser/routineload.rs` 구현 (CREATE/PAUSE/RESUME/STOP ROUTINE LOAD 문법 파싱)
- [x] T062 [US2] `query-node/src/transaction.rs` 구현 (2PC Transaction Manager — TxID 발급, Prepare/Commit/Rollback 코디네이션)
- [x] T063 [US2] `compute-node/src/ingestion.rs` 구현 (Row-to-Columnar 변환 — Kafka/INSERT 배치 → Arrow2 RecordBatch, 파티션 라우팅)
- [x] T064 [US2] `storage-node/src/grpc/write.rs` 구현 (WriteRows 완전 구현 — WAL append, MemTable insert, TxID 연결)
- [x] T065 [P] [US2] `storage-node/src/transaction.rs` 구현 (SN 측 2PC — Prepare 상태 유지, Commit 적용, Rollback 취소)
- [x] T066 [US2] `query-node/src/ingestion/spark.rs` 구현 (axum HTTP Stream Load 엔드포인트 — 포트 8040, TxID 수신, 컬럼 데이터 전달, Commit)
- [x] T067 [US2] `query-node/src/ingestion/async_insert.rs` 구현 (Async INSERT Buffer — QN 메모리 누적, 임계값/타임아웃 도달 시 배치 플러시, 종료 시 데이터 보존)
- [x] T068 [US2] `storage-node/src/partition.rs` 구현 (Auto Partition — INSERT 시 매핑 파티션 부재 시 QN에 파티션 생성 요청, 일/월/연 시간 단위)

**체크포인트**: Redpanda → Routine Load → 쿼리 가능 end-to-end 흐름 확인, `SHOW ROUTINE LOAD` 상태 조회

---

## Phase 5: US4 — 이벤트 스키마(Table) 정의 (Priority: P2)

**목표**: CREATE TABLE, ALTER TABLE, PARTITION, Storage Backend, Flat JSON 등 전체 DDL 지원

**독립 테스트**: CREATE TABLE (JSON 컬럼 포함) → INSERT → Compaction 후 Flat JSON 컬럼 쿼리 확인

- [x] T069 [US4] `query-node/src/sql_parser/table_ddl.rs` 구현 (전체 CREATE TABLE 문법 — PARTITION BY RANGE, AUTO PARTITION, ORDER BY, DISTRIBUTED BY HASH, COLOCATE WITH, STORAGE BACKEND)
- [x] T070 [P] [US4] `query-node/src/meta/alter_table.rs` 구현 (ALTER TABLE ADD/DROP COLUMN — 스키마 버전 관리, Raft 복제)
- [x] T071 [P] [US4] `query-node/src/meta/drop_table.rs` 구현 (DROP TABLE — Tablet 삭제 코디네이션, Raft 메타 정리)
- [x] T072 [US4] `storage-node/src/backend/s3.rs` 구현 (object_store 기반 S3/MinIO 백엔드 — SSTable PUT/GET/DELETE, 로컬 LRU 캐시 레이어)
- [x] T073 [US4] `storage-node/src/columnar/flat_json.rs` 구현 (Flat JSON 자동 컬럼 추출 — Compaction 시 key 출현율 분석, 임계값 이상 key → 독립 `.col` 파일, `_flat_meta.json` 갱신)
- [x] T074 [P] [US4] `storage-node/src/ttl.rs` 구현 (TTL 만료 — Compaction 시 파티션 단위 행 자동 삭제)
- [x] T075 [P] [US4] `query-node/src/meta/preagg_mv.rs` 구현 (CREATE MATERIALIZED VIEW AS SELECT GROUP BY 파싱 및 메타데이터 등록)
- [x] T076 [US4] `compute-node/src/mv_refresh.rs` 구현 (Pre-aggregation MV INSERT 시점/주기 갱신 실행기)
- [x] T077 [P] [US4] `storage-node/src/index/set_index.rs` 구현 (per-Granule SET 인덱스)
- [x] T078 [P] [US4] `storage-node/src/index/ngrambf.rs` 구현 (per-Granule NGRAMBF_V1 인덱스 — N-gram Bloom Filter, 텍스트 LIKE 쿼리 가속)

**체크포인트**: ALTER TABLE로 컬럼 추가 후 기존 데이터 쿼리 유지 확인, Flat JSON 컬럼 자동 생성 확인

---

## Phase 6: US3 — Web UI를 통한 세션 뷰 생성 (Priority: P2)

**목표**: 브라우저에서 Table 선택 → User Key 지정 → Timeout 설정 → DDL 미리보기 → SMV 생성 완료

**독립 테스트**: `http://localhost:8080` 접속 → SMV 마법사 → 생성 완료 후 샘플 세션 데이터 표시 확인

- [x] T079 [US3] `query-node/src/sql_parser/smv_ddl.rs` 구현 (CREATE SESSION MATERIALIZED VIEW 문법 파싱 — FROM, USER KEY, SESSION TIMEOUT, REFRESH)
- [x] T080 [US3] `query-node/src/session_mv/manager.rs` 구현 (SMV 라이프사이클 — 생성/삭제, 갱신 스케줄 관리, 구체화 진행 상태 추적)
- [x] T081 [US3] `compute-node/src/analytics/sessionize.rs` 구현 (SMV 구체화 실행기 — User Key 기준 이벤트 그룹화, Timeout 기반 세션 경계 결정, session_id UUID 생성)
- [x] T082 [US3] `query-node/src/web_ui/server.rs` 구현 (axum HTTP/WebSocket 서버 — 포트 8080, 정적 파일 서빙, API 라우팅)
- [x] T083 [P] [US3] `query-node/src/web_ui/api.rs` 구현 (Web UI REST API — Table 목록, 컬럼 목록, Table 생성 엔드포인트)
- [x] T084 [US3] `query-node/src/web_ui/smv_wizard.rs` 구현 (SMV 마법사 API — User Key 드롭다운, 타임아웃 프리셋, DDL 미리보기 생성, 확정 시 SMV 생성 실행)
- [x] T085 [P] [US3] `query-node/src/web_ui/sql_editor.rs` 구현 (Web SQL 에디터 — SQL 실행, 결과 스트리밍, 쿼리 히스토리 표시)

**체크포인트**: Web UI에서 SMV 마법사 전체 흐름(선택→미리보기→생성→샘플) 확인

---

## Phase 7: US5 — MySQL 클라이언트 호환성 (Priority: P2)

**목표**: MySQL Workbench, JDBC, Python mysql-connector가 드라이버 수정 없이 연결·쿼리 성공

**독립 테스트**: `mysql-connector-python`으로 접속 후 `SHOW TABLES; SELECT * FROM page_events LIMIT 10;` 성공 확인

- [x] T086 [US5] `query-node/src/mysql_protocol/handler.rs` 구현 (opensrv-mysql 완전 구현 — MySQL 8.0 handshake, 인증, COM_QUERY, COM_STMT_PREPARE 처리)
- [x] T087 [P] [US5] `query-node/src/mysql_protocol/result_set.rs` 구현 (MySQL 결과셋 직렬화 — ColumnDef 패킷, Row 패킷, EOF 패킷 표준 포맷)
- [x] T088 [P] [US5] `query-node/src/mysql_protocol/schema_cmds.rs` 구현 (SHOW TABLES, SHOW DATABASES, DESCRIBE, INFORMATION_SCHEMA 가상 테이블)
- [x] T089 [US5] `query-node/src/sql_parser/mysql_compat.rs` 구현 (MySQL 8.0 호환 DDL/DML 처리 — SET, USE, SHOW VARIABLES, Prepared Statement)

**체크포인트**: `mysql -h 127.0.0.1 -P 9030 -u admin -p`, Python `mysql.connector.connect()`, JDBC URL 각각 연결 성공

---

## Phase 8: US6 — 클러스터 모니터링 및 쿼리 프로파일링 (Priority: P3)

**목표**: Web UI 대시보드에서 노드 상태 확인, Query Profiler에서 느린 쿼리 병목 파악, Prometheus 스크레이핑

**독립 테스트**: `http://localhost:8080/profiler`에서 최근 쿼리 목록·단계별 소요 시간 확인

- [x] T090 [US6] `query-node/src/profiler.rs` 구현 (Query Profiler — Circular Buffer 1,000건, query_id/SQL text/시작시각/단계별 메트릭/총 소요시간 기록)
- [x] T091 [P] [US6] `query-node/src/monitoring.rs` 구현 (Prometheus `/metrics` 엔드포인트 — 노드 상태, 쿼리 처리량, 수집 속도, 메모리/CPU 사용률)
- [x] T092 [P] [US6] `query-node/src/web_ui/monitoring.rs` 구현 (Web UI 모니터링 대시보드 API — 전체 노드 상태, 역할, 리소스 사용률)
- [x] T093 [US6] `query-node/src/resource_group.rs` 구현 (Resource Group 정책 적용 — CPU/메모리/동시 쿼리 수/타임아웃 제한, 사용자/롤 매핑)

**체크포인트**: Prometheus `curl http://localhost:8080/metrics` 응답 확인, Web UI Profiler에서 최근 10개 쿼리 표시 확인

---

## Phase 9: 고급 기능 (Advanced Features)

**목적**: Spec FR 중 핵심 경로 이후에 구현 가능한 고급 기능

- [x] T094 [P] `storage-node/src/backend/hdfs.rs` 구현 (opendal + hdfs-native-client HDFS 백엔드 — Kerberos GSSAPI 인증, keytab 자동 갱신)
- [x] T095 [P] `storage-node/src/tiering.rs` 구현 (Tiered Storage — Hot(NVMe)→Cold(S3) 자동 이동, 파티션 age 기반 트리거)
- [x] T096 [P] `query-node/src/meta/global_dict.rs` 구현 (Global Dictionary — 저기수 문자열 컬럼 클러스터 전체 공유 정수 사전, QN Raft KV 저장)
- [x] T097 [P] `compute-node/src/result_cache.rs` 구현 (Query Result Cache — 동일 LogicalPlan + 파티션 버전 기준 Tablet 단위 집계 결과 CN 메모리 캐시)
- [x] T098 [P] `query-node/src/meta/external_table.rs` 구현 (External Table — S3, HDFS, Iceberg/Hive Metastore 가상 테이블 메타데이터 등록)
- [x] T099 [P] `query-node/src/meta/colocate.rs` 구현 (Colocate Group — 동일 분산 키/버킷 수 Table 동일 SN 버킷 배치, 그룹 내 Join 네트워크 Shuffle 제거)

---

## Phase 10: Polish & 통합 테스트

**목적**: 전체 스택 통합 검증, 성능 벤치마크

- [x] T100 `integration-tests/tests/ddl_tests.rs` 구현 (CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE SESSION MV, CREATE MV DDL 통합 테스트)
- [x] T101 [P] `integration-tests/tests/ingestion_tests.rs` 구현 (Kafka Routine Load, Spark Stream Load, MySQL INSERT, Async INSERT Buffer 통합 테스트)
- [x] T102 [P] `integration-tests/tests/analytics_tests.rs` 구현 (FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS 쿼리 정확성 통합 테스트)
- [x] T103 [P] `integration-tests/tests/compat_tests.rs` 구현 (mysql-connector-python, JDBC, MySQL CLI 연결 및 쿼리 호환성 테스트)
- [x] T104 [P] `integration-tests/tests/failover_tests.rs` 구현 (SN 단일 장애 복구, QN Raft Leader 전환, 진행 중 쿼리 재시도 시나리오)
- [x] T105 `integration-tests/benches/throughput.rs` 구현 (수집 처리량 벤치마크 — SC-001: 200억 레코드 목표, SC-005: 60초 수집 레이턴시)
- [x] T106 [P] `quickstart.md` 기반 로컬 검증 실행 (`docker compose up → CREATE TABLE → INSERT → FUNNEL 쿼리` 전체 플로우 smoke test)

---

## Phase 11: LSM Engine 강화 (LSM Hardening)

**목적**: LSM 상세 설계(`design/lsm-engine.md`)에서 도출된 미구현 사항 및 스펙 누락 항목 구현  
**근거**: FR-026~FR-032, 2026-04-14 LSM 전문가 검토 결과 반영  
**선행 조건**: Phase 2 (T023~T025 WAL/MemTable/SSTable 기반 구현) 완료 후 시작

### Storage Node — 멀티레벨 Compaction

- [x] T107 `storage-node/src/lsm/levels.rs` 구현 (LevelState 구조체 — L0~L6, 레벨별 max_bytes = 256MB × 10^(level-1), CompactionConfig, compaction_score 계산, L1+ non-overlapping 불변 조건 런타임 검증)
- [x] T108 `storage-node/src/lsm/compaction.rs` 전면 수정 (현재 L0→L1만 구현된 것을 멀티레벨로 확장 — L0→L1→L2→LN Leveled Compaction, least-recently-compacted 파일 선택, target_file_size 분할, 입력 SSTable list에서 key range overlap 선택 로직)
- [x] T109 `storage-node/src/lsm/compaction.rs` TTL 통합 수정 (Compaction 시 행 단위 TTL 필터링 — `ttl_column_value < now() - ttl_duration` 조건으로 만료 행 출력 제외, FR-027 파티션 경계 위반 방어 코드 추가)
- [x] T110 `storage-node/src/lsm/manifest.rs` 구현 (MANIFEST 파일 원자적 관리 — 활성 SSTable 전체 목록 직렬화/역직렬화, Compaction 완료 시 atomic rename 갱신, 크래시 복구 시 MANIFEST 기반 복원, FR-032)

### Storage Node — SSTable 물리 파일 포맷

- [x] T111 `storage-node/src/lsm/sstable.rs` 수정 (물리 파일 포맷 v1 구현 — `.col` 헤더: magic "WOWDBCOL" + VERSION(4B) + SST_SEQUENCE(8B) + GRANULE_OFFSET_TABLE + FOOTER CRC32, `.bloom` 헤더: magic "WOWBLOOM" + VERSION + BITS_PER_KEY + HASH_FN, `.min_max` 헤더: magic "WOWMINMX", 버전 불일치 시 로딩 거부, FR-031)
- [x] T112 `storage-node/src/lsm/sstable.rs` sequence_num 및 generation 필드 추가 (SstMeta에 sequence_num: u64, generation: u64, compacted_from: Vec<Uuid> 추가, 파티션 내 전역 AtomicU64로 단조 증가 sequence_num 할당)

### Storage Node — Bloom Filter 파라미터

- [x] T113 [P] `storage-node/src/lsm/bloom.rs` 수정 (BloomFilterConfig 구조체 도입 — xxHash3 해시 함수, bits_per_key 설정 가능(10/14), k=bits_per_key×ln(2) 해시 함수 수 계산, Compaction 시 출력 레코드 기반 bloom 재생성 필수, SSTable-level bloom + Granule-level bloom 분리 명확화, FR-029)

### Storage Node — Block Cache 분리

- [x] T114 [P] `storage-node/src/block_cache.rs` 수정 (3-Pool 분리 — DataBlockPool(70%)/FilterBlockPool(20%)/IndexBlockPool(10%), FilterBlock 우선 보존 정책: eviction 시 Data보다 Filter 블록 보호, 전체 캐시 크기 설정 기반 각 풀 크기 계산)

### Storage Node — WAL Group Commit

- [x] T115 [P] `storage-node/src/lsm/wal.rs` 수정 (Group Commit 구현 — WalWriter에 pending_writes 버퍼 추가, flush_interval(기본 4ms)/max_batch_bytes(기본 4MB) 임계값 도달 시 단일 write+fdatasync, 세그먼트 retention 정책: flush 완료 후 연관 WAL 세그먼트 삭제)

### Query Node — Sort Key DDL 검증

- [x] T116 `query-node/src/sql_parser/table_ddl.rs` 수정 (Sort Key 검증 강제 — ORDER BY 컬럼 수 > 4 이면 DDL 에러, 직렬화 크기 > 128 bytes 이면 DDL 에러, JSON 타입 컬럼 Sort Key 사용 시 에러, STRING 컬럼 > 64 bytes 경고 메시지, FR-026)

### Integration Tests — LSM 동작 검증

- [x] T117 `storage-node/tests/lsm_behavior_tests.rs` 구현 (LSM 정확성 및 성능 검증 — ① L1+ key range non-overlapping 불변 조건 검증, ② Bloom FPR 측정: 비존재 key 10,000개 조회 시 FPR ≤ 2% (이론값 1%), ③ Sort Key 제약 불변 조건, ④ Full Compaction 후 L0 공백, ⑤ MANIFEST 크래시 복구)

**체크포인트**: `cargo test -p storage-node -- lsm` 전체 통과, L0→L1→L2 멀티레벨 compaction 로그 확인

---

## Phase 12: 클러스터 보호 — Metadata 일관성 및 디스크 보호 (FR-033~FR-036)

**목적**: 모든 QN이 동일한 메타데이터를 제공하는 단일 시스템 이미지 보장, 디스크 용량 초과 시 데이터 손실 방지  
**참조**: `specs/002-wow-db-srs-v02/design/readonly-mode.md`, FR-033~FR-036

### Query Node — 단일 시스템 이미지 (FR-033, FR-034)

- [x] T118 `query-node/src/meta/cache.rs` 구현 (MetadataCacheEntry — Raft 인덱스 기반 캐시 무효화, DDL 변경은 write-through 즉시 무효화, CBO 통계는 max_staleness_ms=500ms 후 re-fetch, 버전 불일치 시 Raft Leader에서 강제 재조회)
- [x] T119 [P] `query-node/src/meta/session_store.rs` 구현 (WebSession — Raft KV `/sessions/{token}` CRUD, UUID v4 토큰 발급, 24시간 TTL, 모든 QN에서 동일 토큰 검증 가능, QN 재시작 후 세션 유지, FR-035)

### Storage Node — 디스크 용량 감시 (FR-036)

- [x] T120 `storage-node/src/disk_monitor.rs` 구현 (DiskMonitor — statvfs/GetDiskFreeSpaceEx 기반 디스크 사용량 폴링, 5초 주기, disk_full_threshold=0.95 초과 시 QN Leader에 ReportDiskFull gRPC 전송, disk_recovery_threshold=0.85 이하 시 ReportDiskRecovered gRPC 전송, DiskMonitorConfig 설정 가능)
- [x] T121 [P] `query-node/src/disk_monitor.rs` 구현 (Query Node 전용 DiskMonitor — Raft WAL 디스크 및 스냅샷 디스크 모니터링, 동일 임계값 적용)

### Query Node — Read-Only 상태 관리 (FR-036)

- [x] T122 `query-node/src/meta/cluster_guard.rs` 구현 (ClusterGuard — ClusterReadOnlyState Raft KV 직렬화/역직렬화, add_reason/remove_reason으로 다중 원인 관리, check_write_allowed() 메서드, ReadOnlyError → MySQL ER_OPTION_PREVENTS_STATEMENT(1290) 변환)
- [x] T123 `query-node/src/mysql_protocol/` 수정 (쓰기 요청 처리 전 ClusterGuard.check_write_allowed() 호출 추가 — INSERT/DDL/LOAD DATA 핸들러, Read-Only 시 ERROR 1290 HY000 반환, SELECT/SHOW/DESCRIBE는 통과)
- [x] T124 [P] `proto/meta.proto` 수정 (MetaService에 ReportDiskFull, ReportDiskRecovered RPC 추가, DiskCapacityReport 메시지 정의 — node_id, path, usage_ratio 필드)

### Integration Tests — 디스크 보호 검증

- [x] T125 `storage-node/tests/disk_full_tests.rs` 구현 (Read-Only 모드 통합 테스트 — ① 95% 임계값 초과 시 ClusterReadOnlyState.enabled=true 검증, ② Read-Only 상태에서 INSERT 거부 및 SELECT 허용 검증, ③ 85% 이하 복구 시 정상 모드 자동 전환 검증, ④ 복수 노드 DiskFull 시 모든 노드 복구 후 해제 검증)

**체크포인트**: `cargo test -p storage-node -- disk_full` 전체 통과, Prometheus `wowdb_cluster_read_only` 메트릭 정상 노출

---

## Phase 13: 쿼리 실행 단위 및 논리-물리 매핑 (FR-037~FR-042)

**목적**: QN 논리 단위(Table/Partition/Shard)와 SN 물리 단위(Part/Granule/ColumnFile)의 계층 확립, 계층별 CBO 통계 저장, CN-SN 간 Shard 스캔 프로토콜 구현  
**참조**: `specs/002-wow-db-srs-v02/design/logical-to-physical-mapping.md`, `design/query-execution-model.md`

### Shared — 통계 타입 정의 [P]

- [x] T126 [P] `shared/src/types.rs` 확장 (논리-물리 단위 타입 추가 — TableStats, PartitionStats, ShardStats, ColumnStats, Histogram, PartRef 구조체, LsmScanRange, ScanStats, ShardScanRequest 타입 정의, FR-037~FR-040)

### Proto — Shard 스캔 프로토콜 [P]

- [x] T127 [P] `proto/storage.proto` 확장 (ShardScanRequest/Response 메시지 추가 — shard_id, columns, predicates, scan_range(min_level/max_level), runtime_filter, bloom_probe_keys 필드; 스트리밍 응답: PartListResponse(PartMeta 목록), RecordBatchChunk, ScanStats; FR-041, FR-042)

### Query Node — Raft KV 통계 계층

- [x] T128 `query-node/src/meta/stats.rs` 구현 (CBO 통계 계층적 저장 — Raft KV CRUD: /stats/{table_id}, /stats/{table_id}/{partition_id}, /stats/{table_id}/{partition_id}/{shard_id}; MemTable Flush/Compaction 완료 시 SN이 QN에 보고 → Raft KV 증분 갱신; ANALYZE TABLE 시 ShardStats 전체 재계산; FR-040)
- [x] T129 [P] `query-node/src/meta/shard_map.rs` 구현 (Shard → SN 매핑 조회 — Raft KV /tablets/{shard_id}에서 ShardReplica 목록 조회, Leader replica 우선 선택, QN 로컬 캐시 TTL 500ms, DDL/Tablet 재배치 시 즉시 무효화, FR-039)

### Query Node — Physical Planner 확장

- [x] T130 `query-node/src/planner/physical.rs` 수정 (Fragment 생성에 ShardScan 포함 — ShardScan 구조체에 shard_id, sn_endpoint, columns(projection), predicates(pushdown), scan_range, runtime_filter 필드 추가; Shard → CN 할당: Data Locality → Load Balance → Colocate Group 우선순위; FR-041)
- [x] T131 [P] `query-node/src/planner/cbo/partition_prune.rs` 구현 (Partition Pruning — WHERE predicate와 PartitionStats.partition_key_min/max 비교로 범위 밖 Partition 조기 제거; Pruning 완료 후 해당 Partition의 Shard 목록 조회; FR-037)

### Compute Node — Shard Scan Executor

- [x] T132 `compute-node/src/executor/shard_scan.rs` 구현 (ShardScanStage 구현 — SN gRPC 엔드포인트에 ShardScanRequest 전송, PartListResponse 수신 후 CN 측 Part min/max Sort Key 필터링, RecordBatch 스트림 수신; 복수 Shard 병렬 스캔(tokio::spawn per shard); FR-041)
- [x] T133 [P] `compute-node/src/executor/pipeline.rs` 수정 (Fragment 파이프라인에 ShardScanStage 통합 — ShardScanStage → SIMDFilterStage → VectorizedAggStage → ExchangeStage 순서; Backpressure: tokio::mpsc 채널 크기로 CN 내 stage 간 배압 제어; FR-041)

### Storage Node — Shard 스캔 실행기

- [x] T134 `storage-node/src/grpc/scan.rs` 수정 (ShardScanRequest 처리 구현 — MANIFEST에서 활성 Part 목록 조회 후 PartListResponse 반환; Part별 min/max Sort Key 스킵; SSTable-Level Bloom Filter 검사(bloom_probe_keys); Granule MINMAX 스킵(.min_max 파일); 남은 Granule .col 파일 읽기 → Arrow2 RecordBatch 스트리밍; L0 Multi-Version Read(sequence_num 기반 중복 제거); ScanStats 최종 반환; FR-042)
- [x] T135 [P] `storage-node/src/stats_reporter.rs` 구현 (SN → QN 통계 보고 — MemTable Flush 완료 시 ShardStats 증분 갱신 값을 QN Leader에 gRPC 보고; Part min/max Sort Key를 MANIFEST에 기록; Compaction 완료 시 삭제/추가된 Part 반영한 통계 재계산 후 보고; FR-040)

### Integration Tests — 논리-물리 매핑 검증

- [x] T136 `storage-node/tests/logical_physical_mapping_tests.rs` 구현 (매핑 정확성 검증 — ① Part min/max Sort Key 기반 스킵 검증(predicate 범위 밖 Part는 0 rows 반환), ② Granule MINMAX 스킵 검증(스킵 카운터 ScanStats에 정확히 반영), ③ L0 Multi-Version Read 검증(동일 Sort Key에서 최신 sequence_num 값 우선), ④ Column Projection 검증(요청하지 않은 컬럼 파일 미개방), ⑤ Bloom Filter 스킵 통합 검증)

**체크포인트**: `cargo test -p storage-node -- logical_physical` 전체 통과, FragmentMetric의 parts_skipped > 0 확인 (스킵 최적화 동작 증명)

---

## Phase 14: 데이터 분포 가시성 — SHOW PARTITIONS / SHARDS / PARTS (FR-043~FR-046)

**목적**: 운영자와 데이터 엔지니어가 각 Table의 데이터 분산 현황(파티션 범위, Shard별 SN 배치, LSM Part 수준)을 MySQL 클라이언트로 직접 조회할 수 있는 가시성 명령 제공  
**참조**: FR-043, FR-044, FR-045, FR-046

### Proto — SN 파티션/Part 조회 API [P]

- [x] T137 [P] `proto/storage.proto` 확장 (GetPartList/GetShardInfo RPC 추가 — GetPartListRequest: shard_id, PartInfo 스트림 응답(part_id, level, seq_num, row_count, size_bytes, min_sort_key, max_sort_key, bloom_size_bytes); GetShardInfoRequest: shard_id, ShardInfoResponse(row_count, size_bytes, part_count, lsn); FR-045)

### Query Node — 파티션/Shard 메타 집계 서비스

- [x] T138 `query-node/src/meta/partition_info.rs` 구현 (PartitionInfoService — Raft KV에서 파티션·Shard 메타 집계: list_partitions(table_id) → Vec<PartitionMeta>, list_shards(table_id, partition_id?) → Vec<ShardMeta>, get_distributed_status(table_id) → Vec<SnDistributionSummary>; SN gRPC로 Part 목록 조회: list_parts(shard_id) → Vec<PartMeta>; FR-043~FR-046)

### Query Node — MySQL 프로토콜 SHOW 핸들러

- [x] T139 `query-node/src/mysql_protocol/schema_cmds.rs` 수정 (SHOW PARTITIONS FROM <table> 핸들러 추가 — partition_id, range_start/end, row_count, size_bytes, shard_count, part_count, tier, created_at 컬럼; FR-043)
- [x] T140 `query-node/src/mysql_protocol/schema_cmds.rs` 수정 (SHOW SHARDS FROM <table> [PARTITION <pid>] 핸들러 추가 — shard_id, partition_id, partition_range, sn_node_id, sn_endpoint, bucket_id, role, state, row_count, size_bytes, part_count, lsn 컬럼; FR-044)
- [x] T141 `query-node/src/mysql_protocol/schema_cmds.rs` 수정 (SHOW PARTS FROM <table> [PARTITION <pid>] [SHARD <sid>] / SHOW PARTS ON PARTITION <pid> FROM <table> 핸들러 추가 — part_id, shard_id, partition_id, sn_node_id, level, sequence_num, row_count, size_bytes, min_sort_key, max_sort_key, bloom_size_bytes, created_at 컬럼; FR-045)
- [x] T142 [P] `query-node/src/mysql_protocol/schema_cmds.rs` 수정 (SHOW DISTRIBUTED STATUS FROM <table> 핸들러 추가 — sn_node_id, sn_endpoint, shard_count, leader_shard_count, partition_count, row_count, size_bytes, avg_part_per_shard 컬럼; FR-046)

### Integration Tests — SHOW 명령 검증

- [x] T143 `query-node/src/mysql_protocol/schema_cmds.rs` 단위 테스트 추가 (SHOW PARTITIONS/SHARDS/PARTS/DISTRIBUTED STATUS 핸들러 — stub 데이터 기반 컬럼 수·이름 정확성 검증, 없는 Table에 대한 빈 결과셋 반환 검증)

**체크포인트**: MySQL 클라이언트에서 `SHOW PARTITIONS FROM page_events` 실행 시 올바른 컬럼 헤더와 결과셋 반환, `SHOW PARTS FROM page_events WHERE level = 0` 구문 파싱 정상 동작

---

## Phase 15: EXPLAIN — 분산 쿼리 실행 계획 출력 (FR-047~FR-048)

**목적**: `EXPLAIN <sql>`, `EXPLAIN VERBOSE <sql>`, `EXPLAIN COSTS <sql>` 명령으로 CBO가 생성하는 분산 실행 계획을 Fragment 단위로 MySQL 클라이언트에 출력. Colocate Join 여부를 EXPLAIN 출력에 명시적으로 표시하여 운영자가 Shuffle 비용을 즉시 파악할 수 있게 함.  
**참조**: FR-047, FR-048, `design/sql_syntax.md § 9.5`

### Query Node — Explain 핵심 모듈

- [x] T144 `query-node/src/planner/explain.rs` 신규 구현 (`ExplainNode` 열거형 — Scan, HashAggregate, QnMergeAgg, HashJoin, Exchange, FunnelAnalysis, ResultSink 변형; `ExplainPlan` 구조체 — Vec<ExplainFragment>; `ExplainMode` 열거형 — Basic, Verbose, Costs; `explain_sql(sql, mode)` 진입점 → `QueryOutput::Rows` 반환; FR-047)
- [x] T145 `query-node/src/planner/logical.rs` 확장 (LogicalPlan 노드에서 EXPLAIN용 메타 추출 — 테이블 이름, 프레디케이트 문자열, GROUP BY 컬럼 목록 노출; `LogicalPlan::describe() -> String` 메서드 추가)
- [x] T146 `query-node/src/planner/physical.rs` 확장 (PhysicalNode/Fragment에 EXPLAIN 메타 추가 — `ExchangeMode` → Shuffle 방식 문자열 변환 `to_explain_str()` 메서드; Fragment `explain_header()` 메서드 — `PLAN FRAGMENT N` 헤더 생성)

### Query Node — MySQL 프로토콜 핸들러 연결

- [x] T147 `query-node/src/mysql_protocol/schema_cmds.rs` 수정 (`handle_schema_command`에서 `EXPLAIN`, `EXPLAIN VERBOSE`, `EXPLAIN COSTS` 접두사 인식 → `explain::explain_sql()` 호출 → `QueryOutput::Rows` 반환; `Fragment_Id` + `Plan` 두 컬럼 반환; FR-047)

### Tests

- [x] T148 `query-node/src/planner/explain.rs` 단위 테스트 추가 (기본 EXPLAIN — Fragment 구조 검증: FRAGMENT 0에 `QN-MERGE` 포함, FRAGMENT 1에 `SCAN` 포함)
- [x] T149 EXPLAIN COSTS 테스트 추가 (WHERE 절 포함 쿼리 → `Partitions:` 행 및 `CBO: rows=` 행 존재 검증)
- [x] T150 EXPLAIN JOIN 테스트 추가 (JOIN 쿼리 → `HASH JOIN` 및 `BROADCAST` 또는 `HASH_SHUFFLE` 텍스트 포함 검증)
- [x] T151 EXPLAIN FUNNEL 테스트 추가 (FUNNEL_COUNT 포함 SQL → `FUNNEL ANALYSIS` Fragment 존재 검증)
- [x] T152 Colocate Join EXPLAIN 테스트 추가 (`[COLOCATE]` 태그 검증 — 분산 키 동일 + Colocate Group 힌트 포함 SQL 시뮬레이션; `shuffle=NONE` 텍스트 존재 검증; FR-048)

**체크포인트**: `cargo test -p query-node -- explain` 전체 통과, MySQL 클라이언트에서 `EXPLAIN SELECT * FROM page_events` 실행 시 `Fragment_Id` / `Plan` 두 컬럼으로 결과셋 반환

---

## Dependencies & Execution Order

### Phase 의존성

```
Phase 1: Setup          → 즉시 시작 가능
Phase 2: Foundational   → Phase 1 완료 후 시작 (모든 Phase 3+ 차단)
Phase 3: US1 (P1)       → Phase 2 완료 후 시작
Phase 4: US2 (P1)       → Phase 2 완료 후 시작 (Phase 3과 병렬 가능)
Phase 5: US4 (P2)       → Phase 2 완료 후 시작
Phase 6: US3 (P2)       → Phase 5 완료 후 시작 (Table DDL 필요)
Phase 7: US5 (P2)       → Phase 2 완료 후 시작 (MySQL Protocol 스켈레톤 기반)
Phase 8: US6 (P3)       → Phase 3 완료 후 시작 (Query 실행 흐름 완성 필요)
Phase 9: Advanced       → Phase 3~8 완료 후 시작
Phase 10: Polish        → Phase 3~8 완료 후 시작
```

### User Story 의존성

- **US1 (P1)**: Phase 2 이후 독립 시작 — 다른 Story에 의존 없음
- **US2 (P1)**: Phase 2 이후 US1과 병렬 시작 가능
- **US4 (P2)**: Phase 2 이후 독립 시작 — US3의 전제 조건
- **US3 (P2)**: US4 완료 후 시작 (SMV 생성에 Table DDL 필요)
- **US5 (P2)**: Phase 2 이후 독립 시작
- **US6 (P3)**: US1 완료 후 시작 (Query Profiler가 쿼리 실행 흐름 필요)

### Story 내 실행 순서

```
Storage (T035~T042) → Compute SIMD (T043~T050) → Compute Analytics (T051~T053) → QN Planning (T054~T059)
```

### 병렬 실행 기회

- Phase 1 전체 [P] 태스크: T003~T018 동시 실행 가능
- Phase 2 공통 기반: T020~T022 (shared 크레이트) 병렬
- US1 내 Storage 인코딩: T035~T037 병렬
- US1 내 SIMD 오퍼레이터: T044, T045, T048 병렬
- US1 내 Analytics: T052, T053 병렬
- Phase 3 (US1) + Phase 4 (US2): 팀 분리 시 병렬 진행 가능
- Phase 5 (US4) + Phase 7 (US5): 병렬 진행 가능

---

## 병렬 실행 예시: US1 Storage Node 구현

```bash
# 동시 시작 가능한 독립 태스크
Task A: T035 — storage-node/src/columnar/encoding.rs  (Dictionary/Delta/BitPacking)
Task B: T036 — storage-node/src/columnar/compression.rs (LZ4/ZSTD)
Task C: T037 — storage-node/src/lsm/bloom.rs (Bloom Filter)

# A, B, C 완료 후 순차 진행
Task D: T038 — storage-node/src/lsm/compaction.rs (A, B, C 모두 필요)
Task E: T042 — storage-node/src/grpc/scan.rs (D, 인덱스 완료 후)
```

## 병렬 실행 예시: US1 Compute Node SIMD

```bash
# 동시 시작 가능
Task A: T044 — compute-node/src/executor/simd/filter.rs
Task B: T045 — compute-node/src/executor/simd/agg.rs
Task C: T048 — compute-node/src/executor/sort.rs
Task D: T052 — compute-node/src/analytics/cohort.rs
Task E: T053 — compute-node/src/analytics/path.rs
```

---

## Implementation Strategy

### MVP First (US1 + US2 Only — Phase 1~4)

1. Phase 1 완료: workspace 구조 + Docker Compose
2. Phase 2 완료: 3-Node gRPC 파이프라인 기동
3. Phase 3 완료 (US1): FUNNEL/COHORT/PATH 쿼리 동작
4. Phase 4 완료 (US2): Kafka 수집 동작
5. **중간 검증**: `docker compose up`, 이벤트 수집, 분석 쿼리 실행
6. **배포/데모 가능 상태**

### Incremental Delivery

1. Phase 1+2 → 기반 준비 (docker compose up 성공)
2. Phase 3 (US1) → 분석 쿼리 MVP
3. Phase 4 (US2) → Kafka 실시간 수집 추가
4. Phase 5 (US4) → 전체 DDL + Flat JSON 추가
5. Phase 6 (US3) → Web UI SMV 마법사 추가
6. Phase 7 (US5) → MySQL 완전 호환 추가
7. Phase 8 (US6) → 모니터링 추가

### Parallel Team Strategy (3개 팀)

```
Phase 2 완료 후:
  팀 A: US1 (FUNNEL/COHORT/PATH) — Phase 3
  팀 B: US2 (Kafka/Spark 수집)  — Phase 4
  팀 C: US4+US5 (DDL + MySQL 호환) — Phase 5+7
```

---

---

## Phase 16: 로컬 개발 환경 — Native 단일 인스턴스 (NFR-DEV-001~006)

**목적**: Docker 없이 `cargo build` 후 QN×1+CN×1+SN×1을 5초 이내 기동하는 개발 스크립트와 설정 제공  
**상세 설계**: `design/local-dev-setup.md`

### 설정 파일 (완료)

- [x] T191 [P] `dev/configs/storage-node-local.toml` 구현 (native backend, data_dir=/tmp/wowdb-dev/sn, replica_count=1, memtable 32MB, block_cache 128MB, disk_full_threshold=0.98)
- [x] T192 [P] `dev/configs/compute-node-local.toml` 구현 (storage_nodes=["127.0.0.1:9060"], dop=2, hash_join_max_memory 512MB)
- [x] T193 [P] `dev/configs/query-node-local.toml` 구현 (단일 노드 Raft peers=["qn-local-1:9010"], compute_nodes=["127.0.0.1:9040"], rebalance_enabled=false)

### 실행 스크립트 — Linux/macOS (완료)

- [x] T194 `scripts/dev/run-local.sh` 구현 (SN→CN→QN 순서 기동, TCP 포트 오픈 대기, PID 파일 기록, Ctrl+C 시 cleanup trap, --no-build/--release 옵션)
- [x] T195 [P] `scripts/dev/stop-local.sh` 구현 (PID 파일 기반 종료, 파일 없을 시 lsof 포트 기반 fallback)
- [x] T196 [P] `scripts/dev/reset-local.sh` 구현 (실행 중 확인 후 데이터 디렉토리 삭제, 확인 프롬프트)

### 실행 스크립트 — Windows PowerShell (완료)

- [x] T197 `scripts/dev/run-local.ps1` 구현 (TcpClient 포트 대기, Start-Process 비동기 기동, finally 블록 정리, -NoBuild/-Release/-DataDir 파라미터)
- [x] T198 [P] `scripts/dev/stop-local.ps1` 구현 (PID 파일 기반 Stop-Process, Get-NetTCPConnection fallback)
- [x] T199 [P] `scripts/dev/reset-local.ps1` 구현 (-Force 플래그, 실행 중 감지 후 경고)

### 런타임 지원 (미구현 — 바이너리 --config 파라미터 필요)

- [ ] T200 `query-node/src/main.rs` 수정 (CLI 인수 파싱 — `--config <path>` 지원, 환경변수 오버라이드: NODE_ID/RAFT_PEERS/COMPUTE_NODES/QN_PEERS)
- [ ] T201 [P] `compute-node/src/main.rs` 수정 (CLI 인수 파싱 — `--config <path>` 지원, 환경변수 오버라이드: NODE_ID/STORAGE_NODES)
- [ ] T202 [P] `storage-node/src/main.rs` 수정 (CLI 인수 파싱 — `--config <path>` 지원, 환경변수 오버라이드: NODE_ID/DATA_DIR)

### 검증 테스트

- [ ] T203 `scripts/dev/run-local.sh` 검증 (기동 → MySQL 접속 → `SHOW TABLES` → `CREATE TABLE` → `INSERT` → `SELECT COUNT(*)` 전체 플로우 5초 이내 완료)
- [ ] T204 [P] `scripts/dev/reset-local.sh` + `run-local.sh` 반복 검증 (초기화 후 재기동 3회 연속 정상 동작)

**체크포인트**: `./scripts/dev/run-local.sh --no-build` 실행 후 5초 이내 `mysql -h 127.0.0.1 -P 9030` 접속 성공, `CREATE TABLE` DDL 실행 가능

---

## Phase 18: Behavioral Routing — 자동 쿼리 라우팅 (FR-NEW-001)

**목적**: Event Table에서 Behavioral Query(FUNNEL/COHORT/PATH) 실행 시 자동으로 Behavioral Table로 라우팅. BT pair가 없으면 경고 + 생성 권장 DDL 반환.
**상세 설계**: `design/query-routing-smv.md`, 관련 요구사항: FR-000, FR-NEW-001-01 ~ FR-NEW-001-11

### 기반 인프라

- [x] T153 [P] `query-node/src/planner/behavioral_pattern.rs` 구현 (QueryPattern 열거형 Behavioral/Event/Hybrid, BehavioralTrigger, `detect_pattern(ast)` — FUNNEL_COUNT/COHORT_ANALYSIS/PATH_ANALYSIS 함수명 및 session_id/session_start/event_sequence 컬럼 참조 감지)
- [x] T154 [P] `query-node/src/meta/bt_registry.rs` 구현 (BtRegistry — Event Table ↔ BT(Session MV) 페어링 메타데이터 관리, BtEntry/BtState, get_bt_for_table/register/update_state/get_active_bt 메서드, Stale 상태면 get_active_bt None 반환)
- [x] T155 `query-node/src/meta/table_manager.rs` 수정 (TableManager에 BtRegistry 통합 — CREATE SESSION MATERIALIZED VIEW DDL 처리 시 register_behavioral_table() 자동 호출, bt_registry()/bt_registry_mut() 메서드 추가)

### Behavioral Guidance

- [x] T156 [P] `query-node/src/planner/behavioral_guidance.rs` 구현 (BehavioralGuidance 구조체 — warning/suggested_ddl/estimated_speedup/actual_duration_ms 필드, build_guidance() — "No Behavioral Table found" 경고 + CREATE SESSION MV DDL 제안 + row_count 기반 예상 성능 향상 배수)
- [x] T157 `query-node/src/mysql_protocol/handler.rs` 수정 (Behavioral 패턴 감지 후 Active BT 없으면 Event Table Fallback 실행 + MySQL warnings 필드에 BehavioralGuidance.warning 첨부)
- [x] T158 `query-node/src/planner/behavioral_tests.rs` 구현 (Guidance 검증 테스트 3건: BT 없을 때 FUNNEL Query → Warning 포함, Event COUNT(*) → Warning 없음, BT pair 미등록 일반 테이블 → Warning 없음)

### Behavioral Router

- [x] T159 [P] `query-node/src/planner/behavioral_router.rs` 구현 (BehavioralRouter — LogicalPlan Rewriter, BT Registry에서 Active BT 조회, TableScan 노드 BT로 교체, RoutingResult: plan/routed/bt_used/guidance 필드)
- [x] T160 [P] `query-node/src/planner/behavioral_column_map.rs` 구현 (Column Mapping — event_time→session_start (WHERE/Projection 컨텍스트), user_id/device_id 동일 유지, map_column() 함수)
- [x] T161 `query-node/src/mysql_protocol/handler.rs` 수정 (Behavioral Router를 Logical Plan 생성 직후 CBO 이전에 실행 — 라우팅 성공 시 BT 스캔, 라우팅 실패 시 Fallback+Warning, 로그 기록)
- [x] T162 `query-node/src/planner/explain.rs` 수정 (EXPLAIN 출력에 "== Behavioral Routing ==" 섹션 추가 — 원본 테이블, 라우팅 대상 BT, 트리거, BT 마지막 갱신, 예상 speedup; BT 없으면 Guidance DDL 표시)

### 통합 검증 테스트

- [x] T163 `query-node/src/planner/behavioral_tests.rs` 구현 (D-001: BT 생성 후 FUNNEL_COUNT 자동 라우팅 — BT없음+Guidance→BT생성→라우팅→EXPLAIN확인→결과동등성)
- [x] T164 `query-node/src/planner/behavioral_tests.rs` 구현 (D-002: BT 생성 후 COHORT_ANALYSIS 자동 라우팅 — Warning 없음 + 결과 동등성)
- [x] T165 `query-node/src/planner/behavioral_tests.rs` 구현 (D-003: BT 생성 후 PATH_ANALYSIS 자동 라우팅 — Warning 없음 + 결과 동등성)
- [x] T166 `query-node/src/planner/behavioral_tests.rs` 구현 (D-004: Event Query는 BT 있어도 라우팅 안 됨 — COUNT(*)/GROUP BY EXPLAIN에 Behavioral Routing 섹션 없음)
- [x] T167 `query-node/src/planner/behavioral_tests.rs` 구현 (D-005: BT pair 미등록 일반 테이블 — session_id 등 컬럼명에 관계없이 라우팅/Guidance 없음)
- [x] T168 `query-node/src/planner/behavioral_tests.rs` 구현 (D-006: BT Stale 상태 → Fallback + Guidance — Warning에 Stale 사유 포함)
- [x] T169 `query-node/src/planner/behavioral_tests.rs` 구현 (D-007: CREATE SESSION MV 직후 즉시 라우팅 가능 — 지연 없음)
- [x] T170 `query-node/src/planner/behavioral_tests.rs` 구현 (D-008: Guidance 메시지 품질 — table명/CREATE SESSION MV DDL/USER KEY/SESSION TIMEOUT/성능힌트 포함)

### 성능 검증

- [x] T171 `query-node/src/planner/behavioral_tests.rs` 구현 (E-001: BT 라우팅 성능 비교 — 100k행 FUNNEL 쿼리, BT 사용 시 Event Table보다 느리지 않음 검증, 결과 동등성)

**체크포인트**: BT pair 없는 Behavioral Query 실행 시 warnings에 DDL 제안 포함, BT 생성 후 동일 쿼리 재실행 시 자동 BT 라우팅, EXPLAIN에 "Behavioral Routing" 섹션 표시

---

## Phase 19: 클러스터 관리 — Dynamic Node Add/Remove & Rebalance (FR-CM-001~FR-CM-020)

**목적**: QN 중심 노드 자가 등록, 노드 상태 관리(ACTIVE→READONLY→DRAINING), 백그라운드 Shard Rebalance, ALTER CLUSTER JOIN/DRAIN/DISMISS SQL 명령 지원
**상세 설계**: `design/cluster-management.md`, `spec.md` FR-CM-001 ~ FR-CM-020

### 노드 상태 모델 & Raft 토폴로지

- [ ] T172 [P] `shared/src/cluster.rs` 구현 (NodeInfo/NodeType(QN/CN/SN)/NodeState(Active/Readonly/Draining) 타입 정의, 상태 전환 규칙: Active→Readonly→Draining 단방향, NodeState::can_transition_to() 검증, Raft KV 경로: /cluster/nodes/{node_id})
- [ ] T173 `query-node/src/meta/cluster_topology.rs` 구현 (ClusterTopologyManager — register_node/update_node_state/deregister_node/list_nodes/get_node/list_nodes_by_type/list_active_nodes_by_type 메서드, Raft KV CRUD)
- [ ] T174 `proto/cluster.proto` 신규 + `query-node/src/rpc/cluster_service.rs` 구현 (ClusterService gRPC: RegisterNode/UpdateNodeState/GetClusterTopology/GetRebalanceStatus RPC, 포트 9011, SN 등록 시 RebalanceTrigger 채널 이벤트 전송)
- [ ] T175 `compute-node/src/startup.rs`, `storage-node/src/startup.rs` 수정 (기동 시 자가 등록 — 환경변수 QN_PEERS 파싱, 첫 응답 QN에 RegisterNode gRPC 호출, NODE_ID 환경변수 사용, 헬스체크 대기)

### SQL 명령 파서 & 핸들러

- [ ] T176 `query-node/src/sql_parser/cluster_mgmt.rs` 구현 (ALTER CLUSTER 파서 — JOIN `<type> '<addr>:<port>'`/DRAIN `'<node_id>'`/DISMISS `'<node_id>'` [FORCE]/REBALANCE 문법, ClusterCommand AST 열거형)
- [ ] T177 `query-node/src/mysql_protocol/handler.rs` 수정 + `query-node/src/meta/cluster_cmd.rs` 구현 (ClusterCommandExecutor — execute_join/execute_drain/execute_dismiss/execute_rebalance, DISMISS FORCE 시 /cluster/shards/{shard_id}/* Raft KV 일괄 삭제, ER_NODE_HAS_DATA(3001)/ER_RAFT_QUORUM_LOSS(3002)/ER_NODE_NOT_FOUND(3004) 오류 처리)
- [ ] T178 `query-node/src/mysql_protocol/schema_cmds.rs` 수정 (SHOW CLUSTER NODES/STATUS/REBALANCE 핸들러 — NODES: node_id/type/address/state/shards/joined_at, STATUS: metric/value, REBALANCE: job_id/type/from_node/to_node/shards_done/eta_secs)

### INSERT 라우팅 NodeState 필터

- [ ] T179 `query-node/src/planner/shard_placement.rs` 구현 (ShardPlacement — select_write_nodes: ACTIVE 상태 SN만 반환, select_read_nodes: ACTIVE+READONLY+DRAINING 허용, DRAIN 명령 후 최대 100ms 이내 INSERT 라우팅 제외 보장)

### Shard Rebalancer (백그라운드)

- [ ] T180 `query-node/src/meta/rebalance/planner.rs` 구현 (RebalancePlanner — total_shards/active_sn_count 균등 분산 계획, 최소 이전 횟수 greedy 알고리즘, 동시 Rebalance 시 계획 병합)
- [ ] T181 `query-node/src/meta/rebalance/migrator.rs` 구현 (ShardMigrator — execute_plan 비동기 실행, 단일 Shard 이전 4단계: 빈Shard생성→SSTable스트리밍복제→Raft KV location cut-over(원자적)→삭제지시, proto/cluster.proto에 ShardService: CreateShard/CopyShard/DeleteShard RPC 추가)
- [ ] T182 `query-node/src/meta/rebalance/coordinator.rs` 구현 (RebalanceCoordinator — on_node_joined/on_node_draining 이벤트 처리, get_job_status, cluster.rebalance_concurrency 설정 기반 동시 실행 제한, 백그라운드 tokio task로 실행)

### SN 프로토콜 (Shard 복제)

- [ ] T183 `storage-node/src/rpc/shard_service.rs` 구현 (ShardService gRPC 서버 — CreateShard(빈 Shard 디렉토리 생성)/CopyShard(SSTable 파일 스트리밍)/DeleteShard(Raft 확인 후 디렉토리 삭제) 구현)

### Kubernetes / Helm 통합

- [ ] T184 `helm/wowdb/` 신규 구현 (Helm Chart 기본 구조 — Chart.yaml, values.yaml, templates/: configmap.yaml/qn-statefulset.yaml/qn-headless-svc.yaml/qn-svc.yaml/cn-deployment.yaml/cn-hpa.yaml/sn-statefulset.yaml/_helpers.tpl, ConfigMap 필수 키: qn.peers(콤마구분 DNS목록)/cluster.rebalance_enabled, `helm lint` 통과)
- [ ] T185 `docker/docker-compose.yml`, `docker/docker-compose.dev.yml` 수정 + `docker/Dockerfile.compute-node`, `docker/Dockerfile.storage-node` 수정 (QN_PEERS 환경변수 추가 및 ENV 문서화, `docker compose up` 후 SHOW CLUSTER NODES 모든 노드 ACTIVE 확인 가능)

### 통합 테스트

- [ ] T186 `integration-tests/src/cluster_management.rs` 구현 (CM-G-001: SN JOIN 후 Rebalance 완료 엔드투엔드 — 3SN기동→4번째SN JOIN→Rebalance진행중확인→완료→Shard균등분포±1)
- [ ] T187 `integration-tests/src/cluster_management.rs` 구현 (CM-G-002: DRAIN→DISMISS 노드 제거 — 4SN+10k행INSERT→DRAIN→INSERT라우팅제외확인→SELECT가능확인→DRAIN완료→DISMISS→데이터손실없음)
- [ ] T188 `integration-tests/src/cluster_management.rs` 구현 (CM-G-003: FORCE DISMISS 데이터 삭제 — 데이터있는SN DISMISS without FORCE→ER_NODE_HAS_DATA, FORCE→성공→Raft KV Shard항목삭제확인)
- [ ] T189 `integration-tests/src/cluster_management.rs` 구현 (CM-G-004: CN 추가/제거 — CN JOIN 후 쿼리 Fragment 라우팅 포함 확인, CN DRAIN 후 새 쿼리 라우팅 제외+진행중쿼리완료)
- [ ] T190 `integration-tests/src/cluster_management.rs` 구현 (CM-G-005: QN Raft Quorum 보호 — 3QN 클러스터 2번째QN DRAIN시도→ER_RAFT_QUORUM_LOSS, 1번째QN DRAIN 성공 후 2번째 DRAIN→오류)

**체크포인트**: `docker compose up` 후 `SHOW CLUSTER NODES` 모든 노드 ACTIVE 확인, ALTER CLUSTER JOIN/DRAIN/DISMISS 명령 동작, Rebalance 완료 후 `SHOW CLUSTER REBALANCE` 빈 결과, Shard 균등 분산 확인

---

## Notes

- `[P]` = 다른 파일, 의존성 없는 태스크 — 병렬 실행 권장
- `[USN]` = 해당 Story 추적 가능성을 위한 레이블
- SIMD 태스크(T043~T045)는 Linux x86_64 환경에서만 AVX2 활성화 — 비 Linux에서는 스칼라 fallback 필수
- `tokio-uring` 관련 코드는 반드시 `#[cfg(target_os = "linux")]` 조건부 컴파일 적용
- 각 Phase 체크포인트에서 `docker compose up → smoke test` 실행 권장
- Phase 2 완료 전 Phase 3+ 태스크 시작 금지
