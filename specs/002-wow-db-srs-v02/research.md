# 연구 결과: WOW-DB 구현 기술 스택

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-12  
**Phase**: 0 — 기술 불확실성 해소

---

## 1. SIMD 전략

### 결정: `std::arch::x86_64` 직접 인트린식 + CPUID 런타임 디스패치

| 항목 | 선택 | 근거 |
|---|---|---|
| **SIMD 백엔드** | `std::arch::x86_64` | 안정적, 직접 AVX2/AVX-512 제어, 제로비용 추상화 |
| **동적 디스패치** | CPUID 런타임 검사 | AVX2 기본, AVX-512 자동 활성화 |
| **Portable SIMD 배제** | `std::simd` 사용 안 함 | 프로덕션 환경에서 직접 인트린식 대비 제어 부족 |
| **`wide` 크레이트 배제** | 사용 안 함 | 낮은 수준 추상화, 수동 디스패치 로직 과중 |

**오퍼레이터별 SIMD 적용:**
- **컬럼 스캔**: `_mm256_maskload_epi32` + prefetch 힌트 → 스트리밍 읽기
- **필터 (WHERE)**: `_mm256_cmpeq_epi32` + `_mm256_packs_epi32` → predicate 패킹
- **집계 (SUM/COUNT)**: AVX-512 64비트 누산 + conflict detection
- **Hash Join**: AVX2 버킷 prefetch + scatter/gather 프로브

**결론: C++ 완전 불필요** — Rust 인트린식은 2025년 기준 프로덕션 품질.

---

## 2. LSM-Tree 구현 (자체 구현, RocksDB 미사용)

### 결정: 커스텀 Rust LSM + `tokio-uring` (Linux) / `tokio::fs` (이식성)

**아키텍처:**
```
Write Path:
  이벤트 도착
  → WAL 세그먼트 append (CRC32 검증, crc32fast 크레이트)
  → MemTable (Skip-list, Arc+RwLock) — Sort Key 기준 정렬
  → 임계값 초과 → Immutable MemTable
  → Flush → Level-0 SSTable (컬럼별 독립 파일)
  → Background Compaction: L0→L1 Leveled (파티션 경계 내)
```

| 크레이트 | 용도 |
|---|---|
| `tokio-uring` | Linux io_uring 비동기 DIO (NVMe 10-20% 레이턴시 개선) |
| `tokio::fs` | 비-Linux 이식성 fallback |
| `bytes` | WAL/SSTable 제로카피 버퍼 |
| `crc32fast` | WAL 체크섬 |
| `lz4_flex` | 컬럼 파일 LZ4 압축 |
| `zstd` | JSON/고압축 컬럼 ZSTD 압축 |

---

## 3. 컬럼형 인메모리 표현

### 결정: `arrow2` 크레이트 + 원시 버퍼 SIMD 접근

- **`arrow2` vs `arrow`**: `arrow2`가 분석 워크로드에 더 경량, Compute Node에 적합
- **SIMD 통합**: `PrimitiveArray::values_unchecked()` → SIMD 포인터 연산 직접 적용
- **Flat JSON**: `arrow2::array::UnionArray` 로 이종 JSON 키 처리

---

## 4. Raft 합의

### 결정: `openraft` (TiKV 팀, 2025 기준 활발 유지)

- **이유**: 깨끗한 async API, 강한 타입 안전성, Kubernetes 무작위 라우팅 지원
- **저장 데이터**: Cube 스키마, Tablet 위치 맵, 컬럼 통계, 세션 토큰
- **`raft-rs` 배제**: 저수준, 수동 통합 로직 과중

---

## 5. MySQL Wire Protocol (서버측)

### 결정: `opensrv-mysql` (msql-srv의 활발 유지 포크)

- **이유**: MySQL 8.0 프로토콜 에뮬레이션, 드라이버 수정 불필요, BI 도구 호환
- **`mysql_async` 배제**: 클라이언트 전용, 서버 프로토콜 불지원

---

## 6. gRPC (노드 간 통신)

### 결정: `tonic` + `prost`

- **이유**: async-first, Tokio 통합, HTTP/2 멀티플렉싱 → CN 병렬 실행에 최적
- **`grpcio` 배제**: C++ libgrpc 빌드 복잡성 회피 (Rust-first 철학)

---

## 7. SQL 파싱

### 결정: `sqlparser-rs` (MySQL 8.0 방언) + 커스텀 확장 파서

- `sqlparser-rs`: 표준 MySQL 8.0 구문 처리 (SELECT, INSERT, CREATE/ALTER)
- **커스텀 AST 노드 추가**: `FUNNEL_COUNT`, `COHORT_ANALYSIS`, `PATH_ANALYSIS`, `CREATE CUBE`, `CREATE SESSION MATERIALIZED VIEW`
- **전면 커스텀 파서 배제**: MySQL 방언 유지보수 중복 회피

---

## 8. S3 백엔드

### 결정: `object_store` (Apache Arrow 에코시스템)

- **이유**: Arrow 네이티브 통합, S3/MinIO 호환, 단순 API (PUT/GET/DELETE)
- **`opendal` 배제**: 추가 추상화 복잡성 불필요 (S3가 주요 오브젝트 스토리지)

---

## 9. HDFS 백엔드 (Kerberos 필수)

### 결정: `opendal` + `hdfs-native-client`

- **이유**: 순수 Rust, JVM 의존성 없음, GSSAPI/Kerberos 지원 (FR-012 필수)
- **Java JNI 완전 배제**: Rust-first 철학

---

## 10. Kafka 컨슈머

### 결정: `rdkafka` (librdkafka C 바인딩)

- **이유**: 정확히 한 번 전달(exactly-once) 보장, 컨슈머 그룹 오프셋 관리, 15년 이상 검증
- **C 의존성 허용**: librdkafka는 벤더링 가능, JVM 없음, 사양에서 C 허용
- **`rskafka` 배제**: 프로덕션 배포 경험 부족

---

## 11. 비동기 런타임 및 I/O

### 결정: `tokio` (기본) + `tokio-uring` (Linux NVMe 최적화)

- **`tokio`**: 크로스 플랫폼 기반, 에코시스템 표준
- **`tokio-uring`**: Linux io_uring DIO → SSTable 100-500MB 오브젝트 I/O 최적화
- **`monoio` 배제**: 에코시스템 성숙도 부족

---

## 12. Docker Compose 로컬 테스트

### 결정: 멀티스테이지 Dockerfile + cargo-chef + Profiles

- **`cargo-chef`**: 의존성 레이어와 소스코드 레이어 분리 → 빌드 캐시 최적화
- **기본 이미지**: 빌드 `rust:1.87-bookworm` → 런타임 `debian:bookworm-slim`
- **서비스 프로파일**: `full`, `storage`, `query` 프로파일로 부분 스택 테스트 지원
- **Kafka 대체**: Redpanda (faster cold-start, Docker에서 Kafka 호환)
- **S3 대체**: MinIO (로컬 S3 호환)
- **io_uring**: `cfg(target_os = "linux")` feature flag로 조건부 컴파일

---

## 기술 스택 요약

| 계층 | 크레이트/도구 | 비고 |
|---|---|---|
| 비동기 런타임 | `tokio` | 전 노드 공통 |
| SIMD | `std::arch::x86_64` | CN 전용 |
| gRPC | `tonic` + `prost` | QN↔CN↔SN |
| MySQL Protocol | `opensrv-mysql` | QN 전용 |
| Raft | `openraft` | QN 전용 |
| SQL Parser | `sqlparser-rs` + 커스텀 | QN 전용 |
| Web Server | `axum` | QN Web UI |
| 인메모리 컬럼 | `arrow2` | CN 전용 |
| LSM-Tree | 자체 구현 | SN 전용 |
| 파일 I/O | `tokio-uring` / `tokio::fs` | SN 전용 |
| S3 | `object_store` | SN 전용 |
| HDFS | `opendal` + `hdfs-native-client` | SN 전용 |
| Kafka | `rdkafka` | QN/Ingestion |
| 압축 | `lz4_flex`, `zstd` | SN 전용 |
| 체크섬 | `crc32fast` | SN 전용 |
| 직렬화 | `serde`, `serde_json` | 전 노드 공통 |

**C++ 의존성**: `rdkafka`(librdkafka) 한 개만 허용. 나머지 모두 순수 Rust.
