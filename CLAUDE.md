# wow-db Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-04-12

## Active Technologies

- Rust 1.87 stable (주), C++ 없음 (rdkafka의 librdkafka 제외) (002-wow-db-srs-v02)

## Project Structure

```text
query-node/       # Query Node: MySQL Protocol, SQL Parser, CBO, Raft, Web UI
compute-node/     # Compute Node: SIMD Executor, Hash Join, Analytics Functions
storage-node/     # Storage Node: LSM-Tree, Columnar Files, S3/HDFS Backend
shared/           # 공통 타입, Arrow IPC 코덱, 에러 처리
proto/            # Protobuf 정의 (tonic+prost)
integration-tests/
docker/           # Docker Compose, Dockerfiles, 설정 템플릿
```

## Commands

```bash
cargo test --workspace          # 전체 단위 테스트
cargo test -p query-node        # 특정 모듈 테스트
docker compose up --build       # 전체 클러스터 기동 (QN×3, CN×2, SN×3)
docker compose down -v          # 클러스터 종료 + 데이터 초기화
cargo clippy --workspace        # Lint
```

## Code Style

Rust 1.87 stable (주), C++ 없음 (rdkafka의 librdkafka 제외): Follow standard conventions

## Recent Changes

- 002-wow-db-srs-v02: Added Rust 1.87 stable (주), C++ 없음 (rdkafka의 librdkafka 제외)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
