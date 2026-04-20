# Quickstart: Web Monitoring Dashboard

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-17

---

## 대시보드 접속

클러스터가 실행 중이면 브라우저에서 바로 접속 가능합니다.

```
http://localhost:8080/dashboard
```

또는 QN 헬스체크로 기동 확인 후 접속:

```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

---

## 시나리오별 확인 방법

### 1. 클러스터 노드 현황 확인 (Cluster 탭)

브라우저에서 `http://localhost:8080/dashboard` 접속 → **Cluster** 탭

또는 API 직접 호출:

```bash
curl http://localhost:8080/api/v1/cluster | python3 -m json.tool
```

**확인 항목**:
- QN, CN, SN 노드 목록
- 각 노드의 IP 주소, 역할, alive 상태
- Raft 리더 정보

---

### 2. 테이블 및 스토리지 정보 (Storage 탭)

```bash
curl http://localhost:8080/api/cubes | python3 -m json.tool
```

또는 대시보드 **Storage** 탭에서 시각적으로 확인.

---

### 3. LSM Compaction 상태 (LSM Status 탭)

**QN 집계 엔드포인트** (구현 후):

```bash
curl http://localhost:8080/api/v1/lsm | python3 -m json.tool
```

**SN 직접 조회** (구현 후):

```bash
curl http://localhost:8040/api/v1/lsm-status | python3 -m json.tool
```

**대량 데이터로 Compaction 트리거**:

```bash
# 많은 데이터 삽입 → L0 파일 4개 이상 누적
docker run --rm mysql:8.0 mysql -h host.docker.internal -P 9030 -u admin --password= \
  -e "INSERT INTO events SELECT * FROM generate_series(1, 100000)"
```

---

### 4. CN/SN 노드 상세 로그 조회 (구현 후)

**CN 로그**:

```bash
curl http://localhost:10040/logs | python3 -m json.tool
```

**SN 로그**:

```bash
curl http://localhost:8040/logs | python3 -m json.tool
```

**WARN 이상만 필터**:

```bash
curl "http://localhost:8040/logs?level=WARN" | python3 -m json.tool
```

---

## 통합 테스트 시나리오

### 시나리오 A: 기본 접속 테스트

```bash
# 클러스터 기동 (dev-test.ps1 또는 수동)
.\scripts\dev\run-local.ps1 -NoBuild

# 대시보드 접속
curl -s http://localhost:8080/dashboard | grep "<title>"
# Expected: <title>WOW-DB Dashboard</title>

# 헬스체크 + Cluster API
curl -s http://localhost:8080/api/v1/cluster | python3 -c \
  "import sys, json; d=json.load(sys.stdin); print('QN:', len(d['query_nodes']), 'CN:', len(d['compute_nodes']), 'SN:', len(d['data_nodes']))"
```

### 시나리오 B: 노드 OFFLINE 감지

```bash
# SN 종료 후 30초 이내 대시보드에서 OFFLINE 확인
Stop-Process -Name storage-node -Force

# 30초 후
curl -s http://localhost:8080/api/v1/cluster | \
  python3 -c "import sys,json; d=json.load(sys.stdin); [print(n['id'], n['alive']) for n in d['data_nodes']]"
# Expected: sn-local-1 false
```

### 시나리오 C: LSM Compaction 상태 변화

```bash
# 데이터 삽입 후 Compaction 발생 확인
curl -s http://localhost:8040/api/v1/lsm-status | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print('L0:', d['total_l0_files'], 'Control:', d['write_control'])"
```

---

## 포트 정리

| 노드 | HTTP 포트 | 용도 |
|------|----------|------|
| QN | 8080 | 대시보드, REST API |
| SN | 8040 | /health, /logs, /api/v1/lsm-status |
| CN | 10040 | /health, /logs |
| QN MySQL | 9030 | MySQL 프로토콜 |
| CN gRPC | 9040 | ExecuteFragment |
| SN gRPC | 9060 | WriteRows, ScanTablet |
