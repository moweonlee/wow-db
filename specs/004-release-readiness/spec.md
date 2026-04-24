# Feature Specification: WOW-DB Release Readiness

**Feature Branch**: `004-release-readiness`
**Created**: 2026-04-24
**Status**: Draft
**Input**: User description: "이 DB 를 Release 하기 위해서는 Easy Start Up Guide 와 Advanced Guide 그리고 Sample 로 사용할 수 있는 data set 과 Query 그리고 성능이 정말 만족되는지를 면밀히 알수 있는 test 결과 파일들이 필요해 이를 위해서 너는 실제로 이 DB 의 완성도를 높이기 위한 Spec 을 구성해줘"

---

## Overview

WOW-DB is a MySQL-compatible OLAP database designed for web analytics workloads at 20 billion+ row scale. Before public release, the project requires four deliverables that demonstrate its completeness and correctness:

1. **Easy Startup Guide** — enables any developer to run WOW-DB from zero in under 10 minutes
2. **Advanced Guide** — enables engineers to configure, tune, and operate a production cluster
3. **Sample Datasets & Queries** — provides immediately runnable examples demonstrating WOW-DB's unique features
4. **Performance Test Report** — provides reproducible benchmark evidence that WOW-DB meets its performance claims

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - First-Time Developer Quick Start (Priority: P1)

A developer discovers WOW-DB and wants to run their first query within 10 minutes using Docker, with no prior knowledge of the system.

**Why this priority**: This is the #1 release blocker. If a new user cannot start the system quickly and successfully, the release fails on first impression.

**Independent Test**: Can be fully tested by following the Quick Start guide from a clean machine (Docker only) and successfully running all sample queries.

**Acceptance Scenarios**:

1. **Given** a machine with Docker installed, **When** the developer follows the Quick Start guide from top to bottom, **Then** a working WOW-DB cluster is running and accepting MySQL connections within 10 minutes.
2. **Given** a running cluster, **When** the developer copies the sample INSERT and SELECT queries from the guide, **Then** all queries execute without error and return correct results.
3. **Given** a running cluster, **When** the developer runs the provided sample dataset loader script, **Then** the dataset loads completely and the provided sample queries return expected outputs.
4. **Given** an error during startup (e.g., port conflict), **When** the guide's troubleshooting section is consulted, **Then** the issue is resolvable without external help.

---

### User Story 2 - Engineer Deploying a Production Cluster (Priority: P2)

An infrastructure engineer needs to deploy a multi-node WOW-DB cluster (QN×3, CN×3, SN×3), configure it for their environment, and understand operational procedures.

**Why this priority**: Production deployability is the primary value proposition. Without this, WOW-DB cannot be adopted beyond prototyping.

**Independent Test**: Can be fully tested by following the Advanced Guide to deploy a multi-node cluster on bare metal or VMs, configure storage and compute, and verify cluster health.

**Acceptance Scenarios**:

1. **Given** the Advanced Guide, **When** an engineer configures a multi-node cluster, **Then** the cluster starts, all nodes register with each other, and the dashboard shows all nodes as ONLINE.
2. **Given** a running cluster, **When** one Query Node fails, **Then** the remaining QNs continue serving queries (Raft-based failover) and the guide explains how to recover the failed node.
3. **Given** a need to tune performance, **When** the engineer consults the Advanced Guide's tuning section, **Then** they can find guidance on LSM compaction thresholds, gRPC limits, memory allocation, and partition strategy.
4. **Given** a need to load production data, **When** the engineer follows the bulk-load guide, **Then** data can be ingested via INSERT or streaming pipeline without data loss.

---

### User Story 3 - Evaluator Assessing Performance Claims (Priority: P2)

A database evaluator (DBA or architect) needs to verify WOW-DB's performance claims against a reproducible benchmark before recommending adoption.

**Why this priority**: Performance credibility is required for any serious database release. Unverified claims prevent adoption by technical decision-makers.

**Independent Test**: Can be fully tested by running the provided benchmark script (TPC-H SF=0.1) on the reference cluster configuration and comparing results against the published report.

**Acceptance Scenarios**:

1. **Given** the benchmark script and a 3-SN cluster with TPC-H SF=0.1 data loaded, **When** the evaluator runs the benchmark, **Then** all queries complete and produce correct results matching TPC-H expected outputs.
2. **Given** the published performance report, **When** the evaluator runs the same benchmark on equivalent hardware, **Then** their measured results are within 20% of the published figures.
3. **Given** the performance report, **When** the evaluator reads it, **Then** they can find: hardware spec, data volume, query latency per query, throughput (rows/sec), and memory usage.
4. **Given** a query that returns no results, **When** the evaluator checks the benchmark output, **Then** the report explicitly marks it as PASS or FAIL with the expected vs. actual row count.

---

### User Story 4 - Developer Exploring WOW-DB Unique Features (Priority: P3)

A developer wants to understand WOW-DB's unique OLAP capabilities — Cube DDL, Session Materialized Views, behavioral routing, and web analytics queries — through hands-on examples.

**Why this priority**: Differentiating features drive adoption. Without worked examples, WOW-DB appears to be just another MySQL-compatible database.

**Independent Test**: Can be fully tested by running all provided sample queries in the feature showcase section and observing the distinct behavior (e.g., behavioral routing rewriting queries transparently).

**Acceptance Scenarios**:

1. **Given** the sample dataset (events + sessions), **When** the developer runs the session materialized view example, **Then** the behavioral router transparently redirects the query and the result is returned from the pre-aggregated sessions table.
2. **Given** the web analytics sample, **When** the developer runs funnel, cohort, or path analysis queries (if implemented), **Then** results are returned and the guide explains how these differ from standard SQL GROUP BY.
3. **Given** the Cube DDL examples, **When** the developer creates a new Cube with custom distribution keys, **Then** SHOW CUBES confirms the partition and distribution configuration.

---

### Edge Cases

- What happens if the user's machine has port 9030 already occupied? → Quick Start guide must include a port-remapping section.
- How does the guide handle Docker Desktop on Windows vs. Linux/macOS differences?
- What if the benchmark script runs against a cold cache vs. warm cache — are results labeled accordingly?
- What if TPC-H queries return 0 rows due to a bug? → Report must distinguish "correctly returns 0" from "bug returns 0".
- What happens when the sample dataset exceeds available memory? → Guide must specify minimum hardware requirements.

---

## Requirements *(mandatory)*

### Functional Requirements

#### Easy Startup Guide (FR-001 – FR-008)

- **FR-001**: The Quick Start guide MUST enable a user to start a single-node WOW-DB instance using a single `docker compose` command with no configuration changes.
- **FR-002**: The Quick Start guide MUST include a copy-pasteable sequence of SQL commands (CREATE CUBE, INSERT, SELECT) that produces visible output within 60 seconds of cluster start.
- **FR-003**: The Quick Start guide MUST specify minimum hardware requirements (CPU, RAM, disk) for the single-node configuration.
- **FR-004**: The Quick Start guide MUST include a troubleshooting section covering the 5 most common startup failures (port conflict, Docker memory limit, health check timeout, missing image, network conflict).
- **FR-005**: The Quick Start guide MUST be a single self-contained document (Markdown) that requires no external links to be functional.
- **FR-006**: The Quick Start guide MUST include connection instructions for at least two MySQL clients: `mysql` CLI and a GUI tool (e.g., DBeaver, TablePlus).
- **FR-007**: The Quick Start guide MUST show the web monitoring dashboard URL and a screenshot or description of expected healthy state.
- **FR-008**: The Quick Start guide MUST include a "next steps" section pointing to the Advanced Guide and sample datasets.

#### Advanced Guide (FR-009 – FR-018)

- **FR-009**: The Advanced Guide MUST document the full cluster topology: QN (Query Node), CN (Compute Node), SN (Storage Node) roles, ports, and inter-node communication.
- **FR-010**: The Advanced Guide MUST document all environment variables and TOML configuration options for each node type.
- **FR-011**: The Advanced Guide MUST include a multi-node deployment example (QN×3, CN×3, SN×3) with the provided docker-compose configuration.
- **FR-012**: The Advanced Guide MUST document the Cube DDL syntax: `CREATE CUBE`, `ALTER CUBE`, `DROP CUBE`, `SHOW CUBES`, `DESCRIBE`, `SHOW CREATE CUBE`.
- **FR-013**: The Advanced Guide MUST document the Session Materialized View DDL: `CREATE SESSION MATERIALIZED VIEW ... ON ... KEY ... TIMEOUT`.
- **FR-014**: The Advanced Guide MUST include an operational runbook covering: rolling restart, node addition, node removal, data backup, and log rotation.
- **FR-015**: The Advanced Guide MUST document performance tuning parameters: LSM compaction thresholds, gRPC message size limits, MemTable flush thresholds, and partition strategy.
- **FR-016**: The Advanced Guide MUST document the behavioral routing system: when queries are intercepted, how to create and populate session MVs, and how to bypass routing for raw event access.
- **FR-017**: The Advanced Guide MUST document the monitoring dashboard endpoints and what each metric means.
- **FR-018**: The Advanced Guide MUST include a security section covering: network isolation, authentication (current limitations), and recommended production hardening.

#### Sample Datasets & Queries (FR-019 – FR-027)

- **FR-019**: The sample package MUST include a TPC-H dataset loader at scale factor SF=0.01 (approximately 60,000 lineitem rows) that loads in under 5 minutes on the reference hardware.
- **FR-020**: The sample package MUST include a web analytics dataset: a minimum of 10,000 events across 500 sessions with realistic distribution of event types, pages, devices, and countries.
- **FR-021**: The TPC-H sample MUST include all 6 benchmark queries (Q01, Q03, Q05, Q06, Q10, Q14) as runnable SQL files with expected result row counts documented.
- **FR-022**: The web analytics sample MUST include queries demonstrating: session aggregation, funnel analysis (entry → product → checkout), top-N pages, user retention by country, and behavioral routing.
- **FR-023**: The sample package MUST include a `README` explaining what each dataset represents, how to load it, and what each sample query demonstrates.
- **FR-024**: The sample queries MUST be organized by complexity: Basic (COUNT, GROUP BY), Intermediate (multi-table JOIN, subquery), Advanced (EXISTS, correlated subquery, Session MV).
- **FR-025**: Each sample query file MUST include: the query, expected result shape (columns and approximate row count), and a one-line description of what it demonstrates.
- **FR-026**: The sample package MUST include a teardown script to drop all sample tables and reset the cluster to a clean state.
- **FR-027**: The sample loader MUST be executable via a single command (`python bench.py` or `./load-samples.sh`) without requiring manual SQL execution.

#### Performance Test Report (FR-028 – FR-036)

- **FR-028**: The performance test report MUST document the exact hardware and software configuration used: CPU model, core count, RAM, storage type, Docker version, OS.
- **FR-029**: The performance test report MUST include results for TPC-H SF=0.01 with all 6 benchmark queries: query text, execution time (ms), result row count, and PASS/FAIL status.
- **FR-030**: The performance test report MUST include a data ingestion benchmark: rows per second for bulk INSERT into a single Cube across 1, 2, and 3 Storage Nodes.
- **FR-031**: The performance test report MUST include a concurrent query benchmark: query latency (p50, p95, p99) at 1, 5, and 10 concurrent connections.
- **FR-032**: The performance test MUST measure and report cluster startup time: time from `docker compose up` to all nodes reporting healthy.
- **FR-033**: The performance test report MUST clearly label each measurement with: warm-cache vs. cold-cache, cluster size, and data volume.
- **FR-034**: The performance test report MUST be a reproducible artifact: it MUST include the exact commands used to produce each measurement so any reader can reproduce it.
- **FR-035**: The performance test report MUST include a correctness section: for each TPC-H query, the expected row count from the TPC-H spec vs. the actual row count returned by WOW-DB.
- **FR-036**: The performance test report MUST include known limitations and bugs as of the release version, with a severity classification (Critical / Major / Minor).

### Key Entities

- **Quick Start Guide**: A self-contained Markdown document (`docs/quickstart.md`) covering single-node setup, first queries, and dashboard access.
- **Advanced Guide**: A structured Markdown document (`docs/advanced.md`) covering cluster architecture, DDL reference, operations runbook, and tuning.
- **Sample Dataset Package**: A directory (`samples/`) containing loader scripts, SQL files, and a README. Includes TPC-H and web analytics datasets.
- **Performance Test Report**: A structured Markdown document (`docs/performance-report.md`) containing reproducible benchmark results with hardware spec, methodology, and results tables.
- **Benchmark Script**: An executable script (`scripts/tpch/bench.py`) that loads TPC-H data, runs all queries, and outputs a structured results file.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer with no prior WOW-DB knowledge can start a single-node cluster and run their first successful query in under 10 minutes by following the Quick Start guide alone.
- **SC-002**: All 6 TPC-H benchmark queries (Q01, Q03, Q05, Q06, Q10, Q14) return correct results (row counts within ±1% of TPC-H expected values) on SF=0.01 data.
- **SC-003**: TPC-H benchmark queries complete with a total wall-clock time under 60 seconds for all 6 queries combined on the reference 3-SN cluster with SF=0.01 data.
- **SC-004**: Data ingestion rate exceeds 10,000 rows/second for bulk INSERT on a 3-SN cluster with the sample dataset.
- **SC-005**: Cluster startup (all nodes healthy) completes within 60 seconds on the reference hardware.
- **SC-006**: The web analytics sample dataset loads (10,000 events) in under 30 seconds, and all 5 sample analytics queries execute successfully.
- **SC-007**: The performance report is fully reproducible: an independent engineer following its methodology produces results within 20% of published figures on equivalent hardware.
- **SC-008**: The Advanced Guide covers 100% of user-facing configuration options (zero undocumented environment variables or TOML keys).
- **SC-009**: All known Critical and Major bugs are documented in the performance report's known limitations section before release.
- **SC-010**: The Quick Start guide achieves a task completion rate of 90% — defined as a user successfully running their first query — without requiring support.

---

## Assumptions

- The target release audience is software engineers and database administrators; non-technical business users are out of scope for this release.
- Docker Desktop (Windows/macOS) and Docker Engine (Linux) are the primary supported deployment methods; native/bare-metal installation guides are out of scope for this release.
- TPC-H SF=0.01 is the reference benchmark scale; SF=1 and above are aspirational and not required to pass for this release.
- The web analytics sample dataset will be synthetically generated; real-world anonymized data is not available or required.
- The performance test report will be generated on the development machine (Windows 11, Docker Desktop); cloud/bare-metal results are not required for this release.
- Authentication and authorization are currently not implemented in WOW-DB; the security section of the Advanced Guide will document this as a known limitation.
- The existing `scripts/tpch/bench.py` benchmark script is the foundation for the performance report; it will be extended rather than rewritten.
- Multi-language documentation (e.g., Korean) is desirable but the English version is the minimum for release; Korean annotations in code comments are acceptable.
- The sample queries will use WOW-DB-specific DDL (`CREATE CUBE`, `CREATE SESSION MATERIALIZED VIEW`) and will not be portable to standard MySQL without modification — this is by design and must be clearly noted.
