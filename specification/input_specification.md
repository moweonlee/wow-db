MYSQL 과 호환 되는 OLAP DB 를 만드는 것을 목적으로 (STARROCKS 에서 영감 받음) SRS 를 작성하고 싶어
특징은 
1. RUST 로 개발
2. SIMD 를 적극 활용
3. MYSQL Compatible Client 를 사용
4. LSM-TREE 기반의 Storage Engine 을 사용
5. 일반적인 DB 들이 Schema 를 직접 설계 하는데에 반해 이 DB 는 EVENT 기반의 WEB 분석과 이 데이터를 사용자별 Session 으로 전환한 Cube 를 만드는 것을 주 목적으로 WEB 분석에 최적화된 기능을 Narrow Down 하는 것이 목적이야 
6. 따라서 좀더 접근 하기 윕게 Web 기반의 SQL CLient 편집기가 있는 거지 
Event 기반으로 Cube 를 설계하고 Session 분석에 필요한 사용자 Key 를 지정하면 내부적으로는 session 회된 DB 를 자동으로 만드는 거지 

너의 Role 은 이 기능을 구현하기 위한 SRS 는 아주 상세 적고 모르는 것이 있으면 모호한것으로 계속 나에게 물어보며 매우 상세한 SRS 를 작성하는 거야
일단 SRS 는 총 5장으로 되어 있고 
1. 장에는 개요
2.상세한 Interaction Diragram 및 structure 
3. 기능적 요구사항
4. 비기능적 요구사항
5. C

---------------------------------------------------------------------------------


기존 파일은 비어 있고, 프로젝트가 초기 단계군요. SRS 작성 전에 모호한 부분을 먼저 정리해야 합니다.

명확히 해야 할 질문들
[Chapter 5 관련]

5장의 "C"가 잘렸습니다. 다음 중 어떤 건가요?

Constraints (제약사항)
Component Architecture (컴포넌트 아키텍처 상세)
Compliance / Security (보안/컴플라이언스)
다른 것?
[Core 개념 - Session & Event]

Session 기준은 무엇인가요?

시간 기반? (예: 30분 비활동 시 세션 종료)
명시적 session_start / session_end 이벤트?
둘 다 지원?
User Key는 어떤 형태인가요?

단일 키 (예: user_id)만?
복합 키 (예: device_id + user_id fallback) 지원?
익명 사용자 → 로그인 사용자 병합(identity stitching) 기능 필요?
Cube 의 구체적 의미는?

Materialized View 형태로 자동 생성되는 집계 테이블?
Star Schema 형태?
사용자가 "이 이벤트 스트림을 세션화하라" 명령 시 자동으로 내부 스키마 생성?
[기능 범위]

Web 분석에서 지원할 핵심 분석 유형은?

Funnel Analysis (전환 퍼널)?
Cohort Analysis (코호트 리텐션)?
Path Analysis (사용자 이동 경로)?
A/B Test 지원?
데이터 수집(Ingestion) 방식은?

MySQL INSERT 문으로만?
HTTP REST API (JSON 이벤트)?
Kafka / 스트리밍 연동?
[규모 / 성능 목표]

목표 규모는?

일일 이벤트 수 (예: 수억 건)?
동시 쿼리 수?
단일 노드? 분산 클러스터?
실시간성 요구사항?

이벤트 수집 후 즉시 쿼리 가능해야 하나요?
배치 처리도 허용?
[MySQL 호환 범위]

MySQL 호환 수준은?
클라이언트 프로토콜만 (MySQL Connector로 접속)?
DDL/DML 문법도 MySQL 호환?
어떤 MySQL 버전 기준? (5.7? 8.0?)
위 질문들에 답해주시면 5개 장 모두 아주 상세하게 작성하겠습니다



---------------------------------------------------------------------------------

1의 답 맞아 , 다만 5분 30 분등을 사용자에게 물어서 이 부분을 대화형으로 설정해야하지.
즉 event cube 를 생성하면 session 화 DB 를 만들건지 물어보고 ( Materialized view 로 만드는 게 좋겠다 ) 그리고 생성 여부를 web 에서 물어보면 더 좋을 것 같다.
2. user_key 는 사용자가 설정하는 거야 user_key 도 device_key 도 될 수 있고 다른 것도 될 수 있지
materialized view 를 들때 설정하는 거지 우리가 강제하는 것은 아냐 우리는 OLAP DB 를 만드는 것이니깐

3. Cube 는 Schema 를 정의 한다고 보면 되

4. FUNNEL COHORT PATH 모두 포함이지, 
이 모든 WORKLOAD 를 상세히 고려해야해

5. Inestion 은 Kafka, Spark Ingestion 은 필수야 따라서 StarRocks 처럼 대용량 Ingestion interface 가 필요하고 여기에는 Transaction 도 고려애햐해 

6. 목표는 매우 대용량을 처리할 수 있어야 해 일단 200 억 이상의 데이터 처리가 가능한 분삭 DB 구조가 필요해
이를 위해서 StarRocks 도 FE, BE 가 있듯이 
이 DB 도 기능에 따라서 모듈을 나눠야 하는데
기능의 이름은 Storage Layer, Data Serving Layer 이렇게 
SL, DSL 로 나누면 될 것 같아 어떠니 ? 너의 생각은 

7. LSM 으로 구현되어야 하기 때문에 실시간은 반드시 필요해 Clickhouse 와 같이 

8 MySQL Client 로 접근 가능해야하고 필요하면 StarRocks 처럼 별도의 Client 문법 (Custom Query ) 를 지원해야 하지 이해되니 ?


---------------------------------------------------------------------------------

아키텍처 의견 및 마지막 확인
SL / DSL 분리에 대한 내 생각
기본 방향은 좋습니다. 다만 한 가지를 더 고려할 것을 제안합니다.

현재 제안:


SL (Storage Layer)  ←→  DSL (Data Serving Layer)
내가 제안하는 구조:


[MySQL Client / Web Client]
         ↓
  DSL (Data Serving Layer)          ← StarRocks FE 역할
  - MySQL Protocol Handler
  - SQL Parser & Query Planner
  - Distributed Query Coordinator
  - Session/Cube Manager
  - Web SQL Editor Backend
  - Kafka/Spark Ingestion 코디네이터
         ↓
  SL (Storage Layer) × N nodes      ← StarRocks BE 역할
  - LSM-Tree Engine (Rust)
  - SIMD Query Execution
  - Materialized View 물리 저장
  - Compaction & Merge
  - Kafka Stream Writer (실제 쓰기)
이유: Kafka/Spark 수집 시 "어디서 받아서 어디에 쓰느냐"가 두 레이어에 걸쳐 있기 때문입니다. DSL이 ingestion 코디네이션(트랜잭션 관리, offset 추적)을 담당하고 SL이 실제 LSM write를 담당하는 구조가 StarRocks의 검증된 패턴과 일치합니다.

SL 노드는 수평 확장(sharding), DSL 노드도 HA 구성 (Leader/Follower) 으로 가면 될 것 같습니다.

동의하십니까? 아니면 레이어를 더 단순하게 가져가고 싶으신가요?

마지막 질문 하나
5장의 "C" 가 잘렸습니다. 다음 중 어떤 건가요?

후보	내용
Constraints	기술적/비즈니스 제약사항
Component Architecture	모듈별 상세 설계 명세
Change Log / Revision History	문서 이력
Custom Query Syntax	8번 답변 기준 - Custom SQL 문법 정의
이것만 확인되면 바로 SRS 전체 초안 작성에 들어가겠습니다.


---------------------------------------------------------------------------------

5.장에는  Example ) 
코드 예제가 들어가야해 
SELECT 를 사용한 예제
INSERT 를 사용한 예제
LOAD ( 대용량 IINSERT 를 위한 SPARK 3.4,3.1 기반의 코드 예제 등이 들어가야햄)

이를 위해서 Test Case 도 상세히 작성이 되어야 하지 너가 할일은 일단 
SRS 와 Testcase 를 문성의 형태로작성하는 거야 

SRS 는 srs.md
testcase 는 tc.md 에 작성해줘

---------------------------------------------------------------------------------

1. 일단 NODE 구성은 
- Data Node  ( LSM Storage 를 구현한 Horizontally scalable 한 노드 )
- Compute Node ( Data Node 와 같이 붙어 있을 수도 있지만 떨어질수도 있는 Storage 와 Decoupling 되어서 DB 의 연산을 담당 하는 모듈 

Optimizer Planner 가 시킨 역활을 수행하는 실제 Worker 에 해당 
- Query Node ( SQL 과 관련된 Parsing 사용자 Frone End 제공 Plannner, CBO 통계 수집을 통해서 Query Accellarataion 을 수행하는 모듈 ) 
  - Accelleration 을 위해서는 Columnar 최대 최소값 분산 Unique 값의 종류등을 위해서 각 Query 를 실행할때에 이 값들을 통계로 사용해서 Planning 하는 거지 


일단적인 Planncer 와 같은 DB 모듈이 모두 구현되어야 해

- Storage Node 에는 LSM Merge 를 하는 모듈이 필요하고 Partition 을 통해서 파일이 구분되어야해
Partition 내에 있는 경우 LSM Meger 가 일어나는 거지 이를 위해서는 Key 를 받아야해 Create table 을 할때에

- Storage Node 는 Native LSM, S3, HDFS (Kerberos 인증 기능 필수 ) 기능이 들어가야해

- 이 DB 는 COlumnar DB 이며 각각의 Column 이 partition 단위로 나뉘어져 파일로 저장되어야 해

- Query Node 는 홀수개씩 Raft 를 통해서 Metadata 를 관리해야해 이해 되니 ?? Query Node 는 각 테이블의 metadata 를 관리하는데 이 metadata 는 node 들 사이에서 동기화 되어야 하니깐 

_query node 는 kubernetes 환경에서늬 접속을 위해서 어떤 노드에 붙어도 동일한 Endpoint 로 구현되도록 Stateless 해야 하고 이를 위해서 Metadata 정보가 통일되어 관리되어야 해 

Monitoring 기능을 통해서 전체 Cluster 의 상태를 제공해야 하고 

Profiler 를 통해서ㅗ Query 의 실행시간 을 최대 (1000) 개 까지 제공해야 해 

이 설명도 모두  이 스펙에 추가해주고 tc.md 에도 반영해줘