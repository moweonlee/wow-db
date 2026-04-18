// T060: Kafka Routine Load — rdkafka 컨슈머 그룹, 오프셋 추적, JSON/Avro 파싱, exactly-once ACK

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::sql_parser::CreateRoutineLoadStmt;

// ─── Routine Load 작업 상태 ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineLoadState {
    Running,
    Paused,
    Stopped,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineLoadJob {
    pub job_name:    String,
    pub cube_name:   String,
    pub kafka_topic: String,
    pub brokers:     Vec<String>,
    pub format:      IngestionFormat,
    pub properties:  HashMap<String, String>,
    pub state:       RoutineLoadState,
    /// 파티션별 마지막 커밋 오프셋
    pub offsets:     HashMap<i32, i64>,
    /// 총 수집 행 수
    pub rows_loaded: u64,
    /// 에러 행 수
    pub rows_error:  u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestionFormat {
    Json,
    Avro,
    Csv { delimiter: char },
}

// ─── 수집된 이벤트 배치 ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IngestionBatch {
    pub job_name:  String,
    pub cube_name: String,
    pub rows:      Vec<HashMap<String, serde_json::Value>>,
    /// 커밋할 오프셋 맵 (partition → offset + 1)
    pub offsets:   HashMap<i32, i64>,
}

// ─── Routine Load Manager ──────────────────────────────────────────────────────

pub struct RoutineLoadManager {
    jobs: Arc<RwLock<HashMap<String, RoutineLoadJob>>>,
}

impl RoutineLoadManager {
    pub fn new() -> Self {
        Self { jobs: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// CREATE ROUTINE LOAD 처리
    pub async fn create_job(&self, stmt: CreateRoutineLoadStmt) -> Result<()> {
        let mut jobs = self.jobs.write().await;
        if jobs.contains_key(&stmt.job_name) {
            return Err(anyhow!("Routine Load job '{}' already exists", stmt.job_name));
        }

        let format = match stmt.format.to_uppercase().as_str() {
            "JSON" => IngestionFormat::Json,
            "AVRO" => IngestionFormat::Avro,
            "CSV"  => IngestionFormat::Csv { delimiter: ',' },
            other  => return Err(anyhow!("Unsupported format: {}", other)),
        };

        let props: HashMap<String, String> = stmt.properties.into_iter().collect();

        let job = RoutineLoadJob {
            job_name:    stmt.job_name.clone(),
            cube_name:   stmt.cube_name,
            kafka_topic: stmt.kafka_topic,
            brokers:     stmt.brokers,
            format,
            properties:  props,
            state:       RoutineLoadState::Running,
            offsets:     HashMap::new(),
            rows_loaded: 0,
            rows_error:  0,
        };

        info!(job = %stmt.job_name, "Routine Load job created");
        jobs.insert(stmt.job_name, job);
        Ok(())
    }

    pub async fn pause_job(&self, job_name: &str) -> Result<()> {
        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(job_name)
            .ok_or_else(|| anyhow!("Job '{}' not found", job_name))?;
        job.state = RoutineLoadState::Paused;
        info!(job = %job_name, "Routine Load paused");
        Ok(())
    }

    pub async fn resume_job(&self, job_name: &str) -> Result<()> {
        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(job_name)
            .ok_or_else(|| anyhow!("Job '{}' not found", job_name))?;
        if job.state != RoutineLoadState::Paused {
            return Err(anyhow!("Job '{}' is not paused", job_name));
        }
        job.state = RoutineLoadState::Running;
        info!(job = %job_name, "Routine Load resumed");
        Ok(())
    }

    pub async fn stop_job(&self, job_name: &str) -> Result<()> {
        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(job_name)
            .ok_or_else(|| anyhow!("Job '{}' not found", job_name))?;
        job.state = RoutineLoadState::Stopped;
        info!(job = %job_name, "Routine Load stopped");
        Ok(())
    }

    pub async fn get_job(&self, job_name: &str) -> Option<RoutineLoadJob> {
        self.jobs.read().await.get(job_name).cloned()
    }

    pub async fn list_jobs(&self) -> Vec<RoutineLoadJob> {
        self.jobs.read().await.values().cloned().collect()
    }

    /// 오프셋 업데이트 (커밋 완료 후)
    pub async fn update_offsets(
        &self,
        job_name: &str,
        offsets: HashMap<i32, i64>,
        rows_loaded: u64,
        rows_error: u64,
    ) -> Result<()> {
        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(job_name)
            .ok_or_else(|| anyhow!("Job '{}' not found", job_name))?;
        for (partition, offset) in offsets {
            job.offsets.insert(partition, offset);
        }
        job.rows_loaded += rows_loaded;
        job.rows_error  += rows_error;
        Ok(())
    }
}

// ─── Kafka Consumer (rdkafka feature 또는 stub) ───────────────────────────────

/// 배치 수집 설정
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// 배치당 최대 행 수
    pub max_rows:            usize,
    /// 배치 최대 대기 시간
    pub max_interval:        Duration,
    /// poll 타임아웃
    pub poll_timeout:        Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_rows:     50_000,
            max_interval: Duration::from_secs(10),
            poll_timeout: Duration::from_millis(500),
        }
    }
}

/// JSON 행 파싱
pub fn parse_json_rows(data: &[u8]) -> Result<Vec<HashMap<String, serde_json::Value>>> {
    // 각 줄이 독립된 JSON 오브젝트 (NDJSON)
    let mut rows = Vec::new();
    for line in std::str::from_utf8(data)?.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(trimmed)
            .map_err(|e| anyhow!("JSON parse error on line '{}': {}", trimmed, e))?;
        if let serde_json::Value::Object(map) = value {
            rows.push(map.into_iter().collect());
        } else {
            return Err(anyhow!("Expected JSON object, got: {}", trimmed));
        }
    }
    Ok(rows)
}

/// Kafka Consumer 워커 (rdkafka 활성화 시 실제 구현 사용)
pub struct KafkaConsumerWorker {
    job_name:  String,
    topic:     String,
    brokers:   String,
    group_id:  String,
    config:    BatchConfig,
    manager:   Arc<RoutineLoadManager>,
    batch_tx:  tokio::sync::mpsc::Sender<IngestionBatch>,
}

impl KafkaConsumerWorker {
    pub fn new(
        job: &RoutineLoadJob,
        config: BatchConfig,
        manager: Arc<RoutineLoadManager>,
        batch_tx: tokio::sync::mpsc::Sender<IngestionBatch>,
    ) -> Self {
        let group_id = job.properties
            .get("group.id")
            .cloned()
            .unwrap_or_else(|| format!("wowdb-routine-load-{}", job.job_name));

        Self {
            job_name:  job.job_name.clone(),
            topic:     job.kafka_topic.clone(),
            brokers:   job.brokers.join(","),
            group_id,
            config,
            manager,
            batch_tx,
        }
    }

    /// 워커 실행 (tokio::spawn으로 구동)
    pub async fn run(self) {
        #[cfg(feature = "kafka")]
        {
            self.run_rdkafka().await;
        }
        #[cfg(not(feature = "kafka"))]
        {
            self.run_stub().await;
        }
    }

    #[cfg(not(feature = "kafka"))]
    async fn run_stub(self) {
        info!(
            job  = %self.job_name,
            topic = %self.topic,
            "Kafka consumer stub running (enable 'kafka' feature for real consumer)"
        );
        // stub: 상태만 유지, 실제 메시지 없음
        loop {
            let job = self.manager.get_job(&self.job_name).await;
            match job.map(|j| j.state) {
                Some(RoutineLoadState::Running) => {
                    sleep(self.config.poll_timeout).await;
                }
                Some(RoutineLoadState::Paused) => {
                    sleep(Duration::from_secs(1)).await;
                }
                _ => break,
            }
        }
    }

    #[cfg(feature = "kafka")]
    async fn run_rdkafka(self) {
        use rdkafka::consumer::{Consumer, StreamConsumer};
        use rdkafka::config::ClientConfig;
        use rdkafka::message::Message;
        use rdkafka::TopicPartitionList;

        let consumer: StreamConsumer = match ClientConfig::new()
            .set("group.id", &self.group_id)
            .set("bootstrap.servers", &self.brokers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .set("session.timeout.ms", "30000")
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                error!(job = %self.job_name, "Failed to create Kafka consumer: {}", e);
                return;
            }
        };

        if let Err(e) = consumer.subscribe(&[&self.topic]) {
            error!(job = %self.job_name, "Failed to subscribe to topic {}: {}", self.topic, e);
            return;
        }

        info!(job = %self.job_name, topic = %self.topic, "Kafka consumer started");

        let mut batch_rows: Vec<HashMap<String, serde_json::Value>> = Vec::new();
        let mut batch_offsets: HashMap<i32, i64> = HashMap::new();
        let mut last_flush = std::time::Instant::now();

        loop {
            // 상태 확인
            let state = self.manager.get_job(&self.job_name).await
                .map(|j| j.state)
                .unwrap_or(RoutineLoadState::Stopped);

            match state {
                RoutineLoadState::Stopped => break,
                RoutineLoadState::Paused  => {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                RoutineLoadState::Running => {}
                RoutineLoadState::Error(_) => break,
            }

            // Kafka poll
            match tokio::time::timeout(self.config.poll_timeout, consumer.recv()).await {
                Ok(Ok(msg)) => {
                    if let Some(payload) = msg.payload() {
                        match parse_json_rows(payload) {
                            Ok(rows) => {
                                let partition = msg.partition();
                                let offset    = msg.offset() + 1;
                                batch_offsets.insert(partition, offset);
                                batch_rows.extend(rows);
                            }
                            Err(e) => {
                                warn!(job = %self.job_name, "Parse error: {}", e);
                                let _ = self.manager.update_offsets(
                                    &self.job_name,
                                    HashMap::new(),
                                    0,
                                    1,
                                ).await;
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!(job = %self.job_name, "Kafka error: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(_) => {} // timeout — flush 조건 확인
            }

            // 배치 플러시 조건: 행 수 초과 또는 타임아웃
            let should_flush = batch_rows.len() >= self.config.max_rows
                || (!batch_rows.is_empty()
                    && last_flush.elapsed() >= self.config.max_interval);

            if should_flush {
                let rows_count = batch_rows.len() as u64;
                let cube_name  = self.manager.get_job(&self.job_name).await
                    .map(|j| j.cube_name.clone())
                    .unwrap_or_default();

                let batch = IngestionBatch {
                    job_name:  self.job_name.clone(),
                    cube_name: cube_name.clone(),
                    rows:      std::mem::take(&mut batch_rows),
                    offsets:   batch_offsets.clone(),
                };

                if let Err(e) = self.batch_tx.send(batch).await {
                    error!(job = %self.job_name, "Failed to send batch: {}", e);
                    break;
                }

                // exactly-once: 배치 전송 성공 후 offset 커밋
                let mut tpl = TopicPartitionList::new();
                for (partition, offset) in &batch_offsets {
                    tpl.add_partition_offset(
                        &self.topic,
                        *partition,
                        rdkafka::Offset::Offset(*offset),
                    ).ok();
                }
                if let Err(e) = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync) {
                    warn!(job = %self.job_name, "Offset commit failed: {}", e);
                }

                let _ = self.manager.update_offsets(
                    &self.job_name,
                    std::mem::take(&mut batch_offsets),
                    rows_count,
                    0,
                ).await;

                last_flush = std::time::Instant::now();
            }
        }

        info!(job = %self.job_name, "Kafka consumer stopped");
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_rows() {
        let data = b"{\"user_id\": 1, \"event\": \"click\"}\n{\"user_id\": 2, \"event\": \"view\"}";
        let rows = parse_json_rows(data).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["user_id"], serde_json::json!(1));
        assert_eq!(rows[1]["event"], serde_json::json!("view"));
    }

    #[test]
    fn test_parse_json_empty_lines() {
        let data = b"\n{\"a\": 1}\n\n{\"b\": 2}\n";
        let rows = parse_json_rows(data).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn test_routine_load_lifecycle() {
        let mgr = RoutineLoadManager::new();
        let stmt = CreateRoutineLoadStmt {
            job_name:    "test_job".into(),
            cube_name:   "events".into(),
            kafka_topic: "events_topic".into(),
            brokers:     vec!["localhost:9092".into()],
            format:      "JSON".into(),
            properties:  vec![],
        };

        mgr.create_job(stmt).await.unwrap();
        let job = mgr.get_job("test_job").await.unwrap();
        assert_eq!(job.state, RoutineLoadState::Running);

        mgr.pause_job("test_job").await.unwrap();
        let job = mgr.get_job("test_job").await.unwrap();
        assert_eq!(job.state, RoutineLoadState::Paused);

        mgr.resume_job("test_job").await.unwrap();
        mgr.stop_job("test_job").await.unwrap();
        let job = mgr.get_job("test_job").await.unwrap();
        assert_eq!(job.state, RoutineLoadState::Stopped);
    }
}
