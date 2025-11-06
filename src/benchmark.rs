use anyhow::{Result, anyhow};
use postgres_native_tls::MakeTlsConnector;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng, distributions::Alphanumeric};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time;
use tokio_postgres::Client;
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

use crate::config::{Config, SingleInsertTest};

const RECONNECT_DELAY_MS: u64 = 300;
const TEST_DELAY_SECS: u64 = 5;
const NOTE_LENGTH: usize = 16;
const MAX_QUANTITY: i64 = 1000;
const MAX_AMOUNT: f64 = 1000.0;
const ACTIVE_PROBABILITY: f64 = 0.9;

type BatchData = (i64, String, String, serde_json::Value, i64, f64, bool);

/// Encapsulates shared metrics counters for cleaner code
/// Each worker maintains a persistent connection throughout the test.
/// If a connection breaks, we reconnect once but don't retry failed batches.
#[derive(Clone)]
struct Metrics {
    total_rows: Arc<AtomicU64>,
    successful_writes: Arc<AtomicU64>,
    failed_writes: Arc<AtomicU64>,
    total_batches: Arc<AtomicU64>,
    total_batch_latency_ms: Arc<AtomicU64>,
    total_marshal_ms: Arc<AtomicU64>,
    total_execute_ms: Arc<AtomicU64>,
    total_ping_ms: Arc<AtomicU64>,
    ping_count: Arc<AtomicU64>,
    // Error categorization
    timeout_errors: Arc<AtomicU64>,
    connection_errors: Arc<AtomicU64>,
    other_errors: Arc<AtomicU64>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            total_rows: Arc::new(AtomicU64::new(0)),
            successful_writes: Arc::new(AtomicU64::new(0)),
            failed_writes: Arc::new(AtomicU64::new(0)),
            total_batches: Arc::new(AtomicU64::new(0)),
            total_batch_latency_ms: Arc::new(AtomicU64::new(0)),
            total_marshal_ms: Arc::new(AtomicU64::new(0)),
            total_execute_ms: Arc::new(AtomicU64::new(0)),
            total_ping_ms: Arc::new(AtomicU64::new(0)),
            ping_count: Arc::new(AtomicU64::new(0)),
            timeout_errors: Arc::new(AtomicU64::new(0)),
            connection_errors: Arc::new(AtomicU64::new(0)),
            other_errors: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record_success(&self, rows: u64) {
        self.total_rows.fetch_add(rows, Ordering::Relaxed);
        self.successful_writes.fetch_add(rows, Ordering::Relaxed);
    }

    fn record_failure(&self, rows: u64) {
        self.failed_writes.fetch_add(rows, Ordering::Relaxed);
    }

    fn record_timeout_error(&self) {
        self.timeout_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_other_error(&self) {
        self.other_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_batch_success(
        &self,
        rows: u64,
        batch_latency_ms: u64,
        marshal_ms: u64,
        execute_ms: u64,
    ) {
        self.record_success(rows);
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        self.total_batch_latency_ms
            .fetch_add(batch_latency_ms, Ordering::Relaxed);
        self.total_marshal_ms
            .fetch_add(marshal_ms, Ordering::Relaxed);
        self.total_execute_ms
            .fetch_add(execute_ms, Ordering::Relaxed);
    }

    fn record_ping(&self, ping_ms: u64) {
        self.total_ping_ms.fetch_add(ping_ms, Ordering::Relaxed);
        self.ping_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_rows: self.total_rows.load(Ordering::Relaxed),
            successful: self.successful_writes.load(Ordering::Relaxed),
            failed: self.failed_writes.load(Ordering::Relaxed),
            total_batches: self.total_batches.load(Ordering::Relaxed),
            total_batch_latency_ms: self.total_batch_latency_ms.load(Ordering::Relaxed),
            total_marshal_ms: self.total_marshal_ms.load(Ordering::Relaxed),
            total_execute_ms: self.total_execute_ms.load(Ordering::Relaxed),
            total_ping_ms: self.total_ping_ms.load(Ordering::Relaxed),
            ping_count: self.ping_count.load(Ordering::Relaxed),
            timeout_errors: self.timeout_errors.load(Ordering::Relaxed),
            connection_errors: self.connection_errors.load(Ordering::Relaxed),
            other_errors: self.other_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy)]
struct MetricsSnapshot {
    total_rows: u64,
    successful: u64,
    failed: u64,
    total_batches: u64,
    total_batch_latency_ms: u64,
    total_marshal_ms: u64,
    total_execute_ms: u64,
    total_ping_ms: u64,
    ping_count: u64,
    timeout_errors: u64,
    connection_errors: u64,
    other_errors: u64,
}

impl MetricsSnapshot {
    fn success_rate(&self) -> f64 {
        let total_attempts = self.successful + self.failed;
        if total_attempts > 0 {
            (self.successful as f64 / total_attempts as f64) * 100.0
        } else {
            100.0
        }
    }

    fn avg_marshal_ms(&self) -> f64 {
        if self.total_batches == 0 {
            0.0
        } else {
            self.total_marshal_ms as f64 / self.total_batches as f64
        }
    }

    fn avg_execute_ms(&self) -> f64 {
        if self.total_batches == 0 {
            0.0
        } else {
            self.total_execute_ms as f64 / self.total_batches as f64
        }
    }

    fn avg_ping_ms(&self) -> f64 {
        if self.ping_count == 0 {
            0.0
        } else {
            self.total_ping_ms as f64 / self.ping_count as f64
        }
    }
}

fn is_connection_error(msg: &str) -> bool {
    const CONNECTION_ERROR_PATTERNS: &[&str] =
        &["closed", "connection", "broken pipe", "reset", "EOF"];
    CONNECTION_ERROR_PATTERNS
        .iter()
        .any(|pattern| msg.contains(pattern))
}

/// Extract structured PostgreSQL error info (if available) from an anyhow::Error
struct PgErrorInfo {
    code: String,
    message: String,
    severity: Option<String>,
    detail: Option<String>,
    hint: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    column: Option<String>,
    constraint: Option<String>,
}

fn get_pg_error_info(error: &anyhow::Error) -> Option<PgErrorInfo> {
    if let Some(pg_err) = error.downcast_ref::<tokio_postgres::Error>() {
        if let Some(db_err) = pg_err.as_db_error() {
            return Some(PgErrorInfo {
                code: db_err.code().code().to_string(),
                message: db_err.message().to_string(),
                severity: Some(db_err.severity().to_string()),
                detail: db_err.detail().map(|s| s.to_string()),
                hint: db_err.hint().map(|s| s.to_string()),
                schema: db_err.schema().map(|s| s.to_string()),
                table: db_err.table().map(|s| s.to_string()),
                column: db_err.column().map(|s| s.to_string()),
                constraint: db_err.constraint().map(|s| s.to_string()),
            });
        }
    }
    None
}



async fn exec_batch(client: &mut Client, values: &[BatchData]) -> Result<()> {
    // One round trip per batch using column-wise arrays and UNNEST.
    if values.is_empty() {
        return Ok(());
    }

    let mut ks: Vec<i64> = Vec::with_capacity(values.len());
    let mut vs: Vec<String> = Vec::with_capacity(values.len());
    let mut v2s: Vec<String> = Vec::with_capacity(values.len());
    let mut details_vec: Vec<serde_json::Value> = Vec::with_capacity(values.len());
    let mut qtys: Vec<i64> = Vec::with_capacity(values.len());
    let mut amounts: Vec<f64> = Vec::with_capacity(values.len());
    let mut actives: Vec<bool> = Vec::with_capacity(values.len());

    for (k, v2, note, details, qty, amount, active) in values.iter() {
        ks.push(*k);
        vs.push(note.clone());
        v2s.push(v2.clone());
        details_vec.push(details.clone());
        qtys.push(*qty);
        amounts.push(*amount);
        actives.push(*active);
    }

    let sql = r#"
        INSERT INTO bench_kv (k, v, v2, details, qty, amount, active)
        SELECT *
        FROM UNNEST(
            $1::INT8[],
            $2::TEXT[],
            $3::TEXT[],
            $4::JSONB[],
            $5::INT8[],
            $6::FLOAT8[],
            $7::BOOL[]
        )
    "#;

    client
        .execute(
            sql,
            &[&ks, &vs, &v2s, &details_vec, &qtys, &amounts, &actives],
        )
        .await
        .map_err(|e| anyhow!(e))?;

    Ok(())
}

/// Generate batch data into a pre-allocated buffer to avoid repeated allocations
fn generate_batch_data_into(
    rng: &mut StdRng,
    batch_size: usize,
    run_id: u64,
    worker_id: usize,
    seq_counter: &mut u64,
    buffer: &mut Vec<BatchData>,
) {
    for _ in 0..batch_size {
        // Construct a globally unique key for this run:
        // [16 bits run_id][16 bits worker_id][32 bits per-worker sequence]
        let k_u64: u64 = ((run_id & 0xFFFF) << 48)
            | (((worker_id as u64) & 0xFFFF) << 32)
            | (*seq_counter & 0xFFFF_FFFF);
        *seq_counter = seq_counter.wrapping_add(1);
        let k: i64 = k_u64 as i64;
        let v2 = Uuid::new_v4().to_string();
        let note: String = (0..NOTE_LENGTH)
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect();
        let details = json!({"note": note});
        let qty: i64 = rng.gen_range(0..MAX_QUANTITY);
        let amount: f64 = rng.r#gen::<f64>() * MAX_AMOUNT;
        let active: bool = rng.gen_bool(ACTIVE_PROBABILITY);
        buffer.push((k, v2, note, details, qty, amount, active));
    }
}

async fn establish_connection(
    db_url: &str,
    worker_id: usize,
    tls_connector: MakeTlsConnector,
) -> Client {
    loop {
        match tokio_postgres::connect(db_url, tls_connector.clone()).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        warn!(
                            error = %e,
                            worker = worker_id,
                            "connection_task_error"
                        );
                    }
                });
                return client;
            }
            Err(e) => {
                warn!(
                    error = %e,
                    worker = worker_id,
                    "initial_connect_failed_retrying"
                );
                time::sleep(Duration::from_millis(RECONNECT_DELAY_MS)).await;
            }
        }
    }
}

async fn execute_with_timeout<T>(
    future: impl std::future::Future<Output = Result<T>>,
    timeout_ms: u64,
) -> Result<T> {
    if timeout_ms > 0 {
        match time::timeout(Duration::from_millis(timeout_ms), future).await {
            Ok(result) => result,
            Err(_) => Err(anyhow!("timeout")),
        }
    } else {
        future.await
    }
}

/// Categorize error type for metrics
#[derive(Debug, Clone, Copy)]
enum ErrorType {
    Timeout,
    Connection,
    Other,
}

/// Per-worker statistics tracking
struct WorkerStatTracker {
    total_batches: u64,
    successful_batches: u64,
    failed_batches: u64,
    reconnection_count: u64,
    timeout_errors: u64,
    connection_errors: u64,
    other_errors: u64,
}

impl WorkerStatTracker {
    fn new() -> Self {
        Self {
            total_batches: 0,
            successful_batches: 0,
            failed_batches: 0,
            reconnection_count: 0,
            timeout_errors: 0,
            connection_errors: 0,
            other_errors: 0,
        }
    }
}

async fn handle_batch_error(
    error: anyhow::Error,
    worker_id: usize,
    timeout_ms: u64,
    db_url: &str,
    client: &mut Client,
    tls_connector: &MakeTlsConnector,
    metrics: &Metrics,
    worker_stats: &mut WorkerStatTracker,
) -> ErrorType {
    let msg = error.to_string();

    if msg.contains("timeout") {
        metrics.record_timeout_error();
        worker_stats.timeout_errors += 1;
        warn!(
            error = %error,
            worker_id,
            timeout_ms,
            "batch_timeout"
        );
        ErrorType::Timeout
    } else if is_connection_error(&msg) {
        metrics.record_connection_error();
        worker_stats.connection_errors += 1;
        warn!(
            error = %error,
            worker_id,
            "connection_error_reconnecting"
        );

        // Attempt reconnection once (no retries on failure)
        if let Ok((new_client, conn)) = tokio_postgres::connect(db_url, tls_connector.clone()).await
        {
            worker_stats.reconnection_count += 1;
            tokio::spawn(async move {
                if let Err(err) = conn.await {
                    warn!(
                        error = %err,
                        worker_id,
                        "connection_task_error"
                    );
                }
            });
            *client = new_client;
        } else {
            warn!(worker_id, "reconnection_failed");
        }
        ErrorType::Connection
    } else {
        metrics.record_other_error();
        worker_stats.other_errors += 1;
        if let Some(info) = get_pg_error_info(&error) {
            warn!(
                error = %error,
                worker_id,
                sqlstate = %info.code,
                pg_message = %info.message,
                severity = %info.severity.as_deref().unwrap_or(""),
                detail = %info.detail.as_deref().unwrap_or(""),
                hint = %info.hint.as_deref().unwrap_or(""),
                schema = %info.schema.as_deref().unwrap_or(""),
                table = %info.table.as_deref().unwrap_or(""),
                column = %info.column.as_deref().unwrap_or(""),
                constraint = %info.constraint.as_deref().unwrap_or(""),
                "batch_failed_other_error"
            );
        } else {
            // Non-DB error surfaced here; include cause chain
            let cause_chain: String = error
                .chain()
                .skip(1)
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(" | caused by: ");
            warn!(
                error = %error,
                worker_id,
                cause_chain = %cause_chain,
                "batch_failed_other_error"
            );
        }
        ErrorType::Other
    }
}

/// Worker configuration to avoid too many function parameters
struct WorkerConfig {
    worker_id: usize,
    db_url: Arc<str>,
    batch_size: usize,
    duration_secs: u64,
    timeout_ms: u64,
    metrics: Metrics,
    start: std::time::Instant,
    tls_connector: MakeTlsConnector,
    run_id: u64,
    shutdown: Arc<AtomicBool>,
}

async fn run_worker(config: WorkerConfig) {
    let WorkerConfig {
        worker_id,
        db_url,
        batch_size,
        duration_secs,
        timeout_ms,
        metrics,
        start,
        tls_connector,
        run_id,
        shutdown,
    } = config;

    let mut worker_stats = WorkerStatTracker::new();
    let mut rng = StdRng::from_entropy();
    let mut seq_counter: u64 = 0;
    
    // Pre-allocate batch data buffer to avoid repeated allocations
    let mut batch_data: Vec<BatchData> = Vec::with_capacity(batch_size);
    
    let mut client = establish_connection(&db_url, worker_id, tls_connector.clone()).await;
    let is_infinite = duration_secs == 0;

    loop {
        // Check for shutdown signal
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        if !is_infinite && start.elapsed() >= Duration::from_secs(duration_secs) {
            break;
        }

        let t_marshal0 = std::time::Instant::now();
        batch_data.clear();
        generate_batch_data_into(&mut rng, batch_size, run_id, worker_id, &mut seq_counter, &mut batch_data);
        let marshal_ms = t_marshal0.elapsed().as_millis() as u64;
        let batch_rows = batch_data.len() as u64;

        worker_stats.total_batches += 1;

        let t0 = std::time::Instant::now();
        let result = execute_with_timeout(exec_batch(&mut client, &batch_data), timeout_ms).await;
        let execute_ms = t0.elapsed().as_millis() as u64;

        match result {
            Ok(_) => {
                worker_stats.successful_batches += 1;
                metrics.record_batch_success(
                    batch_rows,
                    marshal_ms + execute_ms,
                    marshal_ms,
                    execute_ms,
                );
            }
            Err(e) => {
                worker_stats.failed_batches += 1;
                metrics.record_failure(batch_rows);
                handle_batch_error(
                    e,
                    worker_id,
                    timeout_ms,
                    &db_url,
                    &mut client,
                    &tls_connector,
                    &metrics,
                    &mut worker_stats,
                )
                .await;
            }
        }
    }
}

async fn run_single_insert_test(
    db_url: &str,
    test: &SingleInsertTest,
    tls_connector: MakeTlsConnector,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    let metrics = Metrics::new();
    let start = std::time::Instant::now();
    let test_name = format!("Worker {} Batch {}", test.workers, test.batch_size);
    let is_infinite = test.duration_secs == 0;
    
    // Set baseline immediately at start
    let baseline_snapshot = metrics.get_snapshot();

    // Spawn worker tasks (use Arc to avoid cloning URL string per worker)
    let db_url_arc: Arc<str> = Arc::from(db_url);
    let mut handles = Vec::with_capacity(test.workers);
    // Random run_id to make keys unique for this test across the suite
    let run_id: u64 = rand::random::<u64>() & 0xFFFF; // use 16 bits

    for worker_id in 0..test.workers {
        let worker_metrics = metrics.clone();
        let worker_tls = tls_connector.clone();
        let worker_db_url = Arc::clone(&db_url_arc);
        let worker_shutdown = Arc::clone(&shutdown);

        handles.push(tokio::spawn(run_worker(WorkerConfig {
            worker_id,
            db_url: worker_db_url,
            batch_size: test.batch_size as usize,
            duration_secs: test.duration_secs,
            timeout_ms: test.timeout_in_ms.unwrap_or(0),
            metrics: worker_metrics,
            start,
            tls_connector: worker_tls,
            run_id,
            shutdown: worker_shutdown,
        })));
    }

    // Background ping sampler and metrics reporting loop
    let (ping_host, ping_port) = Url::parse(db_url)
        .ok()
        .and_then(|url| Some((url.host_str()?.to_string(), url.port().unwrap_or(26257))))
        .unwrap_or_else(|| ("localhost".to_string(), 26257));
    let ping_metrics = metrics.clone();
    let ping_handle =
        tokio::spawn(async move { tcp_probe_task(ping_host, ping_port, ping_metrics).await });

    let metrics_handle = tokio::spawn(report_metrics_loop(
        metrics.clone(),
        test.clone(),
        test_name.clone(),
        start,
        is_infinite,
        baseline_snapshot,
    ));

    // Wait for all workers
    for handle in handles {
        let _ = handle.await;
    }

    // Stop metrics reporting
    metrics_handle.abort();
    let _ = metrics_handle.await;
    ping_handle.abort();
    let _ = ping_handle.await;

    // Final results
    let final_snapshot = metrics.get_snapshot();
    let total_duration = start.elapsed().as_secs_f64();

    log_final_results(
        &test_name,
        test,
        &final_snapshot,
        &baseline_snapshot,
        total_duration,
    );

    Ok(())
}

async fn report_metrics_loop(
    metrics: Metrics,
    test: SingleInsertTest,
    test_name: String,
    start: std::time::Instant,
    is_infinite: bool,
    baseline_snapshot: MetricsSnapshot,
) {
    let mut prev_snapshot = metrics.get_snapshot();

    loop {
        time::sleep(Duration::from_secs(test.metrics_interval_secs.max(1))).await;

        let current = metrics.get_snapshot();
        let elapsed = start.elapsed().as_secs_f64();

        // Calculate interval metrics
        let interval_successful = current.successful.saturating_sub(prev_snapshot.successful);
        let interval_failed = current.failed.saturating_sub(prev_snapshot.failed);
        let interval_secs = test.metrics_interval_secs.max(1) as f64;

        // Calculate overall metrics from start
        let successful = current.successful.saturating_sub(baseline_snapshot.successful);
        let total = current.total_rows.saturating_sub(baseline_snapshot.total_rows);
        let (overall_successful_per_sec, overall_attempts_per_sec) = if elapsed > 0.0 {
            (successful as f64 / elapsed, total as f64 / elapsed)
        } else {
            (0.0, 0.0)
        };

        // Interval and overall avg RTT (ms)
        let interval_batches = current
            .total_batches
            .saturating_sub(prev_snapshot.total_batches);
        let interval_latency_ms = current
            .total_batch_latency_ms
            .saturating_sub(prev_snapshot.total_batch_latency_ms);
        let _interval_avg_batch_rtt_ms = if interval_batches > 0 {
            interval_latency_ms as f64 / interval_batches as f64
        } else {
            0.0
        };
        // Compute per-interval averages for marshal/execute/ping
        let interval_marshal_ms = current
            .total_marshal_ms
            .saturating_sub(prev_snapshot.total_marshal_ms);
        let interval_execute_ms = current
            .total_execute_ms
            .saturating_sub(prev_snapshot.total_execute_ms);
        let interval_ping_ms = current
            .total_ping_ms
            .saturating_sub(prev_snapshot.total_ping_ms);
        let avg_marshal_ms = if interval_batches > 0 {
            interval_marshal_ms as f64 / interval_batches as f64
        } else {
            0.0
        };
        let avg_execute_ms = if interval_batches > 0 {
            interval_execute_ms as f64 / interval_batches as f64
        } else {
            0.0
        };
        let interval_ping_count = current.ping_count.saturating_sub(prev_snapshot.ping_count);
        let avg_ping_ms = if interval_ping_count > 0 {
            interval_ping_ms as f64 / interval_ping_count as f64
        } else {
            0.0
        };
        let (
            _overall_avg_batch_rtt_ms,
            overall_avg_marshal_ms,
            overall_avg_execute_ms,
            overall_avg_ping_ms,
        ) = {
            let batches = current.total_batches.saturating_sub(baseline_snapshot.total_batches);
            let lat_ms = current
                .total_batch_latency_ms
                .saturating_sub(baseline_snapshot.total_batch_latency_ms);
            let marshal_ms = current
                .total_marshal_ms
                .saturating_sub(baseline_snapshot.total_marshal_ms);
            let execute_ms = current
                .total_execute_ms
                .saturating_sub(baseline_snapshot.total_execute_ms);
            let ping_ms = current.total_ping_ms.saturating_sub(baseline_snapshot.total_ping_ms);
            let ping_count = current.ping_count.saturating_sub(baseline_snapshot.ping_count);
            if batches > 0 {
                let avg_ping = if ping_count > 0 {
                    ping_ms as f64 / ping_count as f64
                } else {
                    0.0
                };
                (
                    lat_ms as f64 / batches as f64,
                    marshal_ms as f64 / batches as f64,
                    execute_ms as f64 / batches as f64,
                    avg_ping,
                )
            } else {
                (0.0, 0.0, 0.0, 0.0)
            }
        };

        info!(
            event = "single_insert_tick",
            benchmark = %test_name,
            workers = test.workers,
            batch_size = test.batch_size,
            total_rows = current.total_rows,
            total_successful_writes = current.successful,
            total_failed_writes = current.failed,
            successful_writes_per_sec = if interval_secs > 0.0 { interval_successful as f64 / interval_secs } else { 0.0 },
            attempts_per_sec = if interval_secs > 0.0 { (interval_successful + interval_failed) as f64 / interval_secs } else { 0.0 },
            failed_writes = interval_failed,
            interval_secs,
            avg_marshal_ms,
            avg_execute_ms,
            avg_ping_ms,
            total_duration_secs = elapsed,
            overall_successful_writes_per_sec = overall_successful_per_sec,
            overall_attempts_per_sec,
            success_rate_percent = current.success_rate(),
            failure_rate_percent = 100.0 - current.success_rate(),
            avg_marshal_ms_overall = overall_avg_marshal_ms,
            avg_execute_ms_overall = overall_avg_execute_ms,
            avg_ping_ms_overall = overall_avg_ping_ms,
            timeout_errors = current.timeout_errors,
            connection_errors = current.connection_errors,
            other_errors = current.other_errors,
        );
        prev_snapshot = current;

        if !is_infinite && start.elapsed() >= Duration::from_secs(test.duration_secs) {
            break;
        }
    }
}

fn log_final_results(
    test_name: &str,
    test: &SingleInsertTest,
    snapshot: &MetricsSnapshot,
    baseline: &MetricsSnapshot,
    total_duration: f64,
) {
    let successful = snapshot.successful.saturating_sub(baseline.successful);
    let total = snapshot.total_rows.saturating_sub(baseline.total_rows);
    let overall_successful_per_sec = if total_duration > 0.0 {
        successful as f64 / total_duration
    } else {
        0.0
    };
    let overall_attempts_per_sec = if total_duration > 0.0 {
        total as f64 / total_duration
    } else {
        0.0
    };

    info!(
        event = "single_insert_done",
        benchmark = %test_name,
        workers = test.workers,
        batch_size = test.batch_size,
        total_rows = snapshot.total_rows,
        total_successful_writes = snapshot.successful,
        total_failed_writes = snapshot.failed,
        successful_writes_per_sec = 0.0,
        attempts_per_sec = 0.0,
        failed_writes = 0,
        interval_secs = 0.0,
        total_duration_secs = total_duration,
        overall_successful_writes_per_sec = overall_successful_per_sec,
        overall_attempts_per_sec,
        success_rate_percent = snapshot.success_rate(),
        failure_rate_percent = 100.0 - snapshot.success_rate(),
        avg_marshal_ms = 0.0,
        avg_execute_ms = 0.0,
        avg_ping_ms = 0.0,
        avg_marshal_ms_overall = snapshot.avg_marshal_ms(),
        avg_execute_ms_overall = snapshot.avg_execute_ms(),
        avg_ping_ms_overall = snapshot.avg_ping_ms(),
        timeout_errors = snapshot.timeout_errors,
        connection_errors = snapshot.connection_errors,
        other_errors = snapshot.other_errors,
    );
}

pub async fn run_single_insert_suite(cfg: &Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    // Simple TLS connector - actual TLS is controlled via connection string (sslmode=require/disable)
    let tls_connector = MakeTlsConnector::new(native_tls::TlsConnector::new()?);

    let enabled_tests: Vec<_> = cfg
        .single_insert
        .tests
        .iter()
        .enumerate()
        .filter(|(_, t)| t.enabled)
        .collect();

    // Validate that infinite tests are only at the end
    if enabled_tests.len() > 1 {
        for (idx, test) in enabled_tests.iter().take(enabled_tests.len() - 1) {
            if test.duration_secs == 0 {
                return Err(anyhow!(
                    "Error: infinite test (duration_secs=0) must be last (found at index {})",
                    idx
                ));
            }
        }
    }

    let total = enabled_tests.len();
    for (i, (_, test_cfg)) in enabled_tests.into_iter().enumerate() {
        // Check for shutdown signal
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        run_single_insert_test(&cfg.database.url, test_cfg, tls_connector.clone(), shutdown.clone()).await?;

        // Check for shutdown signal before waiting
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        if i + 1 < total {
            time::sleep(Duration::from_secs(TEST_DELAY_SECS)).await;
        }
    }

    Ok(())
}

async fn tcp_probe_task(host: String, port: u16, metrics: Metrics) {
    // Connect-probe once per second and record the connect latency in ms
    loop {
        let addr = format!("{}:{}", host, port);
        let t0 = std::time::Instant::now();
        let res = time::timeout(Duration::from_millis(750), TcpStream::connect(&addr)).await;
        if let Ok(Ok(_stream)) = res {
            let ms = t0.elapsed().as_millis() as u64;
            metrics.record_ping(ms);
        }
        // Close the connection immediately (drop TcpStream)
        time::sleep(Duration::from_secs(1)).await;
    }
}
