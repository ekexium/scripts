use anyhow::Result;
use bench_autocommit::*;
use chrono::Local;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Acquire, Executor, MySql, Pool};
use statistical::mean;
use statistical::median;
use std::cmp::max;
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tokio::sync::Mutex;

#[derive(StructOpt, Debug)]
#[structopt(name = "tidb-benchmark")]
struct Opt {
    #[structopt(long, default_value = "localhost")]
    host: String,
    #[structopt(long, default_value = "10000000")]
    rows: u64,
    #[structopt(long, default_value = "1")]
    concurrency: u64,
    #[structopt(long, default_value = "30s", parse(try_from_str = parse_duration))]
    duration: Duration,
    #[structopt(long, default_value = "15s", parse(try_from_str = parse_duration))]
    operation_interval: Duration,
    #[structopt(long, default_value = "0ms", parse(try_from_str = parse_duration))]
    request_interval: Duration,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim().to_lowercase();
    if s.ends_with("ms") {
        let ms = u64::from_str(&s[..s.len() - 2]).map_err(|e| e.to_string())?;
        Ok(Duration::from_millis(ms))
    } else if s.ends_with('s') {
        let secs = u64::from_str(&s[..s.len() - 1]).map_err(|e| e.to_string())?;
        Ok(Duration::from_secs(secs))
    } else if s.ends_with('m') {
        let mins = u64::from_str(&s[..s.len() - 1]).map_err(|e| e.to_string())?;
        Ok(Duration::from_secs(mins * 60))
    } else {
        Err("Duration must end with ms, s, or m".to_string())
    }
}
#[derive(Debug)]
struct Metrics {
    operation: String,
    total_ops: u64,
    error_count: u64,
    latencies: VecDeque<(Duration, f64)>, // (start time, latency in ms)
}

impl Metrics {
    fn new(operation: &str) -> Self {
        Metrics {
            operation: operation.to_string(),
            total_ops: 0,
            error_count: 0,
            latencies: VecDeque::new(),
        }
    }

    fn add_latency(&mut self, timestamp: Duration, latency: f64) {
        self.latencies.push_back((timestamp, latency));
        self.total_ops += 1;
    }

    fn add_error(&mut self) {
        self.error_count += 1;
    }

    fn calculate_stats(
        &self,
        window_start: Duration,
        window_end: Duration,
    ) -> Option<(LatencyStats, f64)> {
        let window_latencies: Vec<f64> = self
            .latencies
            .iter()
            .filter(|(timestamp, _)| timestamp >= &window_start && timestamp <= &window_end)
            .map(|(_, latency)| *latency)
            .collect();

        if window_latencies.is_empty() {
            return None;
        }

        let mut sorted_latencies = window_latencies.clone();
        sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let throughput =
            window_latencies.len() as f64 / (window_end.as_secs_f64() - window_start.as_secs_f64());
        Some((
            LatencyStats {
                avg: mean(&sorted_latencies),
                median: median(&sorted_latencies),
                p95: percentile(&sorted_latencies, 95.0),
                p99: percentile(&sorted_latencies, 99.0),
                min: *sorted_latencies.first().unwrap(),
                max: *sorted_latencies.last().unwrap(),
            },
            throughput,
        ))
    }
}

#[derive(Debug)]
struct LatencyStats {
    avg: f64,
    median: f64,
    p95: f64,
    p99: f64,
    min: f64,
    max: f64,
}

#[derive(Debug)]
struct WorkloadState {
    remaining_rows: Arc<AtomicI64>,
    start_time: Instant,
    actual_duration_ms: Arc<AtomicU64>,
}

impl WorkloadState {
    fn new(total_rows: i64) -> Self {
        Self {
            remaining_rows: Arc::new(AtomicI64::new(total_rows)),
            start_time: Instant::now(),
            actual_duration_ms: Arc::new(AtomicU64::new(0)),
        }
    }
}

fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (p / 100.0 * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index]
}

async fn prepare_data(pool: Pool<MySql>, populate_data: bool, opts: &Opt) -> Result<()> {
    sqlx::query("DROP TABLE IF EXISTS benchmark_tbl")
        .execute(&pool)
        .await?;

    sqlx::query(
        "CREATE TABLE benchmark_tbl (
            id BIGINT PRIMARY KEY,
            k1 INT,
            k2 VARCHAR(64),
            v1 TEXT,
            created_at TIMESTAMP,
            key idx_k1(k1)
        )",
    )
    .execute(&pool)
    .await?;

    if !populate_data {
        return Ok(());
    }

    let batch_size = 10000;
    let num_workers = opts.concurrency;
    let rows = opts.rows;
    let rows_per_worker = (opts.rows + num_workers - 1) / num_workers;
    let progress = Arc::new(Mutex::new(0));
    let now = Local::now()
        .naive_local()
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();

    let mut handles = vec![];

    for worker_id in 0..num_workers {
        let pool = pool.clone();
        let progress = Arc::clone(&progress);
        let start_id = worker_id * rows_per_worker;
        let end_id = std::cmp::min((worker_id + 1) * rows_per_worker, opts.rows);
        let now = now.clone();

        let handle = tokio::spawn(async move {
            for i in (start_id..end_id).step_by(batch_size) {
                let chunk_size = std::cmp::min(batch_size as u64, end_id - i);
                let placeholders = (0..chunk_size)
                    .map(|_| "(?, ?, ?, ?, ?)")
                    .collect::<Vec<_>>()
                    .join(",");

                let query = format!(
                    "INSERT INTO benchmark_tbl (id, k1, k2, v1, created_at) VALUES {}",
                    placeholders
                );

                let mut query = sqlx::query(&query);

                for j in 0..chunk_size {
                    let id = i + j;
                    let scattered_k1 = scatter_id(id as i64, rows);
                    query = query
                        .bind(id as i64)
                        .bind(scattered_k1)
                        .bind(format!("key-{}", id))
                        .bind("initial-value")
                        .bind(&now);
                }

                query.execute(&pool).await?;

                let mut progress = progress.lock().await;
                *progress += chunk_size;
                print!("\rInserted {}/{} rows", *progress, rows);
                std::io::stdout().flush()?;
            }
            Ok::<(), anyhow::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }
    println!();
    Ok(())
}

async fn prepare_cluster(pool: Pool<MySql>, max_row_id: u64) -> Result<()> {
    println!("Preparing cluster...");

    let region_count = 64;

    println!("Pre-splitting table into {} regions...", region_count);
    let split_sql = format!(
        "SPLIT TABLE benchmark_tbl BETWEEN ({}) AND ({}) REGIONS {}",
        0, max_row_id, region_count
    );
    let mut conn = pool.acquire().await?;
    conn.execute("set tidb_wait_split_region_finish = true")
        .await?;
    conn.execute(split_sql.as_str()).await?;

    println!("Running analyze...");
    conn.execute("ANALYZE TABLE benchmark_tbl").await?;
    Ok(())
}

async fn run_single_benchmark(
    opts: &Opt,
    pessimistic: bool,
    operation_idx: usize,
) -> Result<Metrics> {
    let name = match operation_idx {
        0 => "insert",
        1 => "point_update",
        2 => "range_update",
        3 => "point_delete",
        4 => "range_delete",
        _ => unreachable!(),
    };
    println!(
        "\nPreparing benchmark for {} {}...",
        if pessimistic {
            "pessimistic"
        } else {
            "optimistic"
        },
        name
    );
    INSERT_COUNTER.store(0, Ordering::Relaxed);
    NEXT_DELETE_ID.store(0, Ordering::Relaxed);
    NEXT_DELETE_K1.store(0, Ordering::Relaxed);

    let url = format!("mysql://root@{}:4000/test", opts.host);
    let pool = MySqlPoolOptions::new()
        .max_connections(max(opts.concurrency, 3) as u32)
        .connect(url.as_str())
        .await?;

    if pessimistic {
        pool.execute("SET GLOBAL tidb_pessimistic_autocommit = 1")
            .await?;
    } else {
        pool.execute("SET GLOBAL tidb_pessimistic_autocommit = 0")
            .await?;
    }

    if name == "insert" {
        // special preparation for inserts
        prepare_data(pool.clone(), false, &opts).await?;
        prepare_cluster(pool.clone(), MAX_ROW_ID_FOR_INSERT).await?;
    } else {
        prepare_data(pool.clone(), true, &opts).await?;
        prepare_cluster(pool.clone(), opts.rows).await?;
    }

    let metrics = Arc::new(Mutex::new(Metrics::new(name)));

    let state = Arc::new(WorkloadState::new(opts.rows as i64));
    println!("Sleeping for {:?}...", opts.operation_interval);
    tokio::time::sleep(opts.operation_interval).await;
    println!("Benchmarking {}...", name);

    let duration = opts.duration;
    let rows = opts.rows;
    let request_interval = opts.request_interval;
    let window_start = duration / 4;
    let window_end = duration * 3 / 4;
    let case_start_instant = Instant::now();

    let mut handles = vec![];

    for thread_id in 0..opts.concurrency {
        let pool = pool.clone();
        let metrics = Arc::clone(&metrics);
        let state = Arc::clone(&state);
        let range = ThreadRange::new(thread_id, opts.concurrency, opts.rows);

        let handle = tokio::spawn(async move {
            let mut rng = SmallRng::from_entropy();
            let start_time = Instant::now();

            while (start_time.elapsed() < duration)
                && (operation_idx < 3 || state.remaining_rows.load(Ordering::Relaxed) > 0)
            {
                let op_start = Instant::now();
                let conn = pool.acquire().await;
                match conn {
                    Ok(mut conn) => {
                        let conn = conn.acquire().await.unwrap();
                        let result = match operation_idx {
                            0 => run_insert_workload(conn, MAX_ROW_ID_FOR_INSERT).await,
                            1 => run_point_update_workload(conn, &mut rng, &range).await,
                            2 => run_range_update_workload(conn, &mut rng, &range).await,
                            3 => run_point_delete_workload(conn, rows).await,
                            4 => run_range_delete_workload(conn, rows).await,
                            _ => unreachable!(),
                        };
                        match result {
                            Ok(_) => {
                                let latency = op_start.elapsed().as_micros() as f64 / 1000.0;
                                metrics
                                    .lock()
                                    .await
                                    .add_latency(case_start_instant.elapsed(), latency);
                            }
                            Err(e) => {
                                metrics.lock().await.add_error();
                                eprintln!("Error: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        metrics.lock().await.add_error();
                        eprintln!("Error: {:?}", e);
                    }
                }

                if request_interval.as_millis() > 0 {
                    tokio::time::sleep(request_interval).await;
                }
            }

            if state.remaining_rows.load(Ordering::Relaxed) <= 0 {
                state.actual_duration_ms.store(
                    state.start_time.elapsed().as_millis() as u64,
                    Ordering::Relaxed,
                );
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    let metrics = Arc::try_unwrap(metrics).unwrap().into_inner();
    if let Some((stats, throughput)) = metrics.calculate_stats(window_start, window_end) {
        println!(
            "\nLatency Statistics ({}ms - {}ms window):",
            window_start.as_millis(),
            window_end.as_millis()
        );
        println!("Average: {:.2}ms", stats.avg);
        println!("P95:     {:.2}ms", stats.p95);
        println!("Throughput: {:.2} ops/sec", throughput);
    }

    Ok(metrics)
}

struct BenchmarkResults {
    optimistic: Vec<Metrics>,
    pessimistic: Vec<Metrics>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let opts = Opt::from_args();

    let mut optimistic_metrics = Vec::new();
    let mut pessimistic_metrics = Vec::new();

    for operation_idx in 0..5 {
        let metric = run_single_benchmark(&opts, false, operation_idx).await?;
        optimistic_metrics.push(metric);
        let metric = run_single_benchmark(&opts, true, operation_idx).await?;
        pessimistic_metrics.push(metric);
    }

    let results = BenchmarkResults {
        optimistic: optimistic_metrics,
        pessimistic: pessimistic_metrics,
    };

    println!("\nOutputting results...");
    output_comparative_results(&results, &opts)?;

    Ok(())
}

fn output_comparative_results(results: &BenchmarkResults, opts: &Opt) -> Result<()> {
    let timestamp = Local::now().format("%Y%m%d_%H%M%S");
    let filename = format!("benchmark_results_{}.csv", timestamp);
    let mut file = File::create(&filename)?;

    // 定义统计窗口
    let window_start = Duration::from_secs(opts.duration.as_secs() / 4);
    let window_end = Duration::from_secs(opts.duration.as_secs() * 3 / 4);

    writeln!(
        file,
        "operation,metric,optimistic,pessimistic,difference,percent_difference"
    )?;

    println!(
        "\nComparative Benchmark Results ({}s - {}s window)\n",
        window_start.as_secs(),
        window_end.as_secs()
    );

    for i in 0..results.optimistic.len() {
        let opt_metrics = &results.optimistic[i];
        let pess_metrics = &results.pessimistic[i];

        let (opt_stats, opt_throughput) = opt_metrics
            .calculate_stats(window_start, window_end)
            .expect("No data in window for optimistic test");
        let (pess_stats, pess_throughput) = pess_metrics
            .calculate_stats(window_start, window_end)
            .expect("No data in window for pessimistic test");

        println!("Operation: {}", opt_metrics.operation);
        println!("{:-<65}", "");

        println!("Latencies (ms) and throughtpu (op/s):");
        println!(
            "{:<15} {:>12} {:>12} {:>12} {:>12}",
            "Metric", "Optimistic", "Pessimistic", "Difference", "% Difference"
        );
        println!("{:-<65}", "");

        let metrics = [
            ("Mean", opt_stats.avg, pess_stats.avg),
            ("P95", opt_stats.p95, pess_stats.p95),
            ("Throughput", opt_throughput, pess_throughput),
        ];

        for (name, opt_val, pess_val) in metrics.iter() {
            let diff = opt_val - pess_val;
            let pct = (diff / pess_val) * 100.0;

            println!(
                "{:<15} {:>12.2} {:>12.2} {:>12.2} {:>11.2}%",
                name, opt_val, pess_val, diff, pct
            );

            writeln!(
                file,
                "{},{},{:.2},{:.2},{:.2},{:.2}",
                opt_metrics.operation,
                name.to_lowercase(),
                opt_val,
                pess_val,
                diff,
                pct
            )?;
        }

        println!("\nErrors:");
        println!(
            "Optimistic: {}, Pessimistic: {}",
            opt_metrics.error_count, pess_metrics.error_count
        );

        writeln!(
            file,
            "{},errors,{},{},{},{}",
            opt_metrics.operation,
            opt_metrics.error_count,
            pess_metrics.error_count,
            opt_metrics.error_count as i64 - pess_metrics.error_count as i64,
            if pess_metrics.error_count > 0 {
                ((opt_metrics.error_count as f64 - pess_metrics.error_count as f64)
                    / pess_metrics.error_count as f64)
                    * 100.0
            } else {
                0.0
            }
        )?;

        println!("\n{:=<65}\n", "");
    }

    println!("Detailed results have been saved to {}", filename);
    Ok(())
}
