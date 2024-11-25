use anyhow::Result;
use chrono::Local;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::query;
use sqlx::{Acquire, Executor, MySqlConnection};
use statistical::mean;
use statistical::median;
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use structopt::StructOpt;
use tokio::sync::Mutex;

#[derive(StructOpt, Debug)]
#[structopt(name = "tidb-benchmark")]
struct Opt {
    #[structopt(long, default_value = "localhost")]
    host: String,
    #[structopt(long, default_value = "10000000")]
    rows: u64,
    #[structopt(long, default_value = "8")]
    concurrency: u64,
    #[structopt(long, default_value = "60")]
    duration: u64,
    #[structopt(long, default_value = "60")]
    interval: u64,
}

#[derive(Debug)]
struct Metrics {
    operation: String,
    total_ops: u64,
    error_count: u64,
    duration_ms: u64,
    latencies: VecDeque<f64>,
}

impl Metrics {
    fn new(operation: &str) -> Self {
        Metrics {
            operation: operation.to_string(),
            total_ops: 0,
            error_count: 0,
            duration_ms: 0,
            latencies: VecDeque::new(),
        }
    }

    fn add_latency(&mut self, latency: f64) {
        self.latencies.push_back(latency);
        self.total_ops += 1;
    }

    fn add_error(&mut self) {
        self.error_count += 1;
    }

    fn calculate_stats(&self) -> (f64, f64, f64, f64, f64) {
        let mut sorted_latencies: Vec<f64> = self.latencies.iter().cloned().collect();
        sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let avg = mean(&sorted_latencies);
        let med = median(&sorted_latencies);
        let p95 = percentile(&sorted_latencies, 95.0);
        let p99 = percentile(&sorted_latencies, 99.0);
        let throughput = (self.total_ops as f64 * 1000.0) / self.duration_ms as f64;

        (avg, med, p95, p99, throughput)
    }
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

struct ThreadRange {
    start: u64,
    end: u64,
}

impl ThreadRange {
    fn new(thread_id: u64, total_threads: u64, total_rows: u64) -> Self {
        let range_size = total_rows / total_threads;
        let start = thread_id * range_size;
        let end = if thread_id == total_threads - 1 {
            total_rows
        } else {
            start + range_size
        };
        ThreadRange { start, end }
    }
}

fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (p / 100.0 * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index]
}

async fn prepare_data(opts: &Opt) -> Result<()> {
    let url = format!("mysql://root@{}:4000/test", opts.host);
    let pool = MySqlPoolOptions::new()
        .max_connections(opts.concurrency as u32 * 2)
        .connect(&url)
        .await?;

    sqlx::query("DROP TABLE IF EXISTS benchmark_tbl")
        .execute(&pool)
        .await?;

    sqlx::query(
        "CREATE TABLE benchmark_tbl (
            id BIGINT PRIMARY KEY,
            k1 INT,
            k2 VARCHAR(64),
            v1 TEXT,
            created_at TIMESTAMP
        )",
    )
    .execute(&pool)
    .await?;

    let batch_size = 1000;
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
                    query = query
                        .bind(id as i64)
                        .bind(id as i64 % 1000)
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

    println!("\nCreating index on k1...");
    sqlx::query("CREATE INDEX idx_k1 ON benchmark_tbl(k1)")
        .execute(&pool)
        .await?;

    Ok(())
}

static INSERT_COUNTER: AtomicI64 = AtomicI64::new(0);

async fn run_insert_workload(
    conn: &mut MySqlConnection,
    _rng: &mut SmallRng,
) -> Result<()> {
    let id = INSERT_COUNTER.fetch_add(1, Ordering::SeqCst);
    query("INSERT INTO benchmark_tbl (id, k1, k2, v1, created_at) VALUES (?, ?, ?, ?, NOW())")
        .bind(id)
        .bind(id % 1000)
        .bind(format!("key-{}", id))
        .bind("new-value")
        .execute(conn)
        .await?;
    Ok(())
}

async fn run_point_update_workload(
    conn: &mut MySqlConnection,
    rng: &mut SmallRng,
    range: &ThreadRange,
) -> Result<()> {
    let id = rng.gen_range(range.start..range.end);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();
    query("UPDATE benchmark_tbl SET v1 = ? WHERE id = ?")
        .bind(timestamp)
        .bind(id)
        .execute(conn)
        .await?;
    Ok(())
}

async fn run_range_update_workload(
    conn: &mut MySqlConnection,
    rng: &mut SmallRng,
    range: &ThreadRange,
) -> Result<()> {
    let start = rng.gen_range(range.start..(range.end - 10));
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();
    query("UPDATE benchmark_tbl SET v1 = ? WHERE id BETWEEN ? AND ?")
        .bind(timestamp)
        .bind(start)
        .bind(start + 10)
        .execute(conn)
        .await?;
    Ok(())
}

static NEXT_DELETE_ID: AtomicI64 = AtomicI64::new(0);

async fn run_point_delete_workload(
    conn: &mut MySqlConnection,
    rows: u64,
) -> Result<()> {
    let id = NEXT_DELETE_ID.fetch_add(1, Ordering::Relaxed);
    if id >= rows as i64 {
        return Ok(());
    }
    let result = query("DELETE FROM benchmark_tbl WHERE id = ?")
        .bind(id)
        .execute(conn)
        .await?;

    if result.rows_affected() > 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!(format!("No rows deleted {}", id)))
    }
}

async fn run_range_delete_workload(
    conn: &mut MySqlConnection,
    rows: u64,
) -> Result<()> {
    let batch_size = 10;
    let id = NEXT_DELETE_ID.fetch_add(batch_size, Ordering::Relaxed);
    if id >= rows as i64 {
        return Ok(());
    }

    let result = query("DELETE FROM benchmark_tbl WHERE id BETWEEN ? AND ?")
        .bind(id)
        .bind(id + batch_size - 1)
        .execute(conn)
        .await?;

    let affected = result.rows_affected() as i64;
    if affected > 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!(format!("No rows deleted {}", id)))
    }
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
    prepare_data(&opts).await?;
    INSERT_COUNTER.store(opts.rows as i64, Ordering::Relaxed);
    NEXT_DELETE_ID.store(0, Ordering::Relaxed);

    let url = format!("mysql://root@{}:4000/test", opts.host);
    let pool = MySqlPoolOptions::new()
        .max_connections(opts.concurrency as u32)
        .connect(url.as_str())
        .await?;

    if pessimistic {
        let mut conn = pool.acquire().await?;
        conn.execute("SET GLOBAL tidb_pessimistic_autocommit = 1")
            .await?;
    } else {
        let mut conn = pool.acquire().await?;
        conn.execute("SET GLOBAL tidb_pessimistic_autocommit = 0")
            .await?;
    }

    let metrics = Arc::new(Mutex::new(Metrics::new(name)));

    let state = Arc::new(WorkloadState::new(opts.rows as i64));

    println!("Sleeping for {} seconds...", opts.interval);
    tokio::time::sleep(tokio::time::Duration::from_secs(opts.interval)).await;
    println!("Benchmarking {}...", name);

    let duration = opts.duration;
    let rows = opts.rows;

    let mut handles = vec![];

    for thread_id in 0..opts.concurrency {
        let pool = pool.clone();
        let metrics = Arc::clone(&metrics);
        let state = Arc::clone(&state);
        let range = ThreadRange::new(thread_id, opts.concurrency, opts.rows);

        let handle = tokio::spawn(async move {
            let mut rng = SmallRng::from_entropy();
            let start_time = Instant::now();

            while start_time.elapsed().as_millis() < (duration * 1000).into()
                && (operation_idx < 3 || state.remaining_rows.load(Ordering::Relaxed) > 0)
            {
                let op_start = Instant::now();
                let conn = pool.acquire().await;
                match conn {
                    Ok(mut conn) => {
                        let conn = conn.acquire().await.unwrap();
                        let result = match operation_idx {
                            0 => run_insert_workload(conn, &mut rng).await,
                            1 => run_point_update_workload(conn, &mut rng, &range).await,
                            2 => run_range_update_workload(conn, &mut rng, &range).await,
                            3 => run_point_delete_workload(conn, rows).await,
                            4 => run_range_delete_workload(conn, rows).await,
                            _ => unreachable!(),
                        };
                        match result {
                            Ok(_) => {
                                let latency = op_start.elapsed().as_micros() as f64 / 1000.0;
                                metrics.lock().await.add_latency(latency);
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

    let mut metrics = Arc::try_unwrap(metrics).unwrap().into_inner();
    metrics.duration_ms = state
        .actual_duration_ms
        .load(Ordering::Relaxed)
        .max(state.start_time.elapsed().as_millis() as u64);

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

fn output_comparative_results(results: &BenchmarkResults, _opts: &Opt) -> Result<()> {
    let timestamp = Local::now().format("%Y%m%d_%H%M%S");
    let filename = format!("benchmark_results_{}.csv", timestamp);
    let mut file = File::create(&filename)?;

    writeln!(
        file,
        "operation,metric,optimistic,pessimistic,difference,percent_difference"
    )?;

    println!("\nComparative Benchmark Results\n");

    for i in 0..results.optimistic.len() {
        let opt_metrics = &results.optimistic[i];
        let pess_metrics = &results.pessimistic[i];

        let (opt_avg, opt_med, opt_p95, opt_p99, opt_throughput) = opt_metrics.calculate_stats();
        let (pess_avg, pess_med, pess_p95, pess_p99, pess_throughput) =
            pess_metrics.calculate_stats();

        println!("Operation: {}", opt_metrics.operation);
        println!("{:-<65}", "");

        println!("Throughput (operations/sec):");
        println!(
            "{:<15} {:>12} {:>12} {:>12} {:>12}",
            "Metric", "Optimistic", "Pessimistic", "Difference", "% Difference"
        );
        println!("{:-<65}", "");

        let throughput_diff = opt_throughput - pess_throughput;
        let throughput_pct = (throughput_diff / pess_throughput) * 100.0;

        println!(
            "{:<15} {:>12.2} {:>12.2} {:>12.2} {:>11.2}%\n",
            "Throughput", opt_throughput, pess_throughput, throughput_diff, throughput_pct
        );

        writeln!(
            file,
            "{},throughput,{:.2},{:.2},{:.2},{:.2}",
            opt_metrics.operation, opt_throughput, pess_throughput, throughput_diff, throughput_pct
        )?;

        println!("Latencies (ms):");
        println!(
            "{:<15} {:>12} {:>12} {:>12} {:>12}",
            "Metric", "Optimistic", "Pessimistic", "Difference", "% Difference"
        );
        println!("{:-<65}", "");

        let metrics = [
            ("Average", opt_avg, pess_avg),
            ("Median", opt_med, pess_med),
            ("P95", opt_p95, pess_p95),
            ("P99", opt_p99, pess_p99),
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
                "{},{}_latency,{:.2},{:.2},{:.2},{:.2}",
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
