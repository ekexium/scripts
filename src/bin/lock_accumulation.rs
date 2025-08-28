//! Lock accumulation benchmark
//!
//! N threads repeat executing "begin optimistic; select * from t where id = 1 for update; commit;"
//! All errors are ignored to test lock accumulation and optimistic transaction behavior.

use clap::Parser;
use dmlddl::Result;
use futures::future::join_all;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::pool::PoolConnection;
use sqlx::{query, Executor, MySql};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Parser)]
#[command(name = "lock-accumulation")]
#[command(about = "Lock accumulation benchmark")]
struct Args {
    /// Database URL
    #[arg(short = 'u', long, default_value = "mysql://root@127.0.0.1:4000/test")]
    url: String,

    /// Number of threads
    #[arg(short = 't', long, default_value = "10")]
    threads: u32,

    /// Duration in seconds
    #[arg(short = 'd', long, default_value = "60")]
    duration: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!(
        "Starting {} threads for {} seconds",
        args.threads, args.duration
    );
    println!("Connecting to: {}", args.url);
    println!("Press Ctrl+C to stop early");

    let pool = MySqlPoolOptions::new()
        .max_connections(args.threads + 5)
        .connect(&args.url)
        .await?;

    // Setup table if needed
    let mut conn = pool.acquire().await?;
    let _ = conn
        .execute("create table if not exists t (id int primary key, v int)")
        .await;
    let _ = conn.execute("insert ignore into t values (1, 0)").await;
    drop(conn);

    let counter = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    let mut handles = vec![];
    for thread_id in 0..args.threads {
        let pool_clone = pool.clone();
        let counter_clone = counter.clone();

        handles.push(tokio::spawn(async move {
            if let Ok(conn) = pool_clone.acquire().await {
                worker(thread_id, conn, counter_clone, args.duration).await;
            }
        }));
    }

    // Stats reporting task
    let stats_counter = counter.clone();
    let stats_handle = tokio::spawn(async move {
        let mut last_count = 0;
        loop {
            sleep(Duration::from_secs(5)).await;
            let current_count = stats_counter.load(Ordering::Relaxed);
            let rate = (current_count - last_count) / 5;
            println!("Total: {} transactions, Rate: {}/sec", current_count, rate);
            last_count = current_count;
        }
    });

    join_all(handles).await;
    stats_handle.abort();

    let total_count = counter.load(Ordering::Relaxed);
    let elapsed = start_time.elapsed();
    println!(
        "Completed {} transactions in {:.2}s, avg rate: {:.2}/sec",
        total_count,
        elapsed.as_secs_f64(),
        total_count as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

async fn worker(
    thread_id: u32,
    mut conn: PoolConnection<MySql>,
    counter: Arc<AtomicU64>,
    duration_secs: u64,
) {
    let start = Instant::now();
    let duration = Duration::from_secs(duration_secs);
    let mut local_count = 0u64;

    while start.elapsed() < duration {
        // Ignore all errors as requested
        let _ = conn.execute(query("begin optimistic")).await;
        let _ = conn
            .execute(query("select * from t where id = 1 for update"))
            .await;
        let _ = conn.execute(query("commit")).await;

        local_count += 1;
        counter.fetch_add(1, Ordering::Relaxed);

        // Report progress every 1000 transactions
        if local_count.is_multiple_of(1000) {
            println!(
                "Thread {}: {} transactions completed",
                thread_id, local_count
            );
        }
    }

    println!(
        "Thread {} finished with {} transactions",
        thread_id, local_count
    );
}
