// write a million rows.
// the i-th row: <i 2*i>

use dmlddl::Result;
use log::LevelFilter;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::Executor;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

static COUNTER: AtomicU32 = AtomicU32::new(0);

#[tokio::main]
async fn main() -> Result<()> {
    simple_logging::log_to_file("dmlddl.log", LevelFilter::Info)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(500)
        .connect("mysql://root@127.0.0.1:4000/test")
        .await?;
    let pool = Arc::new(pool);

    let mut conn = pool.acquire().await?;
    conn.execute("use test").await?;
    conn.execute("drop table if exists t").await?;
    conn.execute("create table t(a int primary key, b int)")
        .await?;
    drop(conn);

    let mut handles = Vec::new();
    let batch_size = 100;
    let max = 10_000_000 / batch_size;
    for _ in 0..32 {
        let mut conn = pool.acquire().await?;
        handles.push(tokio::spawn(async move {
            let x = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if x >= max {
                return;
            }
            conn.execute(
                format!(
                    "insert into t values {}",
                    (0..batch_size)
                        .map(|y| format!("({}, {})", x * batch_size + y, (x * batch_size + y) * 2))
                        .collect::<Vec<String>>()
                        .join(","),
                )
                .as_str(),
            )
            .await
            .expect("insert failed");
        }));
    }
    for handle in handles {
        handle.await.expect("spawn failed");
    }
    Ok(())
}
