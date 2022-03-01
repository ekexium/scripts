use dmlddl::Result;
use futures::future::join_all;
use log::{error, info, LevelFilter};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{query, Executor, Row};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;

const NUM_WORKERS: usize = 20;

#[tokio::main]
async fn main() -> Result<()> {
    simple_logging::log_to_file("update.log", LevelFilter::Info)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(500)
        .connect("mysql://root@127.0.0.1:4000/test")
        .await?;
    let pool = Arc::new(pool);

    let mut conn = pool.acquire().await?;
    conn.execute("set @@global.tidb_txn_assertion_level=strict")
        .await?;
    conn.execute("set @@tidb_general_log=1").await?;
    conn.execute("use test").await?;
    conn.execute("drop table if exists cycle").await?;
    conn.execute(
        "create table cycle ( \
        pk  int not null primary key, \
        sk  int not null, \
        val int, \
        key cycle_sk_val(sk, val) \
        );",
    )
    .await?;
    conn.execute("insert into cycle values (1, 1, 1)").await?;
    drop(conn);

    let mut handles = Vec::new();

    // channel to report assertion failure, calling a early termination
    let (error_tx, mut error_rx) = tokio::sync::mpsc::channel(NUM_WORKERS);

    // channel to nofitify workers to stop
    let (end_tx, _) = tokio::sync::broadcast::channel(1);

    for _ in 0..NUM_WORKERS {
        let mut conn = pool.acquire().await?;
        let error_tx = error_tx.clone();
        let mut end_rx = end_tx.subscribe();
        let handle = tokio::spawn(async move {
            loop {
                if end_rx.try_recv().is_ok() {
                    break;
                }
                conn.execute("begin").await.expect("begin should not fail");
                // for update or not??
                let res = query("select val from cycle where sk = 1 for update")
                    .fetch_one(&mut conn)
                    .await
                    .unwrap();
                let val: i32 = res.get("val");
                let res = conn
                    .execute(format!("update cycle set val = {} where sk = 1;", val + 1).as_str())
                    .await;
                check_res(res, &error_tx).await;
                let res = conn.execute("commit").await;
                check_res(res, &error_tx).await;
            }
        });
        handles.push(handle);
    }

    select! {
        _ = error_rx.recv() => {
            info!("assertion failed");
        },
        _ = join_all(handles) => {
            error!("unexpected update finished");
        },
        _ = tokio::time::sleep(Duration::from_secs(60 * 60 * 24)) => {
            info!("time ends")
        }
    };
    end_tx.send(()).unwrap();
    Ok(())
}

async fn check_res(
    res: std::result::Result<sqlx::mysql::MySqlQueryResult, sqlx::Error>,
    end_tx: &tokio::sync::mpsc::Sender<()>,
) {
    if let Err(e) = res {
        info!("{:?}", e);
        if e.to_string().to_lowercase().contains("assertion") {
            error!("{:?}", e);
            end_tx.send(()).await.unwrap();
        }
    }
}
