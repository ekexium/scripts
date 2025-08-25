use rand::prelude::SmallRng;
use rand::Rng;
use sqlx::{query, MySqlConnection};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub static INSERT_COUNTER: AtomicI64 = AtomicI64::new(0);
pub static NEXT_DELETE_ID: AtomicI64 = AtomicI64::new(0);
pub static NEXT_DELETE_K1: AtomicI64 = AtomicI64::new(0);
pub const MAX_ROW_ID_FOR_INSERT: u64 = 0x7fffffff;

pub struct ThreadRange {
    start: u64,
    end: u64,
}

impl ThreadRange {
    pub fn new(thread_id: u64, total_threads: u64, total_rows: u64) -> Self {
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

// an 1-to-1 mapping function from 1~N to 1~N to scatter the sequential ID
pub fn scatter_id(sequential_id: i64, total_rows: u64) -> i64 {
    const MULTIPLIER: i64 = 16777619;

    let scattered = (sequential_id * MULTIPLIER) % (total_rows as i64);

    if scattered < 0 {
        scattered + total_rows as i64
    } else {
        scattered
    }
}

pub async fn run_point_delete_workload(
    conn: &mut MySqlConnection,
    rows: u64,
) -> anyhow::Result<()> {
    let id = NEXT_DELETE_ID.fetch_add(1, Ordering::Relaxed);
    if id >= rows as i64 {
        return Ok(());
    }
    let scattered_id = scatter_id(id, rows);

    let result = query("DELETE FROM benchmark_tbl WHERE k1 = ?")
        .bind(scattered_id)
        .execute(conn)
        .await?;

    if result.rows_affected() > 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!(format!(
            "No rows deleted for scattered_id={}",
            scattered_id
        )))
    }
}

pub async fn run_range_delete_workload(
    conn: &mut MySqlConnection,
    rows: u64,
) -> anyhow::Result<()> {
    let batch_size = 3;
    let k1_start = NEXT_DELETE_K1.fetch_add(batch_size, Ordering::Relaxed);
    if k1_start >= rows as i64 {
        return Ok(());
    }

    let result = query("DELETE FROM benchmark_tbl WHERE k1 BETWEEN ? AND ?")
        .bind(k1_start)
        .bind(k1_start + batch_size - 1)
        .execute(conn)
        .await?;

    let affected = result.rows_affected() as i64;
    if affected > 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!(format!(
            "No rows deleted for k1 range {} to {}",
            k1_start,
            k1_start + batch_size - 1
        )))
    }
}

// split by group, then scatter inside the group, to allow infinite insertions
pub fn scatter_for_pk(sequential_id: i64, total_rows: u64) -> i64 {
    let base = total_rows as i64;
    let region_id = sequential_id / base;
    let offset = scatter_id(sequential_id % base, total_rows);

    region_id * base + offset
}

pub async fn run_insert_workload(
    conn: &mut MySqlConnection,
    max_row_id: u64,
) -> anyhow::Result<()> {
    let sequential_id = INSERT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let scattered_id = scatter_for_pk(sequential_id, max_row_id);

    query("INSERT INTO benchmark_tbl (id, k1, k2, v1, created_at) VALUES (?, ?, ?, ?, NOW())")
        .bind(scattered_id)
        .bind(scattered_id % 1000)
        .bind(format!("key-{}", scattered_id))
        .bind("new-value")
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn run_point_update_workload(
    conn: &mut MySqlConnection,
    rng: &mut SmallRng,
    range: &ThreadRange,
) -> anyhow::Result<()> {
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

pub async fn run_range_update_workload(
    conn: &mut MySqlConnection,
    rng: &mut SmallRng,
    range: &ThreadRange,
) -> anyhow::Result<()> {
    let start = rng.gen_range(range.start..(range.end - 3));
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
