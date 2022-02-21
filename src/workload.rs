use crate::Result;
use rand::prelude::StdRng;
use rand::Rng;
use rand::SeedableRng;
use sqlx::mysql::MySqlConnection;
use sqlx::Executor;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;

async fn insert(conn: &mut MySqlConnection) -> Result<()> {
    conn.execute("INSERT INTO `473d9750-7369-4822-91b0-bc6705131333` SET `4af7ba24-c2fa-4deb-8af2-58d5f98783d0` = '2016-05-24 13:20:38', `c1c104bf-2899-4776-8a94-f01f9d728c74` = 'p8q1g'").await?;
    Ok(())
}

async fn delete(conn: &mut MySqlConnection) -> Result<()> {
    conn.execute("DELETE FROM `473d9750-7369-4822-91b0-bc6705131333`")
        .await?;
    Ok(())
}

pub async fn create_table(conn: &mut MySqlConnection) -> Result<()> {
    conn.execute("DROP TABLE IF EXISTS `473d9750-7369-4822-91b0-bc6705131333`")
        .await?;
    conn.execute("CREATE TABLE `473d9750-7369-4822-91b0-bc6705131333` (`c1c104bf-2899-4776-8a94-f01f9d728c74` SET('pwl', 'k6sg', 'f', '9rfx', 'o', '9ngz', 'p8q1g', 'kk8y', '5', 'lz', 'g'), `4af7ba24-c2fa-4deb-8af2-58d5f98783d0` TIMESTAMP, PRIMARY KEY (`4af7ba24-c2fa-4deb-8af2-58d5f98783d0`, `c1c104bf-2899-4776-8a94-f01f9d728c74`)) COMMENT '85575ad7-e373-49e7-adb0-dd10541d9478' CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_bin'").await?;
    Ok(())
}

async fn add_index(conn: &mut MySqlConnection) -> Result<()> {
    conn.execute("ALTER TABLE `473d9750-7369-4822-91b0-bc6705131333` ADD INDEX `ef9e02dc-578b-4e7f-acd6-0d0fbbe919f5` (`c1c104bf-2899-4776-8a94-f01f9d728c74`)").await?;
    Ok(())
}

async fn drop_index(conn: &mut MySqlConnection) -> Result<()> {
    conn.execute("ALTER TABLE `473d9750-7369-4822-91b0-bc6705131333` DROP INDEX `ef9e02dc-578b-4e7f-acd6-0d0fbbe919f5`").await?;
    Ok(())
}

pub async fn dml_worker(conn: &mut MySqlConnection, mut rx: Receiver<()>) -> Result<()> {
    conn.execute("use test").await?;
    let mut rng = StdRng::from_rng(rand::thread_rng())?;
    loop {
        if rx.try_recv().is_ok() {
            break;
        }
        insert(conn).await?;
        sleep(&mut rng).await;
        delete(conn).await?;
        sleep(&mut rng).await;
    }
    Ok(())
}

pub async fn ddl_worker(conn: &mut MySqlConnection, mut rx: Receiver<()>) -> Result<()> {
    conn.execute("use test").await?;
    let mut rng = StdRng::from_rng(rand::thread_rng())?;
    loop {
        if rx.try_recv().is_ok() {
            break;
        }
        add_index(conn).await?;
        sleep(&mut rng).await;
        drop_index(conn).await?;
        sleep(&mut rng).await;
    }
    Ok(())
}

async fn sleep(rng: &mut StdRng) {
    tokio::time::sleep(Duration::from_millis(rng.gen_range::<u64, _>(0..=300))).await;
}
