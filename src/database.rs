use std::sync::Arc;

use rpki::uri;
use sqlx::{Pool, Postgres, Transaction};

use crate::{settings, utils};



pub struct Database {
    pool: Pool<Postgres>
}

impl Database {
    pub async fn new() -> Self {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(settings::DB_POOL)
            .connect(format!(
                "postgres://{}:{}@{}/{}",
                settings::DB_USER,
                settings::DB_PASS,
                settings::DB_HOST,
                settings::DB_NAME
            ).as_str())
            .await.expect("could not connect to database");
        Database {
            pool: pool
        }
    }

    pub async fn begin_transaction(&self) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
        self.pool.begin().await
    }

    pub async fn commit(&self, transaction: Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        transaction.commit().await
    }

    pub async fn add_startup(&self) -> Result<(), sqlx::Error> {
        self.add_event("startup", utils::timestamp(), None, None, None).await
    }

    pub async fn add_event(&self, event: &str, timestamp: i64, uri: Option<&str>, hash: Option<&str>, publication_point: Option<&str>) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO events (event, timestamp, uri, hash, publication_point) VALUES ($1, $2, $3, $4, $5)")
            .bind(event)
            .bind(timestamp)
            .bind(uri)
            .bind(hash)
            .bind(publication_point)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn add_object(&self, content: &[u8], visible_on: i64, uri: Option<&str>, hash: Option<&str>, publication_point: Option<&str>, transaction: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO objects (content, visible_on, hash, uri, publication_point) VALUES ($1, $2, $3, $4, $5)")
            .bind(content)
            .bind(visible_on)
            .bind(hash)
            .bind(uri)
            .bind(publication_point)
            .execute(&mut **transaction).await?;
        Ok(())
    }
}