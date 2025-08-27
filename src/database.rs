use std::pin::Pin;

use futures_util::Stream;
use serde_json::Value;
use sqlx::{types::Json, Pool, Postgres, Transaction};

use crate::settings;



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

    pub async fn add_startup(&self, timestamp: i64) -> Result<(), sqlx::Error> {
        self.add_event("startup", timestamp, None, None, None).await?;
        self.remove_objects_all(timestamp).await
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

    pub async fn add_object(&self, content: &[u8], content_json: Option<Value>, visible_on: i64, uri: &str, hash: Option<&str>, publication_point: Option<&str>, transaction: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        self.remove_objects_uri(uri, visible_on, transaction).await?;
        sqlx::query("INSERT INTO objects (content, content_json, visible_on, hash, uri, publication_point) VALUES ($1, $2, $3, $4, $5, $6)")
            .bind(content)
            .bind(content_json)
            .bind(visible_on)
            .bind(hash)
            .bind(uri)
            .bind(publication_point)
            .execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn remove_objects_all(&self, disappeared_on: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE objects SET disappeared_on = $1 WHERE disappeared_on IS NULL")
            .bind(disappeared_on)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn remove_objects_publication_point(&self, publication_point: &str, disappeared_on: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE objects SET disappeared_on = $1 WHERE publication_point = $2 AND disappeared_on IS NULL")
            .bind(disappeared_on)
            .bind(publication_point)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn remove_objects_uri(&self, uri: &str, disappeared_on: i64, transaction: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE objects SET disappeared_on = $1 WHERE uri = $2 AND disappeared_on IS NULL")
            .bind(disappeared_on)
            .bind(uri)
            .execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn remove_objects_uri_hash(&self, uri: &str, hash: &str, disappeared_on: i64, transaction: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE objects SET disappeared_on = $1 WHERE uri = $2 AND hash = $3 AND disappeared_on IS NULL")
            .bind(disappeared_on)
            .bind(uri)
            .bind(hash)
            .execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn retrieve_objects(&self, timestamp: i64) -> Pin<Box<dyn Stream<Item = Result<Object, sqlx::Error>> + std::marker::Send + '_>> {
        sqlx::query_as("SELECT * FROM objects WHERE visible_on <= $1 AND (disappeared_on >= $1 OR disappeared_on IS NULL)")
            .bind(timestamp)
            .fetch(&self.pool)
    }
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct Object {
    pub id: i32,
    pub content: Vec<u8>,
    pub content_json: Option<String>,
    pub visible_on: i64,
    pub disappeared_on: Option<i64>,
    pub hash: String,
    pub uri: String,
    pub publication_point: String,
}