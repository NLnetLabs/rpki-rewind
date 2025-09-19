use std::{pin::Pin, time::Duration};

use base64::Engine;
use chrono::NaiveDateTime;
use futures_util::Stream;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgConnectOptions, types::ipnet, ConnectOptions, Pool, Postgres, Row, Transaction};

use crate::settings;



pub struct Database {
    pool: Pool<Postgres>
}

impl Database {
    pub async fn new() -> Self {
        let mut options: PgConnectOptions = format!(
                "postgres://{}:{}@{}/{}",
                settings::DB_USER,
                settings::DB_PASS,
                settings::DB_HOST,
                settings::DB_NAME
            ).parse().expect("invalid database options");

        options = options.log_slow_statements(LevelFilter::Warn, Duration::from_secs(10));

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(settings::DB_POOL)
            .connect_with(options)
            .await.expect("could not connect to database");
        Database {
            pool
        }
    }

    pub async fn begin_transaction(
        &self
    ) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
        self.pool.begin().await
    }

    pub async fn commit(
        &self, 
        transaction: Transaction<'_, Postgres>
    ) -> Result<(), sqlx::Error> {
        transaction.commit().await
    }

    pub async fn add_startup(
        &self, 
        timestamp: i64
    ) -> Result<(), sqlx::Error> {
        self.add_event("startup", timestamp, None, None, None).await?;
        self.remove_objects_all(timestamp).await
    }

    pub async fn add_event(
        &self, 
        event: &str, 
        timestamp: i64, 
        uri: Option<&str>, 
        hash: Option<&str>, 
        publication_point: Option<&str>
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO events 
            (event, timestamp, uri, hash, publication_point) 
            VALUES ($1, $2, $3, $4, $5)")
            .bind(event)
            .bind(timestamp)
            .bind(uri)
            .bind(hash)
            .bind(publication_point)
            .execute(&self.pool).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_object(
        &self, 
        content: &[u8], 
        visible_on: i64, 
        uri: &str, 
        hash: Option<&str>, 
        publication_point: Option<&str>, 
        transaction: &mut Transaction<'_, Postgres>
    ) -> Result<i32, sqlx::Error> {
        self.remove_objects_uri(uri, visible_on, transaction).await?;
        let res:(i32,) = sqlx::query_as::<_, (i32, )>(
            "INSERT INTO objects 
                (content, visible_on, hash, uri, publication_point) 
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id")
            .bind(content)
            .bind(visible_on)
            .bind(hash)
            .bind(uri)
            .bind(publication_point)
            .fetch_one(&mut **transaction).await?;
        Ok(res.0)
    }

    pub async fn remove_objects_all(
        &self, 
        disappeared_on: i64
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE objects SET disappeared_on = $1 
            WHERE disappeared_on IS NULL")
            .bind(disappeared_on)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn remove_objects_publication_point(
        &self, 
        publication_point: &str, 
        disappeared_on: i64
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE objects SET disappeared_on = $1 
            WHERE publication_point = $2 AND disappeared_on IS NULL")
            .bind(disappeared_on)
            .bind(publication_point)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn remove_objects_uri(
        &self, 
        uri: &str, 
        disappeared_on: i64, 
        transaction: &mut Transaction<'_, Postgres>
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE objects SET disappeared_on = $1 
            WHERE uri = $2 AND disappeared_on IS NULL")
            .bind(disappeared_on)
            .bind(uri)
            .execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn remove_objects_uri_hash(
        &self, 
        uri: &str, 
        hash: &str, 
        disappeared_on: i64, 
        transaction: &mut Transaction<'_, Postgres>
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE objects SET disappeared_on = $1 
            WHERE uri = $2 AND hash = $3 AND disappeared_on IS NULL")
            .bind(disappeared_on)
            .bind(uri)
            .bind(hash)
            .execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn retrieve_objects(
        &self, 
        timestamp: i64
    ) -> Pin<Box<dyn Stream<Item = Result<Object, sqlx::Error>> + std::marker::Send + '_>> {
        sqlx::query_as(
            "SELECT id, content, visible_on, 
            disappeared_on, hash, uri, publication_point FROM objects WHERE 
            visible_on <= $1 AND (disappeared_on >= $1 OR disappeared_on IS NULL)")
            .bind(timestamp)
            .fetch(&self.pool)
    }

    pub async fn retrieve_roas_asn(
        &self, 
        as_id: i64
    ) -> Result<Vec<ObjectRoa>, sqlx::Error> {
        let res = sqlx::query(
            r#"SELECT
                    o.id,
                    JSON_AGG(JSON_BUILD_OBJECT(
                        'prefix', r.prefix, 
                        'max_length', r.max_length
                    )) AS parsed,
                    r.as_id,
                    r.not_before,
                    r.not_after,
                    o.content,
                    o.visible_on,
                    o.disappeared_on,
                    o.hash,
                    o.uri,
                    o.publication_point
                FROM
                    objects o,
                    roas r
                WHERE
                    r.as_id = $1
                    AND r.object_id = o.id
                GROUP BY
                    o.id,
                    r.as_id,
                    r.not_before,
                    r.not_after"#)
            .bind(as_id)
            .fetch_all(&self.pool)
            .await?;

        let res = res.into_iter().map(|r| {
            let parsed: Option<Vec<Roa>> = serde_json::from_value(r.get("parsed")).unwrap_or_default();
            let content: &[u8] = r.get("content");
            let content = base64::prelude::BASE64_STANDARD.encode(content);
            ObjectRoa {
                id: r.get("id"),
                prefixes: parsed,
                as_id: r.get("as_id"),
                not_before: r.get("not_before"),
                not_after: r.get("not_after"),
                content,
                visible_on: r.get("visible_on"),
                disappeared_on: r.get("disappeared_on"),
                hash: r.get("hash"),
                uri: r.get("uri"),
                publication_point: r.get("publication_point"),
            }
        }).collect();
        Ok(res)
    }

    pub async fn retrieve_roas_prefix(
        &self, 
        prefix: ipnet::IpNet
    ) -> Result<Vec<ObjectRoa>, sqlx::Error> {
        let res = sqlx::query(
            r#"SELECT
                    o.id,
                    JSON_AGG(JSON_BUILD_OBJECT(
                        'prefix', r.prefix, 
                        'max_length', r.max_length
                    )) AS parsed,
                    r.as_id,
                    r.not_before,
                    r.not_after,
                    o.content,
                    o.visible_on,
                    o.disappeared_on,
                    o.hash,
                    o.uri,
                    o.publication_point
                FROM
                    objects o,
                    roas r
                WHERE
                    r.prefix <<= $1
                    AND r.object_id = o.id
                GROUP BY
                    o.id,
                    r.as_id,
                    r.not_before,
                    r.not_after"#)
            .bind(prefix)
            .fetch_all(&self.pool)
            .await?;

        let res = res.into_iter().map(|r| {
            let parsed: Option<Vec<Roa>> = serde_json::from_value(r.get("parsed")).unwrap_or_default();
            let content: &[u8] = r.get("content");
            let content = base64::prelude::BASE64_STANDARD.encode(content);
            ObjectRoa {
                id: r.get("id"),
                prefixes: parsed,
                as_id: r.get("as_id"),
                not_before: r.get("not_before"),
                not_after: r.get("not_after"),
                content,
                visible_on: r.get("visible_on"),
                disappeared_on: r.get("disappeared_on"),
                hash: r.get("hash"),
                uri: r.get("uri"),
                publication_point: r.get("publication_point"),
            }
        }).collect();
        Ok(res)
    }

    pub async fn retrieve_roas_asn_prefix(
        &self, 
        as_id: i64,
        prefix: ipnet::IpNet
    ) -> Result<Vec<ObjectRoa>, sqlx::Error> {
        let res = sqlx::query(
            r#"SELECT
                    o.id,
                    JSON_AGG(JSON_BUILD_OBJECT(
                        'prefix', r.prefix, 
                        'max_length', r.max_length
                    )) AS parsed,
                    r.as_id,
                    r.not_before,
                    r.not_after,
                    o.content,
                    o.visible_on,
                    o.disappeared_on,
                    o.hash,
                    o.uri,
                    o.publication_point
                FROM
                    objects o,
                    roas r
                WHERE
                    r.as_id = $1
                    AND r.prefix <<= $2
                    AND r.object_id = o.id
                GROUP BY
                    o.id,
                    r.as_id,
                    r.not_before,
                    r.not_after"#)
            .bind(as_id)
            .bind(prefix)
            .fetch_all(&self.pool)
            .await?;

        let res = res.into_iter().map(|r| {
            let parsed: Option<Vec<Roa>> = serde_json::from_value(r.get("parsed")).unwrap_or_default();
            let content: &[u8] = r.get("content");
            let content = base64::prelude::BASE64_STANDARD.encode(content);
            ObjectRoa {
                id: r.get("id"),
                prefixes: parsed,
                as_id: r.get("as_id"),
                not_before: r.get("not_before"),
                not_after: r.get("not_after"),
                content,
                visible_on: r.get("visible_on"),
                disappeared_on: r.get("disappeared_on"),
                hash: r.get("hash"),
                uri: r.get("uri"),
                publication_point: r.get("publication_point"),
            }
        }).collect();
        Ok(res)
    }

    pub async fn retrieve_aspas_customer(
        &self, 
        customer: i64
    ) -> Result<Vec<ObjectAspa>, sqlx::Error> {
        let res = sqlx::query(
            r#"SELECT
                    o.id,
                    a.customer,
                    ARRAY_AGG(a.provider) AS providers,
                    a.not_before,
                    a.not_after,
                    o.content,
                    o.visible_on,
                    o.disappeared_on,
                    o.hash,
                    o.uri,
                    o.publication_point
                FROM
                    objects o,
                    aspas a
                WHERE
                    a.customer = $1
                    AND a.object_id = o.id
                GROUP BY
                    o.id,
                    a.customer,
                    a.not_before,
                    a.not_after"#)
            .bind(customer)
            .fetch_all(&self.pool)
            .await?;

        let res = res.into_iter().map(|r| {
            let content: &[u8] = r.get("content");
            let content = base64::prelude::BASE64_STANDARD.encode(content);
            ObjectAspa {
                id: r.get("id"),
                customer: r.get("customer"),
                providers: r.get("providers"),
                not_before: r.get("not_before"),
                not_after: r.get("not_after"),
                content,
                visible_on: r.get("visible_on"),
                disappeared_on: r.get("disappeared_on"),
                hash: r.get("hash"),
                uri: r.get("uri"),
                publication_point: r.get("publication_point"),
            }
        }).collect();
        Ok(res)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_roa(
        &self, 
        object_id: i32,
        prefix: ipnet::IpNet,
        max_length: Option<i16>,
        as_id: i64,
        not_before: NaiveDateTime,
        not_after: NaiveDateTime,
        transaction: &mut Transaction<'_, Postgres>
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO roas (object_id, prefix, max_length, as_id, 
            not_before, not_after) VALUES ($1, $2, $3, $4, $5, $6)")
            .bind(object_id)
            .bind(prefix)
            .bind(max_length)
            .bind(as_id)
            .bind(not_before)
            .bind(not_after)
            .execute(&mut **transaction).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_aspa(
        &self, 
        object_id: i32,
        customer: i64,
        provider: i64,
        not_before: NaiveDateTime,
        not_after: NaiveDateTime,
        transaction: &mut Transaction<'_, Postgres>
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO aspas (object_id, customer, provider, 
            not_before, not_after) VALUES ($1, $2, $3, $4, $5)")
            .bind(object_id)
            .bind(customer)
            .bind(provider)
            .bind(not_before)
            .bind(not_after)
            .execute(&mut **transaction).await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Object {
    pub id: i32,
    pub content: Vec<u8>,
    pub visible_on: i64,
    pub disappeared_on: Option<i64>,
    pub hash: Option<String>,
    pub uri: Option<String>,
    pub publication_point: Option<String>,
}

#[derive(sqlx::FromRow, sqlx::Type, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct Roa {
    pub prefix: ipnet::IpNet,
    pub max_length: Option<i16>,
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct ObjectRoa {
    pub id: i32,
    pub prefixes: Option<Vec<Roa>>,
    pub as_id: i64,
    pub not_before: NaiveDateTime,
    pub not_after: NaiveDateTime,
    pub content: String,
    pub visible_on: i64,
    pub disappeared_on: Option<i64>,
    pub hash: Option<String>,
    pub uri: Option<String>,
    pub publication_point: Option<String>,
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectAspa {
    pub id: i32,
    pub customer: i64,
    pub providers: Option<Vec<i64>>,
    pub not_before: NaiveDateTime,
    pub not_after: NaiveDateTime,
    pub content: String,
    pub visible_on: i64,
    pub disappeared_on: Option<i64>,
    pub hash: Option<String>,
    pub uri: Option<String>,
    pub publication_point: Option<String>,
}