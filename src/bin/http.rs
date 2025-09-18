use std::{fs::File, io::{Cursor, Write}, str::FromStr};

use axum::{
    body::Body, 
    http::StatusCode, 
    response::{Html, IntoResponse}, 
    routing::{get, post}, 
    Form, 
    Json, 
    Router
};
use futures_util::TryStreamExt;
use rpki_rewind::database::Database;
use serde::{Deserialize, Serialize};
use tokio_util::io::ReaderStream;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/objects.html", get(objects))
        .route("/feed.rss", get(feed))
        .route("/wayback", post(wayback))
        .route("/roas", get(roas))
        .route("/aspas", get(aspas));

    let listener = 
        tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Server listening on {}", &listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../../assets/http/index.html"))
}

async fn objects() -> Html<&'static str> {
    Html(include_str!("../../assets/http/objects.html"))
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RoasRequest {
    as_id: Option<u32>,
    prefix: Option<ipnet::IpNet>,
}

async fn roas(
    Form(form): Form<RoasRequest>
) -> impl IntoResponse {
    if let Some(pfx) = form.prefix {
        if pfx.prefix_len() < 8 {
            // If you requested 0.0.0.0/0 you'd make the server crash
            return Err(StatusCode::BAD_REQUEST);
        }
    }

    let database = Database::new().await;
    let objects = match (form.as_id, form.prefix) {
        (Some(as_id), Some(prefix)) => 
            database.retrieve_roas_asn_prefix(as_id.into(), prefix).await,
        (Some(as_id), _) => 
            database.retrieve_roas_asn(as_id.into()).await,
        (_, Some(prefix)) => 
            database.retrieve_roas_prefix(prefix).await,
        _ => 
            return Err(StatusCode::BAD_REQUEST)
    };
    let objects = match objects {
        Ok(objs) => objs,
        Err(e) => {
            println!("{}", e);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    Ok(Json(objects))
}

async fn feed(
    Form(form): Form<RoasRequest>
) -> impl IntoResponse {
    if let Some(pfx) = form.prefix {
        if pfx.prefix_len() < 8 {
            // If you requested 0.0.0.0/0 you'd make the server crash
            return Err(StatusCode::BAD_REQUEST);
        }
    }

    let database = Database::new().await;
    let objects = match (form.as_id, form.prefix) {
        (Some(as_id), Some(prefix)) => 
            database.retrieve_roas_asn_prefix(as_id.into(), prefix).await,
        (Some(as_id), _) => 
            database.retrieve_roas_asn(as_id.into()).await,
        (_, Some(prefix)) => 
            database.retrieve_roas_prefix(prefix).await,
        _ => 
            return Err(StatusCode::BAD_REQUEST)
    };
    let objects = match objects {
        Ok(objs) => objs,
        Err(e) => {
            println!("{}", e);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    // TODO: Output RSS/Atom feed
    Ok(Json(objects))
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AspasRequest {
    customer: Option<u32>
}

async fn aspas(
    Form(form): Form<AspasRequest>
) -> impl IntoResponse {
    let database = Database::new().await;
    let objects;
    if let Some(customer) = form.customer {
        objects = database.retrieve_aspas_customer(customer.into()).await;
    } else {
        return Err(StatusCode::IM_A_TEAPOT);
    }
    let objects = match objects {
        Ok(objs) => objs,
        Err(e) => {
            println!("{}", e);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    Ok(Json(objects))
}

#[derive(Deserialize, Debug)]
pub struct WaybackRequest { 
    timestamp: Option<String> 
}

async fn wayback(
    Form(form): Form<WaybackRequest>
) -> impl IntoResponse {
    let Some(timestamp) = form.timestamp else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let timestamp = format!("{}:00", timestamp); // It needs seconds too
    let Ok(timestamp) = chrono::NaiveDateTime::from_str(&timestamp) else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let timestamp = timestamp.and_utc().timestamp_millis();

    let database = Database::new().await;
    let mut stream = database.retrieve_objects(timestamp).await;

    let Ok(dir) = tempfile::tempdir() else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };
    let file_path = dir.path().join("tar");
    let Ok(mut file) = File::create(&file_path) else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    {
        let mut tar = tar::Builder::new(std::io::BufWriter::new(&mut file));

        loop {
            let Ok(obj) = stream.try_next().await else {
                return Err(StatusCode::BAD_REQUEST);
            };
            let Some(obj) = obj else {
                break;
            };
            let Some(file_name) = obj.uri else {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            };
            let file_name = file_name.replace("rsync://", "");

            let mut header = tar::Header::new_gnu();
            header.set_size(obj.content.len() as u64);
            header.set_mode(0o644);
            header.set_mtime((obj.visible_on / 1000) as u64);
            header.set_cksum();

            let Ok(_) = tar.append_data(
                &mut header, 
                file_name, 
                Cursor::new(obj.content)
            ) else {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            };
        }

        let Ok(_) = tar.finish() else {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        };
    }

    let Ok(_) = file.flush() else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };
    
    let Ok(file) = tokio::fs::File::open(&file_path).await else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    let Ok(metadata) = file.metadata().await else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    let size = metadata.len().to_string();

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let headers = [
        ("Content-Type", "application/x-tar".to_string()),
        ("Content-Length", size),
        ("Content-Disposition", "attachment; filename=\"rpki.tar\"".to_string())
    ];
    Ok((headers, body))
}