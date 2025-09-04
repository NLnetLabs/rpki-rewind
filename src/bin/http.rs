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
use rpki_rewind::database::{self, Database};
use serde::{Deserialize, Serialize};
use tokio_util::io::ReaderStream;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/wayback", post(wayback))
        .route("/query", get(query));

    let listener = 
        tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../../assets/http/index.html"))
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryRequest {
    as_id: Option<u32>
}

async fn query(
    Form(form): Form<QueryRequest>
) -> impl IntoResponse {
    let database = Database::new().await;
    let mut stream;
    if let Some(as_id) = form.as_id {
        stream = database.retrieve_objects_asn(as_id.into()).await;
    } else {
        return Err(StatusCode::IM_A_TEAPOT);
    }
    let mut objects: Vec<database::Object> = Vec::new();

    loop {
        let Ok(obj) = stream.try_next().await else {
            return Err(StatusCode::BAD_REQUEST);
        };
        let Some(obj) = obj else {
            break;
        };
        objects.push(obj);
    }
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
            let file_name = obj.uri.replace("rsync://", "");

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