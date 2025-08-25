use std::{collections::HashMap, fs::File, io::{BufReader, Cursor, Read, Seek}, net::SocketAddr, str::FromStr};

use bytes::Bytes;
use futures_util::{stream, StreamExt, TryStreamExt};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full, StreamBody};
use hyper::{body::{Body, Frame}, server::conn::http1, service::service_fn, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rpki_rewind::database::Database;
use tokio::net::TcpListener;
use tokio_util::io::ReaderStream;


async fn serve(req: Request<hyper::body::Incoming>) -> Result<Response<BoxBody<Bytes, std::io::Error>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(
            full(include_str!("../../assets/http/index.html"))
        )),
        (&Method::GET, "/obtain")  => {
            let params: HashMap<String, String> = req
                .uri()
                .query()
                .map(|v| {
                    url::form_urlencoded::parse(v.as_bytes())
                        .into_owned()
                        .collect()
                })
                .unwrap_or_else(HashMap::new);

            let Some(timestamp) = params.get("timestamp") else {
                return bad_request();
            };
            let timestamp = format!("{}:00", timestamp); // It needs seconds too
            let Ok(timestamp) = chrono::NaiveDateTime::from_str(&timestamp) else {
                return bad_request();
            };
            let timestamp = timestamp.and_utc().timestamp_millis();

            let database = Database::new().await;
            let mut stream = database.retrieve_objects(timestamp).await;

            let Ok(mut dir) = tempfile::tempdir() else {
                return internal_server_error();
            };
            let file_path = dir.path().join("tar");
            let Ok(mut file) = File::create(&file_path) else {
                return internal_server_error();
            };
            let mut tar = tar::Builder::new(std::io::BufWriter::new(file));

            loop {
                let Ok(obj) = stream.try_next().await else {
                    return bad_request();
                };
                let Some(obj) = obj else {
                    break;
                };
                let file_name = obj.uri.replace("rsync://", "");
                // println!("Adding {}", file_name);

                let mut header = tar::Header::new_gnu();
                header.set_size(obj.content.len() as u64);
                header.set_mode(0o644);
                header.set_mtime((obj.visible_on / 1000) as u64);
                header.set_cksum();

                let Ok(_) = tar.append_data(&mut header, file_name, Cursor::new(obj.content)) else {
                    return internal_server_error();
                };
            }

            let Ok(_) = tar.finish() else {
                return internal_server_error();
            };
            
            let Ok(file) = tokio::fs::File::open(&file_path).await else {
                return internal_server_error();
            };

            let reader_stream = ReaderStream::new(file);
                // Convert to http_body_util::BoxBody
            let stream_body = StreamBody::new(reader_stream.map_ok(Frame::data));
            let boxed_body = BodyExt::boxed(stream_body);

            // Send response
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/x-tar")
                .header("Content-Disposition", "attachment; filename=\"rpki.tar\"")
                .body(boxed_body)
                .unwrap();
            Ok(response)
        },
        _ => {
            let mut not_found = Response::new(empty());
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

fn empty() -> BoxBody<Bytes, std::io::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}
fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, std::io::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

fn bad_request() -> Result<Response<BoxBody<bytes::Bytes, std::io::Error>>, hyper::Error> {
    let mut bad_request = Response::new(empty());
    *bad_request.status_mut() = StatusCode::BAD_REQUEST;
    Ok(bad_request) 
}

fn internal_server_error() -> Result<Response<BoxBody<bytes::Bytes, std::io::Error>>, hyper::Error> {
    let mut internal_server_error = Response::new(empty());
    *internal_server_error.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    Ok(internal_server_error) 
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await?;

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, service_fn(serve))
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}