use std::error::Error;
use std::{collections::HashMap, fmt::Display};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;

use clokwerk::{AsyncScheduler, TimeUnits};
use daemonbase::logging::Logger;
use log::{info, warn, error};
use reqwest::IntoUrl;
use rpki::rrdp::{NotificationFile, Snapshot};
use sha2::Digest;
use tokio::{io::AsyncWriteExt};
use tokio::sync::RwLock;

use crate::database::Database;

mod database;
mod settings;
mod utils;


#[tokio::main]
async fn main() {
    let app = App::new();
    app.main().await;
}

struct App {

}

impl App {
    pub fn new() -> Self {
        Self { }
    }

    pub async fn main(&self) {
        Logger::init_logging();

        let database = Arc::new(Database::new().await);
        if let Err(err) = database.add_startup().await {
            error!("{}", err);
        };

        let mut scheduler = AsyncScheduler::with_tz(chrono::Utc);
        let runners = Arc::new(RwLock::new(HashMap::new()));
        let rn = runners.clone();
        let db = database.clone();
        scheduler.every(settings::UPDATE_RRDP.seconds()).run(move || Self::update_runners(db.to_owned(), rn.to_owned()));

        Self::update_runners(database.clone(), runners.clone()).await;

        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            // warn!("{:#?}", runners.clone().read().await);
        }
    }

    async fn update_runners(database: Arc<Database>, runners: Arc<RwLock<HashMap<String, Runner>>>) {
        match Self::fetch_rrdp_urls().await {
            Ok(rrdps) => {
                let mut runners = runners.write().await;
                for url in &rrdps {
                    if !&runners.contains_key(url) {
                        let _ = &runners.insert(
                            url.to_string(), 
                            Runner::new(url.to_string())
                        );
                    }
                }
                runners.retain(|k, v: &mut Runner| {
                    if !rrdps.contains(k) {
                        let k = k.clone();
                        let database = database.clone();
                        tokio::spawn(async move {
                            if let Err(err) = database.add_event(
                                "removed", 
                                utils::timestamp(), 
                                None, 
                                None, 
                                Some(&k)
                            ).await {
                                warn!("Could not add remove event to database: {}", err);
                            };
                        });
                        v.stop();
                        return false;
                    }
                    true
                });
            },
            Err(e) => {
                warn!("{}", e);
            }
        };
    }

    async fn fetch_rrdp_urls() -> Result<Vec<String>, FetchError> {
        let client = reqwest::Client::builder()
            .user_agent(settings::USER_AGENT)
            .build()?;
        let resp = client.get(settings::ROUTINATOR_URL).send().await?;
        let value = resp.json::<serde_json::Value>().await?;
        let rrdps = || -> Option<Vec<String>> {
            let root = value.as_object()?;
            let rrdp = root["rrdp"].as_object()?;
            Some(rrdp.keys().cloned().collect())
        }();
        let Some(rrdps) = rrdps else {
            return Err(FetchError::EmptyRRDPs);
        };
        Ok(rrdps)
    }
}

#[derive(Debug)]
enum FetchError {
    RequestError(reqwest::Error),
    EmptyRRDPs,
}

impl Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestError(e) => {
                writeln!(f, "Something went wrong requesting the list of RRDP endpoints: {}", e)
            },
            Self::EmptyRRDPs => { 
                writeln!(f, "The resulting list of RRDPs was malformed")
            }
        }
    }
}

impl From<reqwest::Error> for FetchError {
    fn from(value: reqwest::Error) -> Self {
        Self::RequestError(value)
    }
}

struct Runner {
    url: String,
    notification: Arc<RwLock<Option<NotificationFile>>>,
    stop: Arc<Mutex<bool>>
}

impl Runner {
    pub fn new(url: String) -> Self {
        let mut runner = Self { 
            url: url,
            notification: Arc::new(RwLock::new(None)),
            stop: Arc::new(Mutex::new(false))
        };
        runner.start();
        runner
    }

    fn download_path<
        U: std::convert::AsRef<[u8]>,
        P: std::fmt::Display + std::convert::AsRef<std::ffi::OsStr>
    >(time: P, base: P, url: U) -> String {
        let result = sha2::Sha256::digest(url);
        let file_path = std::path::Path::new(&base).join(format!("{}-{}",
            time,
            hex::encode(result)
        ));
        let file_path = file_path.to_str().expect("invalid path");
        file_path.to_string()
    }

    async fn download<U: std::convert::AsRef<[u8]>, P: std::fmt::Display + std::convert::AsRef<std::ffi::OsStr>>(
        client: &reqwest::Client, 
        time: &P,
        url: &U,
        output: &P
    ) -> Result<(), Box<dyn Error>> where for<'a> &'a U: IntoUrl {
        let file_path = Self::download_path(time, output, url);
        let response = client.get(url).send().await?;
        let mut headers = HashMap::new();
        for (k, v) in response.headers() {
            if let Ok(v) = v.to_str() {
                headers.insert(k.to_string(), v.to_string());
            }
        }
        tokio::fs::write(
            format!("{}.headers.json", file_path),
            serde_json::to_string(&headers)?
        ).await?;

        let mut file = tokio::fs::File::create(format!("{}.xml", file_path)).await?;

        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
        }

        file.flush().await?;
        Ok(())
    }

    pub fn start(&mut self) {
        let client = reqwest::Client::builder()
            .user_agent(settings::USER_AGENT)
            .build().expect("reqwest is broken beyond repair");
        let url = String::from(self.url.as_str());

        let notification = self.notification.clone();
        
        let stop: Arc<Mutex<bool>> = self.stop.clone();
        tokio::spawn(async move {
            while !*stop.lock().expect("Stop Mutex read err'd") {
                let client = client.clone();
                let url = url.clone();
                let notification = notification.clone();
                let now = tokio::time::Instant::now();
                let time = chrono::Utc::now().timestamp_millis().to_string();
                let _ = async || -> Result<(), Box<dyn Error>> {
                    let output = settings::DOWNLOAD_FOLDER.to_string();

                    Self::download(&client, &time, &url, &output).await?;

                    let notification_path = Self::download_path(&time, &output, &url);
                    let notification_path = format!("{}.xml", notification_path);
                    let reader = std::io::BufReader::new(std::fs::File::open(notification_path)?);
                    let mut notification_file = rpki::rrdp::NotificationFile::parse(reader);

                    if let Ok(mut new_notification) = notification_file {
                        let mut notification = notification.write().await;
                        let mut download_snapshot = false;
                        let mut last_serial = 0;
                        if let Some(notification) = notification.as_mut() {
                            if notification.session_id() != new_notification.session_id() {
                                download_snapshot = true;
                            }
                            if !notification.sort_and_verify_deltas(None) {
                                download_snapshot = true;
                            }
                            if !new_notification.sort_and_verify_deltas(None) {
                                download_snapshot = true;
                            }
                            if let Some(last) = notification.deltas().last() {
                                if let Some(new_last) = new_notification.deltas().last() {
                                    if new_last.serial() < last.serial() {
                                        download_snapshot = true;
                                    } else {
                                        last_serial = last.serial();
                                    }
                                }
                            }
                        } else {
                            download_snapshot = true;
                        };
                        if download_snapshot {
                            warn!("SNAPSHOT {}", &new_notification.snapshot().uri().to_string());
                            Self::download(
                                &client, 
                                &time,
                                &new_notification.snapshot().uri().to_string(), 
                                &output
                            ).await?;

                            let snapshot_path = Self::download_path(&time, &output, &url);
                            let snapshot_path = format!("{}.xml", snapshot_path);
                            let reader = std::io::BufReader::new(std::fs::File::open(snapshot_path)?);
                            let snapshot = Snapshot::parse(reader);

                            if let Ok(snapshot) = snapshot {
                                for element in snapshot.elements() {
                                    
                                }
                            }

                        } else {
                            for delta in new_notification.deltas() {
                                if delta.serial() > last_serial {
                                    warn!("DELTA {}", &delta.uri().to_string());
                                    Self::download(
                                        &client, 
                                        &time,
                                        &delta.uri().to_string(), 
                                        &output
                                    ).await?;
                                }
                            }
                        }
                        *notification = Some(new_notification);
                    }
                    Ok(())
                }().await.map_err(|e| {
                    warn!("{}", e);
                });
                tokio::time::sleep_until(
                    now + Duration::from_secs(settings::UPDATE_NOTIFICATION.into())
                ).await;                
            }
        });
    }

    pub fn stop(&mut self) {
        let mut stop = self.stop.lock().expect("Stop Mutex err'd");
        *stop = true;
    }
}


#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_fetch_rrdp_urls() {
        let urls = crate::App::fetch_rrdp_urls().await.unwrap();
        dbg!(urls);
    }
}