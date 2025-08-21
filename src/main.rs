use std::error::Error;
use std::{collections::HashMap, fmt::Display};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;

use clokwerk::{AsyncScheduler, TimeUnits};
use daemonbase::logging::Logger;
use log::{info, warn};
use reqwest::IntoUrl;
use sha2::Digest;
use tokio::{io::AsyncWriteExt};
use tokio::sync::RwLock;

mod settings;


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

        let mut scheduler = AsyncScheduler::with_tz(chrono::Utc);
        let runners = Arc::new(RwLock::new(HashMap::new()));
        let rn = runners.clone();
        scheduler.every(settings::UPDATE_RRDP.seconds()).run(move || Self::update_runners(rn.to_owned()));

        Self::update_runners(runners.clone()).await;

        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            // warn!("{:#?}", runners.clone().read().await);
        }
    }

    async fn update_runners(runners: Arc<RwLock<HashMap<String, Runner>>>) {
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

#[derive(Debug)]

struct Runner {
    url: String,
    // serial: Arc<Mutex<u32>>,
    stop: Arc<Mutex<bool>>
}

impl Runner {
    pub fn new(url: String) -> Self {
        let mut runner = Self { 
            url: url,
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

        let mut scheduler = AsyncScheduler::with_tz(chrono::Utc);
        scheduler.every(settings::UPDATE_NOTIFICATION.seconds()).run(move || {
            let client = client.clone();
            let url = url.clone();
            let time = chrono::Utc::now().timestamp_millis().to_string();
            async move {
                let _ = async || -> Result<(), Box<dyn Error>> {
                    let output = "/tmp/rpki-rewind/".to_string();

                    Self::download(&client, &time, &url, &output).await?;

                    let notification_path = Self::download_path(&time, &output, &url);
                    let notification_path = format!("{}.xml", notification_path);
                    dbg!(&notification_path);
                    let reader = std::io::BufReader::new(std::fs::File::open(notification_path)?);
                    let notification_file = rpki::rrdp::NotificationFile::parse(reader);

                    if let Ok(notification_file) = notification_file {
                        Self::download(
                            &client, 
                            &time,
                            &notification_file.snapshot().uri().to_string(), 
                            &output
                        ).await?;
                    }
                    Ok(())
                }().await.map_err(|e| {
                    warn!("{}", e);
                });


            }
        });

        let stop: Arc<Mutex<bool>> = self.stop.clone();
        tokio::spawn(async move {
            loop {
                if !*stop.lock().expect("Stop Mutex read err'd") {
                    scheduler.run_pending().await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                } else {
                    break;
                }
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