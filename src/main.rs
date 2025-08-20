use std::{collections::HashMap, fmt::Display};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clokwerk::{AsyncScheduler, TimeUnits};
use daemonbase::logging::Logger;
use log::{info, warn};
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
            warn!("{:#?}", runners.clone().read().await);
        }
    }

    async fn update_runners(runners: Arc<RwLock<HashMap<String, Runner>>>) {
        match Self::fetch_rrdp_urls().await {
            Ok(rrdps) => {
                let mut runners = runners.write().await;
                for url in &rrdps {
                    if !&runners.contains_key(url) {
                        let _ = &runners.insert(url.to_string(), Runner::new());
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
            .user_agent("NLnet Labs RPKI-Rewind")
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
    stop: Arc<Mutex<bool>>
}

impl Runner {
    pub fn new() -> Self {
        let mut runner = Self { 
            stop: Arc::new(Mutex::new(false))
        };
        runner.start();
        runner
    }

    pub fn start(&mut self) {
        let mut scheduler = AsyncScheduler::with_tz(chrono::Utc);
        scheduler.every(settings::UPDATE_NOTIFICATION.seconds()).run(|| async {
            warn!("THING!");
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
            warn!("DONE");
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