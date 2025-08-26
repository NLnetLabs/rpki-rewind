use std::error::Error;
use std::{collections::HashMap, fmt::Display};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;

use clokwerk::{AsyncScheduler, TimeUnits};
use daemonbase::logging::Logger;
use log::{info, warn, error};
use reqwest::IntoUrl;
use rpki::rrdp::{Delta, NotificationFile, Snapshot};
use rpki_rewind::objects::{RoaObject, RpkiObject};
use rpki_rewind::{settings, utils};
use sha2::Digest;
use tokio::{io::AsyncWriteExt};
use tokio::sync::RwLock;

use rpki_rewind::database::Database;



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
        if let Err(err) = database.add_startup(utils::timestamp()).await {
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
                            Runner::new(database.clone(), url.to_string())
                        );
                    }
                }
                runners.retain(|k, v: &mut Runner| {
                    if !rrdps.contains(k) {
                        v.stop();
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
                            if let Err(err) = database.remove_objects_publication_point(
                                &k, 
                            utils::timestamp()
                            ).await {
                                warn!("Could not remove objects from database: {}", err);
                            }
                        });
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
    database: Arc<Database>,
    url: String,
    notification: Arc<RwLock<Option<NotificationFile>>>,
    stop: Arc<Mutex<bool>>
}

impl Runner {
    pub fn new(database: Arc<Database>, url: String) -> Self {
        let mut runner = Self { 
            database,
            url,
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
        let database = self.database.clone();
        let notification = self.notification.clone();
        
        let stop: Arc<Mutex<bool>> = self.stop.clone();
        tokio::spawn(async move {
            while !*stop.lock().expect("Stop Mutex read err'd") {
                let database = database.clone();
                let client = client.clone();
                let url = url.clone();
                let database = database.clone();
                let notification = notification.clone();
                let now = tokio::time::Instant::now();
                let time = chrono::Utc::now().timestamp_millis();
                let _ = async || -> Result<(), Box<dyn Error>> {
                    let output = settings::DOWNLOAD_FOLDER.to_string();

                    Self::download(&client, &time.to_string(), &url, &output).await?;

                    let notification_path = Self::download_path(&time.to_string(), &output, &url);
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
                                &time.to_string(),
                                &new_notification.snapshot().uri().to_string(), 
                                &output
                            ).await?;
                            database.add_event(
                                "snapshot", 
                                time, 
                                Some(new_notification.snapshot().uri().as_str()), 
                                Some(new_notification.snapshot().hash().to_string().as_str()), 
                                Some(&url)
                            ).await?;
                            database.remove_objects_publication_point(&url, time).await?;

                            let snapshot_path = Self::download_path(&time.to_string(), &output, &new_notification.snapshot().uri().as_str());
                            let snapshot_path = format!("{}.xml", snapshot_path);
                            let reader = std::io::BufReader::new(std::fs::File::open(snapshot_path)?);
                            let snapshot = Snapshot::parse(reader);

                            if let Ok(snapshot) = snapshot {
                                let mut transaction = database.begin_transaction().await?;
                                for element in snapshot.elements() {
                                    let mut sha256 = sha2::Sha256::new();
                                    sha256.update(element.data());
                                    let hash = hex::encode(sha256.finalize());

                                    let uri: &rpki::uri::Rsync = element.uri();
                                    let data: Option<String> = match uri {
                                        _ if uri.ends_with(".roa") => {  
                                            let roa = rpki::repository::roa::Roa::decode(element.data().clone(), true);
                                            if let Ok(roa) = roa {
                                                match serde_json::to_string(&roa) {
                                                    Ok(x) => Some(x),
                                                    Err(_) => None
                                                }
                                            } else {
                                                None
                                            }
                                        },
                                        _ => None
                                    };

                                    if let Err(err) = database.add_object(
                                        element.data(),
                                         time, 
                                         &element.uri().to_string(), 
                                         Some(hash.as_str()), 
                                         Some(&url), 
                                         &mut transaction
                                    ).await {
                                        warn!("Could not add object to database: {}", err);
                                    }
                                }
                                if let Err(err) = database.commit(transaction).await {
                                    warn!("Could not commit database transaction: {}", err);
                                }
                            }
                        } else {
                            let mut transaction = database.begin_transaction().await?;
                            for delta in new_notification.deltas() {
                                if delta.serial() > last_serial {
                                    warn!("DELTA {}", &delta.uri().to_string());
                                    Self::download(
                                        &client, 
                                        &time.to_string(),
                                        &delta.uri().to_string(), 
                                        &output
                                    ).await?;
                                    database.add_event(
                                        "delta", 
                                        time, 
                                        Some(delta.uri().as_str()), 
                                        Some(delta.hash().to_string().as_str()), 
                                        Some(&url)
                                    ).await?;
                                    let delta_path = Self::download_path(&time.to_string(), &output, &delta.uri().as_str());
                                    let delta_path = format!("{}.xml", delta_path);
                                    let reader = std::io::BufReader::new(std::fs::File::open(delta_path)?);
                                    let delta_file = Delta::parse(reader);

                                    if let Ok(delta_file) = delta_file {
                                        for element in delta_file.elements() {
                                            match element {
                                                rpki::rrdp::DeltaElement::Publish(publish_element) => {
                                                    let mut sha256 = sha2::Sha256::new();
                                                    sha256.update(publish_element.data());
                                                    let hash = hex::encode(sha256.finalize());

                                                    database.add_object(
                                                        publish_element.data(), 
                                                        time, 
                                                        publish_element.uri().as_str(), 
                                                        Some(hash.as_str()), 
                                                        Some(&url), 
                                                        &mut transaction
                                                    ).await?;
                                                },
                                                rpki::rrdp::DeltaElement::Update(update_element) => {
                                                    let mut sha256 = sha2::Sha256::new();
                                                    sha256.update(update_element.data());
                                                    let hash = hex::encode(sha256.finalize());

                                                    database.add_object(
                                                        update_element.data(), 
                                                        time, 
                                                        update_element.uri().as_str(), 
                                                        Some(hash.as_str()), 
                                                        Some(&url), 
                                                        &mut transaction
                                                    ).await?;
                                                },
                                                rpki::rrdp::DeltaElement::Withdraw(withdraw_element) => {
                                                    database.remove_objects_uri_hash(
                                                        withdraw_element.uri().as_str(), 
                                                        hex::encode(withdraw_element.hash().as_slice()).as_str(),
                                                        time, 
                                                        &mut transaction
                                                    ).await?;
                                                },
                                            }
                                        }
                                    }
                                }
                            }
                            if let Err(err) = database.commit(transaction).await {
                                error!("Could not commit: {}", err);
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

    #[tokio::test]
    async fn retrieve_files() {
        
    }
}