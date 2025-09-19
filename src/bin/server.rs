use std::collections::HashSet;
use std::error::Error;
use std::io::Read;
use std::{collections::HashMap, fmt::Display};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;

use clokwerk::{AsyncScheduler, TimeUnits};
use log::{info, warn, error, debug};
use rand::Rng;
use reqwest::IntoUrl;
use rpki::rrdp::{Delta, NotificationFile, Snapshot};
use rpki::uri::Rsync;
use rpki_rewind::objects::RpkiObject;
use rpki_rewind::{settings, utils};
use sha2::Digest;
use sqlx::Transaction;
use tempfile::TempDir;
use tokio::{io::AsyncWriteExt};
use tokio::sync::RwLock;

use rpki_rewind::database::Database;
use walkdir::WalkDir;



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
        colog::default_builder().filter_level(log::LevelFilter::Info).init();

        let database = Arc::new(Database::new().await);
        if let Err(err) = database.add_startup(utils::timestamp()).await {
            error!("{}", err);
        };

        let mut scheduler = AsyncScheduler::with_tz(chrono::Utc);
        let runners = Arc::new(RwLock::new(HashMap::new()));
        let rn = runners.clone();
        let db = database.clone();
        scheduler.every(settings::UPDATE_RRDP.seconds())
            .run(move || Self::update_runners(db.to_owned(), rn.to_owned()));

        Self::update_runners(database.clone(), runners.clone()).await;

        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn update_runners(database: Arc<Database>, runners: Arc<RwLock<HashMap<String, Runner>>>) {
        match Self::fetch_urls().await {
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

    async fn fetch_urls() -> Result<Vec<String>, FetchError> {
        let client = reqwest::Client::builder()
            .user_agent(settings::USER_AGENT)
            .gzip(true)
            .build()?;
        let resp = client.get(settings::ROUTINATOR_URL).send().await?;
        let value = resp.json::<serde_json::Value>().await?;
        let rrdps = || -> Option<Vec<String>> {
            let root = value.as_object()?;
            let mut result: Vec<String> = Vec::new();
            let mut rrdp: Vec<String> = root["rrdp"].as_object()?
                .keys().cloned().collect();
            let mut rsync: Vec<String> = root["rsync"].as_object()?
                .keys().cloned().collect();
            result.append(&mut rrdp);
            result.append(&mut rsync);
            Some(result)
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
    rsync_state: Arc<RwLock<HashSet<(String, String)>>>,
    stop: Arc<Mutex<bool>>
}

impl Runner {
    pub fn new(database: Arc<Database>, url: String) -> Self {
        let mut runner = Self { 
            database,
            url: url.clone(),
            notification: Arc::new(RwLock::new(None)),
            rsync_state: Arc::new(RwLock::new(HashSet::new())),
            stop: Arc::new(Mutex::new(false))
        };
        if url.starts_with("https://") {
            runner.start_rrdp();
        } else if url.starts_with("rsync://") {
            runner.start_rsync();
        } else {
            panic!("I have no idea how this happened");
        }
        runner
    }

    fn download_path<
        U: std::convert::AsRef<[u8]>,
        P: std::fmt::Display + std::convert::AsRef<std::ffi::OsStr>
    >(time: P, base: P, url: U) -> String {
        let result = sha2::Sha256::digest(url);
        let file_path = std::path::Path::new(&base).join(format!(
            "{}-{}",
            time,
            hex::encode(result)
        ));
        let file_path = file_path.to_str().expect("invalid path");
        file_path.to_string()
    }

    async fn download<
        U: std::convert::AsRef<[u8]>, 
        P: std::fmt::Display + std::convert::AsRef<std::ffi::OsStr>
    >(
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

    pub fn start_rrdp(&mut self) {
        info!("Started {}", &self.url);
        let client = reqwest::Client::builder()
            .user_agent(settings::USER_AGENT)
            .build().expect("reqwest is broken beyond repair");
        let url = String::from(self.url.as_str());
        let database = self.database.clone();
        let notification = self.notification.clone();
        
        let stop: Arc<Mutex<bool>> = self.stop.clone();
        tokio::spawn(async move {
            let random_secs = {
                let mut rng = rand::rng();
                rng.random_range(0..settings::UPDATE_NOTIFICATION)
            };
            tokio::time::sleep(Duration::from_secs(random_secs.into())).await;

            while !*stop.lock().expect("Stop Mutex read err'd") {
                let now = tokio::time::Instant::now();

                let database = database.clone();
                let client = client.clone();
                let url = url.clone();
                let notification = notification.clone();
                let time = chrono::Utc::now().timestamp_millis();

                let _ = Self::retrieve_rrdp(
                    database,
                    client,
                    url,
                    notification,
                    time
                ).await.map_err(|e| {
                    warn!("{}", e);
                });
                tokio::time::sleep_until(
                    now + Duration::from_secs(settings::UPDATE_NOTIFICATION.into())
                ).await;                
            }
        });
    }

    pub async fn add_to_database(
        database: Arc<Database>, 
        transaction: &mut Transaction<'_, sqlx::Postgres>,
        object_id: i32,
        parsed: Option<RpkiObject>
    ) -> Result<(), Box<dyn Error>> {
        match parsed {
            Some(RpkiObject::Roa(roa)) => {
                for ip in roa.content().v4_addrs().iter() {
                    database.add_roa(
                        object_id, 
                        ipnet::Ipv4Net::new(
                            ip.prefix().to_v4(),
                            ip.prefix().addr_len()
                        ).expect("rpki-rs error").into(),
                        ip.max_length().map(Into::into),
                        roa.content().as_id().into_u32().into(),
                        roa.cert().validity().not_before().naive_utc(),
                        roa.cert().validity().not_after().naive_utc(),
                        transaction
                    ).await?;
                }
                for ip in roa.content().v6_addrs().iter() {
                    database.add_roa(
                        object_id, 
                        ipnet::Ipv6Net::new(
                            ip.prefix().to_v6(),
                            ip.prefix().addr_len()
                        ).expect("rpki-rs error").into(),
                        ip.max_length().map(Into::into),
                        roa.content().as_id().into_u32().into(),
                        roa.cert().validity().not_before().naive_utc(),
                        roa.cert().validity().not_after().naive_utc(),
                        transaction
                    ).await?;
                }
            },
            Some(RpkiObject::Aspa(aspa)) => {
                for provider in aspa.content().provider_as_set().iter() {
                    database.add_aspa(
                        object_id, 
                        aspa.content().customer_as().into_u32().into(), 
                        provider.into_u32().into(), 
                        aspa.cert().validity().not_before().naive_utc(), 
                        aspa.cert().validity().not_after().naive_utc(), 
                        transaction
                    ).await?;
                }
            },
            None => {}
        }
        Ok(())
    }

    pub async fn retrieve_rrdp(
        database: Arc<Database>,
        client: reqwest::Client,
        url: String,
        notification: Arc<RwLock<Option<NotificationFile>>>,
        time: i64,
    ) -> Result<(), Box<dyn Error>> {
        // let output = settings::DOWNLOAD_FOLDER.to_string();
        let output_dir = Arc::new(
            tempfile::TempDir::with_prefix("rewind-rrdp-")
            .expect("Cannot create temporary folder")
        );
        let output = output_dir.clone().path().to_string_lossy().to_string();

        Self::download(&client, &time.to_string(), &url, &output).await?;

        let notification_path = Self::download_path(&time.to_string(), &output, &url);
        let notification_path = format!("{}.xml", notification_path);
        let reader = std::io::BufReader::new(std::fs::File::open(notification_path)?);
        let notification_file = rpki::rrdp::NotificationFile::parse(reader);

        let Ok(mut new_notification) = notification_file else {
            warn!("Notification file not parseable: {}", &url);
            // Might as well return here, there is nothing we can do
            return Ok(());
        };

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
            if 
                let Some(last) = notification.deltas().last() && 
                let Some(new_last) = new_notification.deltas().last() 
            {
                if new_last.serial() < last.serial() {
                    download_snapshot = true;
                } else {
                    last_serial = last.serial();
                }
            }
        } else {
            download_snapshot = true;
        };
        if download_snapshot {
            info!("SNAPSHOT {}", &new_notification.snapshot().uri().to_string());
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

            let snapshot_path = Self::download_path(
                &time.to_string(), 
                &output, 
                new_notification.snapshot().uri().as_str()
            );
            let snapshot_path = format!("{}.xml", snapshot_path);
            let reader = std::io::BufReader::new(
                std::fs::File::open(snapshot_path)?
            );
            let snapshot = Snapshot::parse(reader);

            if let Ok(snapshot) = snapshot {
                let mut transaction = database.begin_transaction().await?;
                for element in snapshot.elements() {
                    let hash = utils::sha256(element.data());

                    let parsed = RpkiObject::parse_rpki_object(
                        element.uri(), 
                        element.data()
                    );

                    let object_id = database.add_object(
                        element.data(),
                            time, 
                            element.uri().as_ref(), 
                            Some(hash.as_str()), 
                            Some(&url), 
                            &mut transaction
                    ).await;

                    match object_id {
                        Err(err) => { 
                            error!("Could not add object to database: {}", err);
                        },
                        Ok(object_id) => {
                            Self::add_to_database(
                                database.clone(), 
                                &mut transaction, 
                                object_id, 
                                parsed
                            ).await?;
                        }
                    };
                }
                if let Err(err) = database.commit(transaction).await {
                    error!("Could not commit database transaction: {}", err);
                }
            }
        } else {
            let mut transaction = database.begin_transaction().await?;
            for delta in new_notification.deltas() {
                if delta.serial() > last_serial {
                    info!("DELTA {}", &delta.uri().to_string());
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
                    let delta_path = Self::download_path(
                        &time.to_string(), 
                        &output, 
                        delta.uri().as_str()
                    );
                    let delta_path = format!("{}.xml", delta_path);
                    let reader = std::io::BufReader::new(
                        std::fs::File::open(delta_path)?);
                    let delta_file = Delta::parse(reader);

                    if let Ok(delta_file) = delta_file {
                        for element in delta_file.elements() {
                            match element {
                                rpki::rrdp::DeltaElement::Publish(publish_element) => {
                                    let hash = utils::sha256(publish_element.data());
                                    let parsed = RpkiObject::parse_rpki_object(
                                        publish_element.uri(), 
                                        publish_element.data()
                                    );

                                    let object_id = database.add_object(
                                        publish_element.data(), 
                                        time, 
                                        publish_element.uri().as_str(), 
                                        Some(hash.as_str()), 
                                        Some(&url), 
                                        &mut transaction
                                    ).await?;

                                    Self::add_to_database(
                                        database.clone(), 
                                        &mut transaction, 
                                        object_id, 
                                        parsed
                                    ).await?;
                                },
                                rpki::rrdp::DeltaElement::Update(update_element) => {
                                    let hash = utils::sha256(update_element.data());
                                    let parsed = RpkiObject::parse_rpki_object(
                                        update_element.uri(), 
                                        update_element.data()
                                    );

                                    let object_id = database.add_object(
                                        update_element.data(), 
                                        time, 
                                        update_element.uri().as_str(), 
                                        Some(hash.as_str()), 
                                        Some(&url), 
                                        &mut transaction
                                    ).await?;

                                    Self::add_to_database(
                                        database.clone(), 
                                        &mut transaction, 
                                        object_id, 
                                        parsed
                                    ).await?;
                                },
                                rpki::rrdp::DeltaElement::Withdraw(withdraw_element) => {
                                    database.remove_objects_uri_hash(
                                        withdraw_element.uri().as_str(), 
                                        hex::encode(
                                            withdraw_element.hash().as_slice()
                                        ).as_str(),
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
        
        Ok(())
    }

    pub fn start_rsync(&mut self) {
        info!("Started {}", &self.url);
        let uri = String::from(self.url.as_str());
        let database = self.database.clone();
        let state = self.rsync_state.clone();

        let dir = Arc::new(
            tempfile::TempDir::with_prefix("rewind-rsync-")
            .expect("Cannot create temporary directory")
        );
        
        let stop: Arc<Mutex<bool>> = self.stop.clone();
        tokio::spawn(async move {
            let random_secs = {
                let mut rng = rand::rng();
                rng.random_range(0..settings::UPDATE_NOTIFICATION)
            };
            tokio::time::sleep(Duration::from_secs(random_secs.into())).await;
            
            while !*stop.lock().expect("Stop Mutex read err'd") {
                let now = tokio::time::Instant::now();

                let database = database.clone();
                let uri = uri.clone();
                let state = state.clone();
                let dir = dir.clone();
                let time = chrono::Utc::now().timestamp_millis();
                let _ = Self::retrieve_rsync(
                    database,
                    uri,
                    state,
                    dir,
                    time
                ).await.map_err(|e| {
                    warn!("{}", e);
                });
                tokio::time::sleep_until(
                    now + Duration::from_secs(settings::UPDATE_NOTIFICATION.into())
                ).await;                
            }
        });
    }

    pub async fn retrieve_rsync(
        database: Arc<Database>,
        uri: String,
        state: Arc<RwLock<HashSet<(String, String)>>>,
        dir: Arc<TempDir>,
        time: i64
    ) -> Result<(), Box<dyn Error>> {
        let path = dir.path().to_string_lossy().into_owned();
        let _output = std::process::Command::new("rsync")
            .args(["-a", "--contimeout=10", uri.as_str(), &path])
            .output()?;

        let mut new_state: HashSet<(String, String)> = HashSet::new();

        for entry in WalkDir::new(&path).into_iter().filter_map(|e| e.ok()) {
            if entry.path().is_dir() {
                continue;
            }
            debug!("{}", entry.path().display());
            let mut file = std::fs::File::open(entry.path())?;
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;
            let hash = utils::sha256(&content.into());

            let entry_uri = utils::rsync_path_to_uri(
                &uri, 
                &path, 
                entry.path()
            )?;
            new_state.insert((entry_uri, hash));
        }

        let mut state = state.write().await;
        let mut transaction = database.begin_transaction().await?;

        let mut changes = false;
        let removed = state.difference(&new_state);
        for (entry_uri, entry_hash) in removed {
            changes = true;
            database.remove_objects_uri_hash(
                entry_uri,
                entry_hash,
                time,
                &mut transaction
            ).await?;
        }
        let added_or_updated = new_state.difference(&state);
        for (entry_uri, entry_hash) in added_or_updated {
            changes = true;
            let path = utils::rsync_uri_to_path(&uri, &path, entry_uri)?;
            let mut file = std::fs::File::open(path)?;
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;

            let content: Bytes = content.into();

            let parsed = RpkiObject::parse_rpki_object(
                &Rsync::from_string(entry_uri.to_string())?, 
                &content
            );

            let object_id = database.add_object(
                &content, 
                time, 
                entry_uri, 
                Some(entry_hash), 
                Some(&uri), 
                &mut transaction
            ).await?;

            Self::add_to_database(
                database.clone(),
                &mut transaction,
                object_id,
                parsed
            ).await?;
        }

        if changes {
            info!("RSYNC UPDATE {}", &uri);
        }        

        database.commit(transaction).await?;
        *state = new_state;

        Ok(())
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
        let urls = crate::App::fetch_urls().await.unwrap();
        dbg!(urls);
    }

    #[tokio::test]
    async fn retrieve_files() {
        
    }
}