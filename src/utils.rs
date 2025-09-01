use std::path::{Path, PathBuf};

use sha2::Digest;

use crate::objects::{AspaObject, RoaObject};

pub fn timestamp() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn sha256(data: &bytes::Bytes) -> String {
    let mut sha256 = sha2::Sha256::new();
    sha256.update(data);
    hex::encode(sha256.finalize())
}

pub fn parse_rpki_object(
    uri: &rpki::uri::Rsync, 
    data: &bytes::Bytes
) -> Option<serde_json::Value> {
    match uri {
        _ if uri.ends_with(".roa") => {  
            let roa = 
                rpki::repository::roa::Roa::decode(data.clone(), true);
            if let Ok(roa) = roa {
                let roa_object = RoaObject::from(roa);
                serde_json::to_value(&roa_object).ok()
            } else {
                None
            }
        },
        _ if uri.ends_with(".asa") => { 
            let aspa = 
                rpki::repository::aspa::Aspa::decode(data.clone(), true);
            if let Ok(aspa) = aspa {
                let aspa_object = AspaObject::from(aspa);
                serde_json::to_value(&aspa_object).ok()
            } else {
                None
            }
        },
        _ => None
    }
}

pub fn rsync_path_to_uri(
    base_uri: &str, 
    base_path: &str, 
    file_path: &Path
) -> Result<String, Box<dyn std::error::Error>> {
    let relative_path = file_path.strip_prefix(base_path)?;
    let relative_path = relative_path.to_string_lossy().into_owned();
    let relative_path = relative_path.strip_prefix("/").unwrap_or(&relative_path);
    let uri = base_uri.strip_suffix("/").unwrap_or(base_uri);
    Ok(format!("{}/{}", uri, relative_path))
}

pub fn rsync_uri_to_path(
    base_uri: &str, 
    base_path: &str, 
    uri: &str
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let Some(relative_uri) = uri.strip_prefix(base_uri) else {
        return Err(std::io::Error::other("rsync path makes no sense").into());
    };

    Ok(Path::new(base_path).join(relative_uri))
}
