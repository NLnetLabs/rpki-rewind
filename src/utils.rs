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

pub fn parse_rpki_object(uri: &rpki::uri::Rsync, data: &bytes::Bytes) -> Option<serde_json::Value> {
    match uri {
        _ if uri.ends_with(".roa") => {  
            let roa = rpki::repository::roa::Roa::decode(data.clone(), true);
            if let Ok(roa) = roa {
                let roa_object = RoaObject::from(roa);
                match serde_json::to_value(&roa_object) {
                    Ok(json) => Some(json),
                    Err(_) => None,
                }
            } else {
                None
            }
        },
        _ if uri.ends_with(".asa") => { 
            let aspa = rpki::repository::aspa::Aspa::decode(data.clone(), true);
            if let Ok(aspa) = aspa {
                let aspa_object = AspaObject::from(aspa);
                match serde_json::to_value(&aspa_object) {
                    Ok(json) => Some(json),
                    Err(_) => None,
                }
            } else {
                None
            }
        },
        _ => None
    }
}