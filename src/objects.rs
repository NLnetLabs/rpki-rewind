use serde::{Serialize, Deserialize};

pub enum RpkiObject {
    Roa(RoaObject),
}

pub fn create_cert(cert: rpki::repository::Cert) -> CertObject {
    todo!()
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CertObject {
    issuer: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RoaObject {
    #[serde(flatten)]
    cert: CertObject, 

    as_id: u32,
    ipv4_addresses: Vec<u32>
}