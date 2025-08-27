use rpki::repository::Roa;
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
    // #[serde(flatten)]
    // cert: CertObject, 

    as_id: u32,
    ipv4: Vec<RoaIpObject>,
    ipv6: Vec<RoaIpObject>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RoaIpObject {
    ip_addr: String,
    prefix_len: u8,
    max_len: Option<u8>
}

impl From<Roa> for RoaObject {
    fn from(value: Roa) -> Self {
        let as_id = value.content().as_id().into_u32();
        let ipv4 = value.content().v4_addrs().iter()
            .map(|a| RoaIpObject {
                ip_addr: a.prefix().addr().to_v4().to_string(),
                prefix_len: a.prefix().addr_len(),
                max_len: a.max_length()
            }).collect();
        let ipv6 = value.content().v6_addrs().iter()
            .map(|a| RoaIpObject {
                ip_addr: a.prefix().addr().to_v6().to_string(),
                prefix_len: a.prefix().addr_len(),
                max_len: a.max_length()
            }).collect();
        Self {
            as_id,
            ipv4,
            ipv6
        }
    }
}