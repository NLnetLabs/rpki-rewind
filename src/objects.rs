use rpki::repository::{aspa::Aspa, Roa};
use serde::{Serialize, Deserialize};


pub fn create_cert(_cert: rpki::repository::Cert) -> CertObject {
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

#[derive(Serialize, Deserialize, Debug)]
pub struct AspaObject {
    customer: u32,
    providers: Vec<u32>,
}

impl From<Aspa> for AspaObject {
    fn from(value: Aspa) -> Self {
        Self { 
            customer: value.content().customer_as().into_u32(), 
            providers: value.content().provider_as_set()
                .iter().map(|p| p.into_u32()).collect()
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ManifestObject {
    
}