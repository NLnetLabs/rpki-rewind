use rpki::repository::{aspa::Aspa, Cert, Roa};
use serde::{Deserialize, Serialize};


pub fn create_cert(_cert: rpki::repository::Cert) -> CertObject {
    todo!()
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RpkiObject {
    Roa(Roa),
    Aspa(Aspa),
}

impl RpkiObject {
    pub fn parse_rpki_object(
        uri: &rpki::uri::Rsync, 
        data: &bytes::Bytes
    ) -> Option<Self> {
        match uri {
            _ if uri.ends_with(".roa") => {  
                let roa = 
                    rpki::repository::roa::Roa::decode(data.clone(), true);
                if let Ok(roa) = roa {
                    // let roa_object = RoaObject::from(roa);
                    Some(Self::Roa(roa))
                } else {
                    None
                }
            },
            _ if uri.ends_with(".asa") => { 
                let aspa = 
                    rpki::repository::aspa::Aspa::decode(data.clone(), true);
                if let Ok(aspa) = aspa {
                    // let aspa_object = AspaObject::from(aspa);
                    Some(Self::Aspa(aspa))
                } else {
                    None
                }
            },
            _ => None
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CertObject {
    not_before: String,
    not_after: String
}

impl CertObject {
    pub fn new(cert: &Cert) -> Self {
        let not_before = 
            cert.validity().not_before().to_rfc3339();
        let not_after = 
            cert.validity().not_after().to_rfc3339();

        Self { not_before, not_after }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RoaObject {
    #[serde(flatten)]
    cert: CertObject, 

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
        let cert = CertObject::new(value.cert());
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
            cert,
            as_id,
            ipv4,
            ipv6
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AspaObject {
    #[serde(flatten)]
    cert: CertObject, 

    customer: u32,
    providers: Vec<u32>,
}

impl From<Aspa> for AspaObject {
    fn from(value: Aspa) -> Self {
        Self { 
            cert: CertObject::new(value.cert()),
            customer: value.content().customer_as().into_u32(), 
            providers: value.content().provider_as_set()
                .iter().map(|p| p.into_u32()).collect()
        }
    }
}
