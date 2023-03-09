use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct MessageId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientId(pub u64);
impl_id_serde!(ClientId, 'c');

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);
impl_id_serde!(NodeId, 'n');

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiteId {
    Client(ClientId),
    Node(NodeId),
}

impl Serialize for SiteId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            SiteId::Client(id) => id.serialize(serializer),
            SiteId::Node(id) => id.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for SiteId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let site_id_string = String::deserialize(deserializer)?;
        let mut site_id_chars = site_id_string.chars();
        let site_char = site_id_chars.next();
        match site_char {
            Some('c') => Ok(Self::Client(ClientId(
                site_id_chars
                    .as_str()
                    .parse()
                    .map_err(serde::de::Error::custom)?,
            ))),
            Some('n') => Ok(SiteId::Node(NodeId(
                site_id_chars
                    .as_str()
                    .parse()
                    .map_err(serde::de::Error::custom)?,
            ))),
            invalid => Err(serde::de::Error::custom(format!(
                "site id must start with either \"c\" (for client) or \"n\" (for node), got \"{}\"",
                invalid.map_or_else(|| String::from("<NONE>"), String::from)
            ))),
        }
    }
}

macro_rules! impl_id_serde {
    ($ty:ty, $c:literal) => {
        impl Serialize for $ty {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&format!(concat!($c, "{}"), self.0))
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let site_id_string = String::deserialize(deserializer)?;
                let mut site_id_chars = site_id_string.chars();
                let site_char = site_id_chars.next();
                match site_char {
                    Some($c) => Ok(Self(
                        site_id_chars
                            .as_str()
                            .parse()
                            .map_err(|_| serde::de::Error::custom("expected"))?,
                    )),
                    invalid => Err(serde::de::Error::custom(format!(
                        concat!(
                            concat!("client id string must start with \"", $c),
                            "\", got \"{}\""
                        ),
                        invalid
                            .map(String::from)
                            .unwrap_or_else(|| String::from("<NONE>"))
                    ))),
                }
            }
        }
    };
}
use impl_id_serde;
