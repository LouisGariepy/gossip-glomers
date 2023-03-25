use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
/// A type representing message IDs.
pub struct MessageId(pub u64);

/// A type representing the ID of a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientId(u64);

/// A type representing the ID of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u64);

/// A type representing the ID of a site.
///
/// Sites can be clients, nodes, or services.
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(untagged)]
pub enum SiteId {
    /// The site is a client.
    Client(ClientId),
    /// The site is a node.
    Node(NodeId),
}

impl From<ClientId> for SiteId {
    fn from(value: ClientId) -> Self {
        SiteId::Client(value)
    }
}

impl From<NodeId> for SiteId {
    fn from(value: NodeId) -> Self {
        SiteId::Node(value)
    }
}

impl PartialEq<ClientId> for SiteId {
    fn eq(&self, other: &ClientId) -> bool {
        match self {
            SiteId::Client(client_id) => client_id == other,
            SiteId::Node(_) => false,
        }
    }
}

impl PartialEq<SiteId> for ClientId {
    fn eq(&self, other: &SiteId) -> bool {
        other == self
    }
}

impl PartialEq<NodeId> for SiteId {
    fn eq(&self, other: &NodeId) -> bool {
        match self {
            SiteId::Client(_) => false,
            SiteId::Node(node_id) => node_id == other,
        }
    }
}

impl PartialEq<SiteId> for NodeId {
    fn eq(&self, other: &SiteId) -> bool {
        other == self
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

impl_id_serde!(ClientId, 'c');
impl_id_serde!(NodeId, 'n');
