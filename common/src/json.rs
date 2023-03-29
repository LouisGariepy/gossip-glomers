use serde::{Deserialize, Serialize};

use crate::message::Message;

/// A strongly-typed JSON type.
///
/// The purpose of this type is to restrict the ways you can construct
/// serialized values. This prevents accidentally sending serialized
/// messages that are not supported by the nodes.
#[derive(Debug)]
pub struct Json(String);

impl<Body: Serialize> Message<Body> {
    /// Crate-private utility to serialize a message to [`Json`].
    pub(crate) fn into_json(self) -> Json {
        Json(serde_json::to_string(&self).unwrap())
    }
}

impl<'de, Body: Deserialize<'de>> Message<Body> {
    /// Crate-private utility to deserialize a JSON string to a message.
    pub(crate) fn from_json_str(s: &'de str) -> Self {
        serde_json::from_str(s).unwrap()
    }
}

impl Json {
    /// Crate-private utility to obtain a string
    /// slice reference out of a [`Json`] struct
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
