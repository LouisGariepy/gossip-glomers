use std::{
    fmt::{Debug, Display},
    sync::MutexGuard,
};

use serde::{Deserialize, Serialize, Serializer};

use crate::message::Message;

pub struct Json(String);

impl<Body: Serialize> Message<Body> {
    pub(crate) fn to_json(&self) -> Json {
        Json(serde_json::to_string(self).unwrap())
    }

    pub(crate) fn into_json(self) -> Json {
        Json(serde_json::to_string(&self).unwrap())
    }
}

impl<'de, Body: Deserialize<'de>> Message<Body> {
    pub(crate) fn from_json_str(s: &'de str) -> Self {
        serde_json::from_str(s).unwrap()
    }
}

impl Json {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub fn serialize_guard<T: Serialize, S>(
    guard: &MutexGuard<T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    guard.serialize(serializer)
}
