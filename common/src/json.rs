use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::message::Message;

pub struct Json(String);

impl<Body: Serialize> Message<Body> {
    pub fn to_json(&self) -> Json {
        Json(serde_json::to_string(self).unwrap())
    }
}

impl<'de, Body: Deserialize<'de>> Message<Body> {
    pub fn from_json_str(s: &'de str) -> Self {
        serde_json::from_str(s).unwrap()
    }
}

impl Debug for Json {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}
impl Display for Json {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl Json {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<Body: Serialize> From<&Message<Body>> for Json {
    fn from(value: &Message<Body>) -> Self {
        value.to_json()
    }
}
