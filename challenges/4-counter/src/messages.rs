use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InboundRequest {
    Add { delta: u64 },
    Read {},
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutboundResponse {
    AddOk {},
    ReadOk { value: u64 },
}
