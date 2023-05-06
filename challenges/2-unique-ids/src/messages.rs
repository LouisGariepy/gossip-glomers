use serde::{Deserialize, Serialize};

use common::id::{MsgId, NodeId};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InboundRequest {
    Generate {},
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutboundResponse {
    GenerateOk { id: (NodeId, MsgId) },
}
