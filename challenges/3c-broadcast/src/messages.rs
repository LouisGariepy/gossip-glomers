use common::{FxIndexSet, IndexSetSlice};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InboundRequest {
    Read {},
    Broadcast { message: u64 },
    BroadcastMany { messages: FxIndexSet<u64> },
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutboundResponse<'a> {
    ReadOk { messages: &'a FxIndexSet<u64> },
    BroadcastOk {},
    BroadcastManyOk {},
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutboundRequest<'a> {
    Broadcast { message: u64 },
    BroadcastMany { messages: &'a IndexSetSlice<u64> },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InboundResponse {
    BroadcastOk {},
    BroadcastManyOk {},
}
