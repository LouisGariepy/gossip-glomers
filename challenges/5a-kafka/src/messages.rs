use common::{
    json::{SerIterMap, SerIterSeq},
    FxHashMap,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InboundRequest {
    Send { key: String, msg: u64 },
    Poll { offsets: FxHashMap<String, usize> },
    CommitOffsets { offsets: FxHashMap<String, usize> },
    ListCommittedOffsets { keys: Vec<String> },
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutboundResponse<'a> {
    SendOk {
        offset: usize,
    },
    PollOk {
        msgs: SerIterMap<'a, String, SerIterSeq<'a, (usize, u64)>>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: SerIterMap<'a, String, usize>,
    },
}
