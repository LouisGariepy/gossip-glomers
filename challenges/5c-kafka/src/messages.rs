use common::json::{SerIterMap, SerIterSeq};
use serde::{Deserialize, Serialize};

use crate::Wal;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum LeaderInboundRequest {
    Send {
        #[serde(rename = "key")]
        queue_key: String,
        msg: u64,
    },
    Poll {
        offsets: Vec<(String, u64)>,
    },
    CommitOffsets {
        #[serde(rename = "offsets")]
        commits: Vec<(String, u64)>,
    },
    ListCommittedOffsets {
        #[serde(rename = "keys")]
        queue_keys: Vec<String>,
    },
    ReserveOffset {},
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum LeaderOutboundResponse<'a> {
    SendOk {
        offset: u64,
    },
    PollOk {
        msgs: SerIterMap<'a, String, SerIterSeq<'a, (&'a u64, &'a u64)>>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: SerIterMap<'a, String, u64>,
    },
    ReserveOffsetOk {
        offset: u64,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum LeaderOutboundRequest<'a> {
    Wal {},
    Replicate { merged_wal: &'a Wal },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum LeaderInboundResponse {
    WalOk { wal: Wal },
    ReplicateOk {},
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum FollowerInboundRequest {
    Send {
        #[serde(rename = "key")]
        queue_key: String,
        msg: u64,
    },
    Poll {
        offsets: Vec<(String, u64)>,
    },
    CommitOffsets {
        #[serde(rename = "offsets")]
        commits: Vec<(String, u64)>,
    },
    ListCommittedOffsets {
        #[serde(rename = "keys")]
        queue_keys: Vec<String>,
    },
    Wal {},
    Replicate {
        merged_wal: Wal,
    },
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum FollowerOutboundResponse<'a> {
    SendOk {
        offset: u64,
    },
    PollOk {
        msgs: SerIterMap<'a, String, SerIterSeq<'a, (&'a u64, &'a u64)>>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: SerIterMap<'a, String, u64>,
    },
    WalOk {
        wal: &'a Wal,
    },
    ReplicateOk {},
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum FollowerOutboundRequest {
    ReserveOffset {},
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum FollowerInboundResponse {
    ReserveOffsetOk { offset: u64 },
}
