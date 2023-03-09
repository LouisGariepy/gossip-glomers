use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::id::{MessageId, NodeId, SiteId};

pub type Topology = FxHashMap<NodeId, Vec<NodeId>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<Body> {
    pub src: SiteId,
    pub dest: SiteId,
    pub body: Body,
}

impl<Body: Serialize> Message<Body> {
    pub fn to_json(&self) -> JsonString {
        JsonString(serde_json::to_string(self).unwrap())
    }
}

impl_msg!(
    "init",
    #[Serialize, Deserialize]
    struct InitRequest {
        pub msg_id: MessageId,
        pub node_id: NodeId,
        pub node_ids: Vec<NodeId>,
    }
);

impl_msg!(
    "init_ok",
    #[Serialize]
    struct InitResponse {
        pub in_reply_to: MessageId,
    }
);

impl_msg!(
    "echo",
    #[Serialize, Deserialize]
    struct EchoRequest {
        pub msg_id: MessageId,
        pub echo: String,
    }
);

impl_msg!(
    "echo_ok",
    #[Serialize]
    struct EchoResponse {
        pub in_reply_to: MessageId,
        pub echo: String,
    }
);

impl_msg!(
    "generate",
    #[Serialize, Deserialize]
    struct GenerateRequest {
        pub msg_id: MessageId,
    }
);

impl_msg!(
    "generate_ok",
    #[Serialize]
    struct GenerateResponse {
        pub in_reply_to: MessageId,
        pub id: [u64; 2],
    }
);

impl_msg!(
    "topology",
    #[Serialize, Deserialize]
    struct TopologyRequest {
        pub msg_id: MessageId,
        pub topology: FxHashMap<NodeId, Vec<NodeId>>,
    }
);

impl_msg!(
    "topology_ok",
    #[Serialize]
    struct TopologyResponse {
        pub in_reply_to: MessageId,
    }
);

impl_msg!(
    #[Serialize, Deserialize]
    enum BroadcastRequest {
        Broadcast { msg_id: MessageId, message: u64 },
        Read { msg_id: MessageId },
    }
);

impl_msg!(
    #[Serialize]
    enum BroadcastResponse<'a> {
        BroadcastOk {
            in_reply_to: MessageId,
        },
        ReadOk {
            messages: &'a FxHashSet<u64>,
            in_reply_to: MessageId,
        },
    }
);

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

pub struct JsonString(String);

impl JsonString {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! impl_msg {
    ($rename:literal, #[$($derive:path),*] struct $($tokens:tt)*) => {
        #[derive(Debug, $($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename = $rename)]
        pub struct $($tokens)*
    };
    (#[$($derive:path),*] enum $($tokens:tt)*) => {
        #[derive(Debug, $($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        pub enum $($tokens)*
    };
}
use impl_msg;
