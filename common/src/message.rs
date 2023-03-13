use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::id::{MessageId, NodeId, SiteId};

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageType<RequestBody, ResponseBody> {
    Request(Request<RequestBody>),
    Response(Response<ResponseBody>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Request<Kind> {
    pub msg_id: MessageId,
    #[serde(flatten)]
    pub kind: Kind,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response<Kind> {
    pub in_reply_to: MessageId,
    #[serde(flatten)]
    pub kind: Kind,
}

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

impl<'de, Body: Deserialize<'de>> Message<Body> {
    pub fn from_json_str(s: &'de str) -> Self {
        serde_json::from_str(s).unwrap()
    }
}

pub struct JsonString(String);

impl JsonString {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

define_msg_kind!(
    "init",
    #[Serialize, Deserialize]
    pub struct InitRequest {
        pub node_id: NodeId,
        pub node_ids: Vec<NodeId>,
    }
);

define_msg_kind!(
    "init_ok",
    #[Serialize, Deserialize]
    pub struct InitResponse {}
);

define_msg_kind!(
    "echo",
    #[Serialize, Deserialize]
    pub struct EchoRequest {
        pub echo: String,
    }
);

define_msg_kind!(
    "echo_ok",
    #[Serialize, Deserialize]
    pub struct EchoResponse {
        pub echo: String,
    }
);

define_msg_kind!(
    "generate",
    #[Serialize, Deserialize]
    pub struct GenerateRequest {}
);

define_msg_kind!(
    "generate_ok",
    #[Serialize, Deserialize]
    pub struct GenerateResponse {
        pub id: [u64; 2],
    }
);

pub type Topology = FxHashMap<NodeId, Vec<NodeId>>;
define_msg_kind!(
    "topology",
    #[Serialize, Deserialize]
    pub struct TopologyRequest {
        pub topology: Topology,
    }
);

define_msg_kind!(
    "topology_ok",
    #[Serialize, Deserialize]
    pub struct TopologyResponse {}
);

define_msg_kind!(
    #[Serialize, Deserialize]
    pub enum BroadcastRequest {
        Broadcast { message: u64 },
        Read {},
    }
);

define_msg_kind!(
    #[Serialize]
    pub enum OutboundBroadcastResponse<'a> {
        BroadcastOk {},
        ReadOk { messages: &'a FxHashSet<u64> },
    }
);

define_msg_kind!(
    #[Serialize, Deserialize]
    pub enum InboundBroadcastResponse {
        BroadcastOk {},
        ReadOk { messages: FxHashSet<u64> },
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

macro_rules! define_msg_kind {
    ($rename:literal, #[$($derive:path),*] $vis:vis struct $($tokens:tt)*) => {
        #[derive(Debug, $($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename = $rename)]
        $vis struct $($tokens)*
    };
    (#[$($derive:path),*] $vis:vis enum $($tokens:tt)*) => {
        #[derive(Debug, $($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        $vis enum $($tokens)*
    };
}
use define_msg_kind;
