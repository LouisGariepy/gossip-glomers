use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::id::{MessageId, NodeId, SiteId};

pub trait MaybeBorrowed {
    type T<'b>: Serialize;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<B> {
    pub src: SiteId,
    pub dest: SiteId,
    pub body: B,
}

impl_req_msg!(
    "init",
    struct InitRequest {
        pub msg_id: MessageId,
        pub node_id: NodeId,
        pub node_ids: Vec<NodeId>,
    }
);

impl_res_msg!(
    "init_ok",
    struct InitResponse {
        pub in_reply_to: MessageId,
    }
);

impl_req_msg!(
    "echo",
    struct EchoRequest {
        pub msg_id: MessageId,
        pub echo: String,
    }
);

impl_res_msg!(
    "echo_ok",
    struct EchoResponse {
        pub in_reply_to: MessageId,
        pub echo: String,
    }
);

impl_req_msg!(
    "generate",
    struct GenerateRequest {
        pub msg_id: MessageId,
    }
);

impl_res_msg!(
    "generate_ok",
    struct GenerateResponse {
        pub in_reply_to: MessageId,
        pub id: [u64; 2],
    }
);

impl_req_msg!(
    enum BroadcastRequest {
        Topology {
            msg_id: MessageId,
            topology: FxHashMap<NodeId, Vec<NodeId>>,
        },
        Broadcast {
            msg_id: MessageId,
            message: u64,
        },
        Read {
            msg_id: MessageId,
        },
    }
);

impl_res_msg!(
    enum BroadcastResponse<'a> {
        TopologyOk {
            in_reply_to: MessageId,
        },
        BroadcastOk {
            in_reply_to: MessageId,
        },
        ReadOk {
            messages: &'a [u64],
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

macro_rules! impl_res_msg {
    ($rename:literal, struct $id:ident<$lifetime:lifetime> $($tokens:tt)*) => {
        #[derive(Debug, Serialize)]
        #[serde(tag = "type")]
        #[serde(rename = $rename)]
        pub struct $id<$lifetime> $($tokens)*
        impl<'a> MaybeBorrowed for $id<'a> {
            type T<'b> = $id<'b>;
        }
    };
    (enum $id:ident<$lifetime:lifetime> $($tokens:tt)*) => {
        #[derive(Debug, Serialize)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        pub enum $id<$lifetime> $($tokens)*
        impl<'a> MaybeBorrowed for $id<'a> {
            type T<'b> = $id<'b>;
        }
    };
    ($rename:literal, struct $id:ident $($tokens:tt)*) => {
        #[derive(Debug, Serialize)]
        #[serde(tag = "type")]
        #[serde(rename = $rename)]
        pub struct $id $($tokens)*
        impl MaybeBorrowed for $id {
            type T<'b> = $id;
        }
    };
    (enum $id:ident $($tokens:tt)*) => {
        #[derive(Debug, Serialize)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        pub enum $id $($tokens)*
        impl MaybeBorrowed for $id {
            type T<'b> = $id;
        }
    };
}
use impl_res_msg;

macro_rules! impl_req_msg {
    ($rename:literal, struct $($tokens:tt)*) => {
        #[derive(Debug, Deserialize)]
        #[serde(tag = "type")]
        #[serde(rename = $rename)]
        pub struct $($tokens)*
    };
    (enum $($tokens:tt)*) => {
        #[derive(Debug, Deserialize)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        pub enum $($tokens)*
    };
}
use impl_req_msg;
