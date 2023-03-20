use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{
    id::{MessageId, NodeId, SiteId},
    FxIndexSet, TopologyMap,
};

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

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum InitRequest {
        Init {
            node_id: NodeId,
            node_ids: Vec<NodeId>,
        },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum InitResponse {
        InitOk {},
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum EchoRequest {
        Echo { echo: String },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum EchoResponse {
        EchoOk { echo: String },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum GenerateRequest {
        Generate {},
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum GenerateResponse {
        GenerateOk { id: [u64; 2] },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum TopologyRequest {
        Topology { topology: TopologyMap },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum TopologyResponse {
        TopologyOk {},
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum OutboundBroadcastRequest<'a> {
        Read {},
        Broadcast {
            message: u64,
        },
        BroadcastMany {
            messages: &'a indexmap::set::Slice<u64>,
        },
    }
);

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    pub enum InboundBroadcastRequest {
        Read {},
        Broadcast { message: u64 },
        BroadcastMany { messages: FxIndexSet<u64> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum OutboundBroadcastResponse<'a> {
        ReadOk { messages: &'a FxIndexSet<u64> },
        BroadcastOk {},
        BroadcastManyOk {},
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum InboundBroadcastResponse {
        ReadOk { messages: FxIndexSet<u64> },
        BroadcastOk {},
        BroadcastManyOk {},
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
    (
        #[derive($($derive:path),*)]
        $vis:vis enum $enum_ident:ident {
            $variant_ident:ident {
                $($field_ident:ident : $field_ty:ty),+ $(,)?
            }$(,)?
        }
    ) =>
    {
        #[derive($($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        $vis enum $enum_ident{
            $variant_ident($variant_ident),
        }

        #[derive($($derive),*)]
        $vis struct $variant_ident {
            $($vis $field_ident : $field_ty,)+
        }

        impl $enum_ident {
            $vis fn as_inner(&self) -> &$variant_ident {
                match self {
                    $enum_ident::$variant_ident(inner) => inner
                }
            }
            $vis fn into_inner(self) -> $variant_ident {
                match self {
                    $enum_ident::$variant_ident(inner) => inner
                }
            }
        }
    };
    (
        #[derive($($derive:path),*)]
        $vis:vis enum $($tokens:tt)*
    ) => {
        #[derive($($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        $vis enum $($tokens)*
    };
}
use define_msg_kind;
