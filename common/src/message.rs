use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{
    id::{MessageId, NodeId, SiteId},
    TopologyMap,
};

/// Denotes a message that can either be a request or a response.
/// This type is mainly used for deserialization purposes.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageType<RequestBody, ResponseBody> {
    /// The message is a request.
    Request(Request<RequestBody>),
    /// The message is a reponse.
    Response(Response<ResponseBody>),
}

/// The contents of a request message.
#[derive(Debug, Serialize, Deserialize)]
pub struct Request<Kind> {
    /// Requests must have a message ID.
    pub msg_id: MessageId,
    /// This field holds the data for the specific
    /// kind of request.
    #[serde(flatten)]
    pub kind: Kind,
}

/// The contents of a response message.
#[derive(Debug, Serialize, Deserialize)]
pub struct Response<Kind> {
    /// Responses must have an ID corresponding
    /// to a previously sent request.
    pub in_reply_to: MessageId,
    /// This field holds the data for the specific
    /// kind of response.
    #[serde(flatten)]
    pub kind: Kind,
}

/// The base structure for a message.
#[derive(Debug, Serialize, Deserialize)]
pub struct Message<Body> {
    /// The site from which the message originates.
    pub src: SiteId,
    /// The site to which the message is going.
    pub dest: SiteId,
    /// The specific data of this message.
    pub body: Body,
}

/// Specific error codes reserved by Maelstrom.
#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq)]
#[repr(u8)]
pub enum MaelstromErrorCode {
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

/// A macro utility to define message kinds.
#[macro_export]
macro_rules! define_msg_kind {
    (
        $(#[doc = $enum_doc:expr])*
        #[derive($($derive:path),*)]
        $vis:vis enum $enum_ident:ident {
            $(#[doc = $variant_doc:expr])*
            $variant_ident:ident {
                $(
                    $(#[doc = $field_doc:expr])*
                    $field_ident:ident : $field_ty:ty
                ),+ $(,)?
            }$(,)?
        }
    ) =>
    {
        $(#[doc = $enum_doc])*
        #[derive($($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        $vis enum $enum_ident{
            $(#[doc = $variant_doc])*
            $variant_ident($variant_ident),
        }

        impl $enum_ident {
            #[doc = "Converts [`"]
            #[doc = stringify!($enum_ident)]
            #[doc = "`] into it's concrete struct type [`"]
            #[doc = stringify!($variant_ident)]
            #[doc = "`]."]
            #[must_use]
            $vis fn into_inner(self) -> $variant_ident {
                match self {
                    $enum_ident::$variant_ident(inner) => inner
                }
            }
        }

        #[doc = "Concrete data-holding struct for the [`"]
        #[doc = stringify!($enum_ident)]
        #[doc = "`] message kind"]
        #[derive($($derive),*)]
        $vis struct $variant_ident {
            $(
                $(#[doc = $field_doc])*
                $vis $field_ident : $field_ty,
            )+
        }
    };
    (
        $(#[doc = $doc:expr])*
        #[derive($($derive:path),*)]
        $vis:vis enum $($tokens:tt)*
    ) => {
        $(#[doc = $doc])*
        #[derive($($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        $vis enum $($tokens)*
    };
}

define_msg_kind!(
    /// Initial request that Maelstrom sends
    /// after node initialization.
    #[derive(Debug, Deserialize)]
    pub enum InitRequest {
        Init {
            /// The ID of the node.
            node_id: NodeId,
            /// The IDs of every node in the network.
            node_ids: Vec<NodeId>,
        },
    }
);

define_msg_kind!(
    /// Response to [`InitRequest`].
    #[derive(Debug, Serialize)]
    pub enum InitResponse {
        InitOk {},
    }
);

define_msg_kind!(
    /// Initial topology request message with which
    /// Maelstrom shares the network topology with
    /// nodes.
    #[derive(Debug, Deserialize)]
    pub enum TopologyRequest {
        Topology {
            /// A map containing all node's neighbours.
            topology: TopologyMap,
        },
    }
);

define_msg_kind!(
    /// Response to [`TopologyRequest`].
    #[derive(Debug, Serialize)]
    pub enum TopologyResponse {
        TopologyOk {},
    }
);
