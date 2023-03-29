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
    /// Indicates that the requested operation could not be completed within a timeout.
    Timeout = 0,
    /// Thrown when a client sends an RPC request to a node which does not exist.
    NodeNotFound = 1,
    /// Use this error to indicate that a requested operation is not supported by the
    /// current implementation. Helpful for stubbing out APIs during development.
    NotSupported = 10,
    /// Indicates that the operation definitely cannot be performed at this time
    /// perhaps because the server is in a read-only state, has not yet been
    /// initialized, believes its peers to be down, and so on. Do not use this error
    /// for indeterminate cases, when the operation may actually have taken place.
    TemporarilyUnavailable = 11,
    /// The client's request did not conform to the server's expectations, and could
    /// not possibly have been processed.
    MalformedRequest = 12,
    /// Indicates that some kind of general, indefinite error occurred. Use this as a
    /// catch-all for errors you can't otherwise categorize, or as a starting point for
    /// your error handler: it's safe to return `crash` for every problem by
    /// default, then add special cases for more specific errors later.
    Crash = 13,
    /// Indicates that some kind of general, definite error occurred. Use this as a catch-all
    /// for errors you can't otherwise categorize, when you specifically know that the
    /// requested operation has not taken place. For instance, you might encounter an indefinite
    /// failure during the prepare phase of a transaction: since you haven't started the commit
    /// process yet, the transaction can't have taken place. It's therefore safe to return a
    /// definite `abort` to the client.
    Abort = 14,
    /// The client requested an operation on a key which does not exist (assuming the operation
    /// should not automatically create missing keys).
    KeyDoesNotExist = 20,
    /// The client requested the creation of a key which already exists, and the server will
    /// not overwrite it.
    KeyAlreadyExists = 21,
    /// The requested operation expected some conditions to hold, and those conditions were not met.
    /// For instance, a compare-and-set operation might assert that the value of a key is
    /// currently 5; if the value is 3, the server would return `precondition-failed`.
    PreconditionFailed = 22,
    /// The requested transaction has been aborted because of a conflict with another transaction.
    /// Servers need not return this error on every conflict: they may choose to retry
    /// automatically instead.
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
            } $(,)?
        }
    ) =>
    {
        $(#[doc = $enum_doc])*
        #[derive($($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        $vis enum $enum_ident{
            $(#[doc = $variant_doc])*
            #[allow(missing_docs)]
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
        $(#[doc = $enum_doc:expr])*
        #[derive($($derive:path),*)]
        $vis:vis enum $enum_ident:ident $(<$lifetime:lifetime>)? {
            $(
                $(#[doc = $variant_doc:expr])*
                $variant_ident:ident {
                    $(
                        $(#[doc = $field_doc:expr])*
                        $field_ident:ident : $field_ty:ty
                    ),* $(,)?
                }
            ),* $(,)?
        }
    ) => {
        $(#[doc = $enum_doc])*
        #[derive($($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        #[allow(clippy::enum_variant_names)]
        $vis enum $enum_ident $(<$lifetime>)? {
            $(
                $(#[doc = $variant_doc:expr])*
                #[allow(missing_docs)]
                $variant_ident {
                    $(
                        $(#[doc = $field_doc:expr])*
                        $field_ident : $field_ty
                    ),*
                }
            ),*
        }
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
