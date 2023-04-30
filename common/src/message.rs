use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{
    id::{MsgId, NodeId, SiteId},
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
    pub msg_id: MsgId,
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
    pub in_reply_to: MsgId,
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
pub enum MaelstromError {
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

/// Initial request that Maelstrom sends
/// after node initialization.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InitRequest {
    Init(Init),
}

/// Concrete data-holding struct for the [`InitRequest`] message kind.
#[derive(Debug, Deserialize)]
pub(crate) struct Init {
    // The ID of the node.
    pub(crate) node_id: NodeId,
    // The IDs of every node in the network.
    pub(crate) node_ids: Vec<NodeId>,
}

impl InitRequest {
    #[must_use]
    pub(crate) fn into_inner(self) -> Init {
        match self {
            InitRequest::Init(inner) => inner,
        }
    }
}

/// Response to [`InitRequest`].
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InitResponse {
    /// Response to [`InitRequest::Init`]
    InitOk {},
}

/// Initial topology request message with which
/// Maelstrom shares the network topology with
/// nodes.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum TopologyRequest {
    /// A request that holds the topology sent by Maelstrom.
    Topology {
        /// A map containing all node's neighbours.
        topology: TopologyMap,
    },
}

impl TopologyRequest {
    /// Converts into the type containing the actual topology data.
    pub fn topology(self) -> TopologyMap {
        match self {
            TopologyRequest::Topology { topology } => topology,
        }
    }
}

/// Response to [`TopologyRequest`].
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum TopologyResponse {
    /// Response to [`TopologyRequest::Topology`].
    TopologyOk {},
}

/// A request that nodes send to interact with the key-value service.
#[doc = " A request that nodes send to interact with the key-value service."]
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum KvRequest<K, V> {
    /// Reads the value associated with the given key.
    Read {
        /// The key to read.
        key: K,
    },
    /// Overwrites the given value to the given key. Creates a new
    /// entry if the the key doesn't already exists.
    Write {
        /// The key to write.
        key: K,
        /// The value to write at the corresponding key.
        value: V,
    },
    /// Atomically compare-and-sets the value of the given key key: if
    /// the value of key is currently from, sets it to to. Returns error
    /// 20 if the key doesn't exist, and 22 if the from value doesn't match.
    Cas {
        /// The key to compare.
        key: K,
        /// The value to compare.
        from: V,
        /// The value to set if the comparison was successful.
        to: V,
        /// Causes the `cas` request to create missing keys, rather than
        /// returning a [`MaelstromError::KeyDoesNotExist`] error.
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        create_if_not_exists: bool,
    },
}

/// Response to [`KvRequest`].
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum KvResponse<V> {
    /// Response to [`KvRequest::Read`].
    ReadOk {
        /// The value that was read
        value: V,
    },
    /// Response to [`KvRequest::Read`].
    WriteOk {},
    /// [`KvRequest::Read`]
    CasOk {},
    /// Signifies that a [`KvRequest`] resutled in an error.
    Error {
        /// The error returned by Maelstrom.
        code: MaelstromError,
    },
}
