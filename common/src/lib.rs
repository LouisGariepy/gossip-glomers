#![warn(missing_docs)]
//! This crate provides some useful abstractions to complete the [gossip-glomers challenge](https://fly.io/dist-sys/).
//!
//! The most useful item is the [`Node`] type. It handles creating a node that is able to receive
//! and send strongly-typed messages. Nodes are multi-threaded using a asynchronous
//! task-per-request model using tokio. The main request-handling loop is started using the
//! [`Node::run`] method.
//!
//! Nodes are created using a [`NodeBuilder`] that handles the initial message exchange. The node
//! builder allows the user to handle further setup messages as needed and create the node's
//! internal state using the [`NodeBuilder::with_state`] method.

use std::hash::BuildHasherDefault;

use indexmap::{IndexMap, IndexSet};
use rustc_hash::FxHasher;

mod id;
mod json;
mod message;
mod node;
mod utils;

pub use indexmap::{map::Slice as IndexMapSlice, set::Slice as IndexSetSlice};
pub use json::Json;
pub use rustc_hash::{FxHashMap, FxHashSet};

pub use id::{ClientId, MessageId, NodeId, SiteId};
pub use message::{
    Init, InitRequest, InitResponse, MaelstromErrorCode, Message, Request, Response, Topology,
    TopologyRequest, TopologyResponse,
};
pub use node::{Node, NodeBuilder, NodeChannel, RpcCallback};
pub use utils::{HealthyMutex, Never, TupleMap};

/// A fast hashset that preserves insertion order.
pub type FxIndexSet<T> = IndexSet<T, BuildHasherDefault<FxHasher>>;
/// A fast hashmap that preserves insertion order.
pub type FxIndexMap<T> = IndexMap<T, BuildHasherDefault<FxHasher>>;
/// A type alias that describes the topology of the network.
pub type TopologyMap = FxHashMap<NodeId, Vec<NodeId>>;
