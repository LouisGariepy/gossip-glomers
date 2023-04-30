#![warn(missing_docs)]
//! This crate provides some useful abstractions to complete the [gossip-glomers challenge](https://fly.io/dist-sys/).
//!
//! The most useful item is the [`node::Node`] type. It handles creating a node that is able to receive
//! and send strongly-typed messages. Nodes are multi-threaded using a asynchronous
//! task-per-request model using tokio. The main request-handling loop is started using the
//! [`node::Node::run`] method.
//!
//! Nodes are created using a [`node::NodeBuilder`] that handles the initial message exchange. The node
//! builder allows the user to handle further setup messages as needed and create the node's
//! internal state using the [`node::NodeBuilder::with_state`] method.

use std::hash::BuildHasherDefault;

use id::NodeId;
use indexmap::{IndexMap, IndexSet};
use rustc_hash::FxHasher;

mod utils;

/// Identification is an important part of distributed systems.
/// This module contains the types for generating and handling IDs.
pub mod id;
/// Utilities and helpers for (de)serialization.
pub mod json;
/// Definitions for message types.
pub mod message;
/// Core definitions and utilities for nodes.
pub mod node;

pub use indexmap::{map::Slice as IndexMapSlice, set::Slice as IndexSetSlice};
pub use rustc_hash::{FxHashMap, FxHashSet};

pub use utils::{HealthyMutex, PushGetIndex, TupleMap};

/// A fast hashset that preserves insertion order.
pub type FxIndexSet<T> = IndexSet<T, BuildHasherDefault<FxHasher>>;
/// A fast hashmap that preserves insertion order.
pub type FxIndexMap<K, V> = IndexMap<K, V, BuildHasherDefault<FxHasher>>;
/// A type alias that describes the topology of the network.
pub type TopologyMap = FxHashMap<NodeId, Vec<NodeId>>;
