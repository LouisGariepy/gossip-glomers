use std::hash::BuildHasherDefault;

use indexmap::{IndexMap, IndexSet};
use rustc_hash::FxHasher;

mod id;
mod json;
mod message;
mod node;
mod utils;

pub use indexmap::{map::Slice as IndexMapSlice, set::Slice as IndexSetSlice};
pub use json::{serialize_guard, Json};
pub use rustc_hash::{FxHashMap, FxHashSet};

pub use id::{ClientId, MessageId, NodeId, SiteId};
pub use message::{
    Init, InitRequest, InitResponse, MaelstromErrorCode, Message, MessageType, Request, Response,
    Topology, TopologyRequest, TopologyResponse,
};
pub use node::{Node, NodeBuilder, NodeChannel, RpcCallback};
pub use utils::{Never, TupleMap};

pub type FxIndexSet<T> = IndexSet<T, BuildHasherDefault<FxHasher>>;
pub type FxIndexMap<T> = IndexMap<T, BuildHasherDefault<FxHasher>>;
pub type TopologyMap = FxHashMap<NodeId, Vec<NodeId>>;
