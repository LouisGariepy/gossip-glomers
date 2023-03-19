use std::hash::BuildHasherDefault;

use id::NodeId;
use indexmap::set::IndexSet;
use rustc_hash::{FxHashMap, FxHasher};

mod json;

pub mod id;
pub mod message;
pub mod node;

pub type FxIndexSet<T> = IndexSet<T, BuildHasherDefault<FxHasher>>;
pub type TopologyMap = FxHashMap<NodeId, Vec<NodeId>>;
pub use json::Json;
