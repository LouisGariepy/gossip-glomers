use std::hash::BuildHasherDefault;

use id::NodeId;
use indexmap::{IndexMap, IndexSet};
use rustc_hash::FxHasher;

mod json;

pub mod id;
pub mod message;
pub mod node;

pub use indexmap::{map::Slice as IndexMapSlice, set::Slice as IndexSetSlice};
pub use json::{serialize_guard, Json};
pub use rustc_hash::{FxHashMap, FxHashSet};

pub type FxIndexSet<T> = IndexSet<T, BuildHasherDefault<FxHasher>>;
pub type FxIndexMap<T> = IndexMap<T, BuildHasherDefault<FxHasher>>;
pub type TopologyMap = FxHashMap<NodeId, Vec<NodeId>>;
