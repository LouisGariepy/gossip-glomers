use serde::{Deserialize, Serialize};

/// A simple trait to allow mapping whole tuples.
pub trait TupleMap {
    fn map<U>(self, f: fn(Self) -> U) -> U;
}

impl<T1, T2> TupleMap for (T1, T2) {
    fn map<U>(self, f: fn(Self) -> U) -> U {
        f(self)
    }
}

/// A ZST type that cannot be constructed.
#[derive(Debug, Serialize, Deserialize)]
pub enum Never {}
