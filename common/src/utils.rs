use std::sync::{Mutex, MutexGuard};

/// A trait to allow mapping whole tuples.
pub trait TupleMap {
    /// This function maps the tuple to a new value.
    /// This is useful when you need to transform a
    /// tuple in a long function chain.
    fn map<U>(self, f: fn(Self) -> U) -> U;
}

impl<T1, T2> TupleMap for (T1, T2) {
    fn map<U>(self, f: fn(Self) -> U) -> U {
        f(self)
    }
}

/// A trait for insertions into collections that have a notion
/// of indexing.
pub trait PushGetIndex<T> {
    /// Pushes the value and returns the insertion index.
    fn push_get_index(&mut self, value: T) -> usize;
}

impl<T> PushGetIndex<T> for Vec<T> {
    fn push_get_index(&mut self, value: T) -> usize {
        let index = self.len();
        self.push(value);
        index
    }
}

/// A mutex wrapper that is never poisoned.
#[derive(Debug, Default)]
pub struct HealthyMutex<T>(Mutex<T>);

impl<T> HealthyMutex<T> {
    /// Locks the mutex to acquire the guard.
    ///
    /// # Panics
    /// This function panics if the mutex was poisoned.
    pub fn lock(&self) -> MutexGuard<T> {
        self.0.lock().unwrap()
    }

    /// Creates a new mutex protecting a value.
    pub fn new(value: T) -> Self {
        HealthyMutex(Mutex::new(value))
    }
}
