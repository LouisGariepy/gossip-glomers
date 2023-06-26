use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

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

/// A [`Mutex`] wrapper that is never poisoned.
#[derive(Debug, Default)]
pub struct HealthyMutex<T>(Mutex<T>);

impl<T> HealthyMutex<T> {
    /// Creates a new mutex guarding a value.
    pub fn new(value: T) -> Self {
        Self(Mutex::new(value))
    }

    /// Locks the mutex to acquire the guard.
    ///
    /// # Panics
    /// This function panics if the mutex was poisoned.
    pub fn lock(&self) -> MutexGuard<T> {
        self.0.lock().unwrap()
    }
}

/// A mutex wrapper that is never poisoned.
#[derive(Debug, Default)]
pub struct HealthyRwLock<T>(RwLock<T>);

impl<T> HealthyRwLock<T> {
    /// Creates a new rw-lock guarding a value.
    pub fn new(value: T) -> Self {
        Self(RwLock::new(value))
    }

    /// Tries to locks the rw-lock to acquire a read guard.
    ///
    /// # Panics
    /// This function panics if the lock was poisoned.
    pub fn read(&self) -> RwLockReadGuard<T> {
        self.0.read().unwrap()
    }

    /// Locks the rw-lock to acquire a write guard.
    ///
    /// # Panics
    /// This function panics if the lock was poisoned.
    pub fn write(&self) -> RwLockWriteGuard<T> {
        self.0.write().unwrap()
    }
}

/// A utility trait that essentially allows us to link the type
/// of two different generics into one.
pub trait SameType {
    /// Associated type that denotes which type the implementer is the same as.
    type As: ?Sized;
}

/// The blanket implementation that makes [`SameType`] work out.
impl<T: ?Sized> SameType for T {
    type As = Self;
}
