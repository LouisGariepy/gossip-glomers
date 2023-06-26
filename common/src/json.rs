use std::{any::type_name, cell::RefCell, fmt::Debug};

use serde::{Deserialize, Serialize};

use crate::message::Message;

/// A strongly-typed JSON type.
///
/// The purpose of this type is to restrict the ways you can construct
/// serialized values. This prevents accidentally sending serialized
/// messages that are not supported by the nodes.
#[derive(Debug)]
pub struct Json(String);

impl<Body: Serialize> Message<Body> {
    /// Crate-private utility to serialize a message to [`Json`].
    pub(crate) fn into_json(self) -> Json {
        Json(serde_json::to_string(&self).unwrap())
    }
}

impl<'de, Body: Deserialize<'de>> Message<Body> {
    /// Crate-private utility to deserialize a JSON string to a message.
    pub(crate) fn from_json_str(s: &'de str) -> Self {
        serde_json::from_str(s).unwrap()
    }
}

impl Json {
    /// Crate-private utility to obtain a string
    /// slice reference out of a [`Json`] struct
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// A wrapper for iterators that can be serialized as a sequence of values.
///
/// Serializing is usually understood as a process without side-effects.
/// To serialize an iterator, one must introduce side-effects to advance
/// the iterator. This wrapper uses interior mutability to deal with
/// [`serde`]'s API.
pub struct SerIterSeq<'a, Item: Serialize>(RefCell<Box<dyn Iterator<Item = Item> + 'a>>);

impl<'a, Item: Serialize> SerIterSeq<'a, Item> {
    /// Creates a new instance out of an iterator.
    pub fn new(iter: impl Iterator<Item = Item> + 'a) -> Self {
        Self(RefCell::new(Box::new(iter)))
    }
}

impl<'a, Item: Serialize> Serialize for SerIterSeq<'a, Item> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.0.borrow_mut().by_ref())
    }
}

impl<'a, Item: Serialize> Debug for SerIterSeq<'a, Item> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SerIterMap<{}>", type_name::<Item>())
    }
}

impl<'a, Item: Serialize, I: Iterator<Item = Item> + 'a> From<I> for SerIterSeq<'a, Item> {
    fn from(value: I) -> Self {
        Self::new(value)
    }
}

/// A wrapper for iterators that can be serialized as a key-value map.
///
/// Serializing is usually understood as a process without side-effects.
/// To serialize an iterator, one must introduce side-effects to advance
/// the iterator. This wrapper uses interior mutability to deal with
/// [`serde`]'s API.
pub struct SerIterMap<'a, Key: Serialize, Value: Serialize>(
    RefCell<Box<dyn Iterator<Item = (Key, Value)> + 'a>>,
);

impl<'a, Key: Serialize, Value: Serialize> SerIterMap<'a, Key, Value> {
    /// Creates a new instance out of an iterator.
    pub fn new(iter: impl Iterator<Item = (Key, Value)> + 'a) -> Self {
        Self(RefCell::new(Box::new(iter)))
    }
}

impl<'a, Key: Serialize, Value: Serialize> Debug for SerIterMap<'a, Key, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SerIterMap<{},{}>",
            type_name::<Key>(),
            type_name::<Value>()
        )
    }
}

impl<'a, Key: Serialize, Value: Serialize> Serialize for SerIterMap<'a, Key, Value> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(self.0.borrow_mut().by_ref())
    }
}

impl<'a, Key: Serialize, Value: Serialize, I: Iterator<Item = (Key, Value)> + 'a> From<I>
    for SerIterMap<'a, Key, Value>
{
    fn from(value: I) -> Self {
        Self::new(value)
    }
}
