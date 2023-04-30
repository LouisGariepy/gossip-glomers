use std::{cell::RefCell, marker::PhantomData};

use serde::{Deserialize, Serialize};
use serde_json::value::{to_raw_value, RawValue};

use crate::message::Message;

/// Marker type denoting a request that was serialized.
pub struct JsonRequest;

/// Marker type denoting a response that was serialized.
pub struct JsonResponse;

/// A strongly-typed JSON type.
///
/// The purpose of this type is to restrict the ways you can construct
/// serialized values. This prevents accidentally sending serialized
/// messages that are not supported by the nodes.
#[derive(Debug)]
pub struct Json<T>(String, PhantomData<T>);

impl<Body: Serialize> Message<Body> {
    /// Crate-private utility to serialize a message to [`Json`].
    pub(crate) fn into_json<T>(self) -> Json<T> {
        Json(serde_json::to_string(&self).unwrap(), PhantomData)
    }
}

impl<'de, Body: Deserialize<'de>> Message<Body> {
    /// Crate-private utility to deserialize a JSON string to a message.
    pub(crate) fn from_json_str(s: &'de str) -> Self {
        serde_json::from_str(s).unwrap()
    }
}

impl<T> Json<T> {
    /// Crate-private utility to obtain a string
    /// slice reference out of a [`Json`] struct
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// A trait for iterators that can be serialized.
///
/// Serializing is usually understood as a process without side-effects.
/// To serialize an iterator, one must introduce side-effects to advance
/// the iterator. This requires using interior mutability to deal with
/// [`serde`]'s API.
struct SerIterSeq<I>(RefCell<I>);

impl<Item: Serialize, Iter: Iterator<Item = Item>> Serialize for SerIterSeq<Iter> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.0.borrow_mut().by_ref())
    }
}

/// Typed wrapper around a [`RawValue`] used to represent an iterator,
/// serializable as a sequence. This is useful because iterator
/// types are not always nameable and therefore can cause proliferation
/// of generics or the need for dynamic dispatch.
#[derive(Debug)]
pub struct SerIterSeqJson<Item>(Box<RawValue>, PhantomData<Item>);

impl<Item> Serialize for SerIterSeqJson<Item> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

struct SerIterMap<I>(RefCell<I>);

impl<Key: Serialize, Value: Serialize, Iter: Iterator<Item = (Key, Value)>> Serialize
    for SerIterMap<Iter>
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(self.0.borrow_mut().by_ref())
    }
}

/// Typed wrapper around a [`RawValue`] used to represent an iterator
/// of (key, value) pairs, serializable as a map. This is useful because iterator
/// types are not always nameable and therefore can cause proliferation
/// of generics or the need for dynamic dispatch.
#[derive(Debug)]
pub struct SerIterMapJson<Key, Value>(Box<RawValue>, PhantomData<(Key, Value)>);

impl<Key, Value> Serialize for SerIterMapJson<Key, Value> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

/// An extension trait for iterators that can be serialized as sequences.
pub trait SerializeIteratorSeq {
    /// The type of item that the iterator yields.
    type Item;

    /// Converts the iterator into a serde-compatible opaque wrapper.
    /// This wrapper will serialize as a sequence.
    fn to_json_seq(self) -> SerIterSeqJson<Self::Item>;
}

impl<I, Item: Serialize> SerializeIteratorSeq for I
where
    I: Iterator<Item = Item>,
{
    type Item = Item;

    fn to_json_seq(self) -> SerIterSeqJson<Self::Item> {
        SerIterSeqJson(
            to_raw_value(&SerIterSeq(RefCell::new(self))).unwrap(),
            PhantomData,
        )
    }
}

/// An extension trait for iterators of pairs that can be serialized as maps.
/// Pairs must be in the form of a `(key, value)` tuple.
pub trait SerializeIteratorMap {
    /// The type of the keys that the iterator yields.
    /// Keys must are the first element of the item tuple.
    type Key;

    /// The type of the values that the iterator yields.
    /// Values must are the second element of the item tuple.
    type Value;

    /// Converts the iterator into a serde-compatible opaque wrapper.
    /// This wrapper will serialize as a map.
    fn to_json_map(self) -> SerIterMapJson<Self::Key, Self::Value>;
}

impl<I, Key: Serialize, Value: Serialize> SerializeIteratorMap for I
where
    I: Iterator<Item = (Key, Value)>,
{
    type Key = Key;

    type Value = Value;

    fn to_json_map(self) -> SerIterMapJson<Self::Key, Self::Value> {
        SerIterMapJson(
            to_raw_value(&SerIterMap(RefCell::new(self))).unwrap(),
            PhantomData,
        )
    }
}
