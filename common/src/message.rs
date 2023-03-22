use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{
    id::{MessageId, NodeId, SiteId},
    node::LifetimeGeneric,
    TopologyMap,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageType<RequestBody, ResponseBody> {
    Request(Request<RequestBody>),
    Response(Response<ResponseBody>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Request<Kind> {
    pub msg_id: MessageId,
    #[serde(flatten)]
    pub kind: Kind,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response<Kind> {
    pub in_reply_to: MessageId,
    #[serde(flatten)]
    pub kind: Kind,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<Body> {
    pub src: SiteId,
    pub dest: SiteId,
    pub body: Body,
}

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum InitRequest {
        Init {
            node_id: NodeId,
            node_ids: Vec<NodeId>,
        },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum InitResponse {
        InitOk {},
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum TopologyRequest {
        Topology { topology: TopologyMap },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum TopologyResponse {
        TopologyOk {},
    }
);

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

// #[macro_export]
// macro_rules! define_msg_kind {
//     (
//         #[derive($($derive:path),*)]
//         $vis:vis enum $enum_ident:ident {
//             $variant_ident:ident {
//                 $($field_ident:ident : $field_ty:ty),+ $(,)?
//             }$(,)?
//         }
//     ) =>
//     {
//         #[derive($($derive),*)]
//         #[serde(tag = "type")]
//         #[serde(rename_all = "snake_case")]
//         $vis enum $enum_ident{
//             $variant_ident($variant_ident),
//         }

//         #[derive($($derive),*)]
//         $vis struct $variant_ident {
//             $($vis $field_ident : $field_ty,)+
//         }

//         impl $enum_ident {
//             $vis fn as_inner(&self) -> &$variant_ident {
//                 match self {
//                     $enum_ident::$variant_ident(inner) => inner
//                 }
//             }
//             $vis fn into_inner(self) -> $variant_ident {
//                 match self {
//                     $enum_ident::$variant_ident(inner) => inner
//                 }
//             }
//         }
//     };
//     (
//         #[derive($($derive:path),*)]
//         $vis:vis enum $($tokens:tt)*
//     ) => {
//         #[derive($($derive),*)]
//         #[serde(tag = "type")]
//         #[serde(rename_all = "snake_case")]
//         $vis enum $($tokens)*
//     };
// }

#[macro_export]
macro_rules! define_msg_kind {
    (
        #[derive($($derive:path),*)]
        $vis:vis enum $enum_ident:ident<$lifetime:lifetime> {
            $variant_ident:ident {
                $($field_ident:ident : $field_ty:ty),+ $(,)?
            }$(,)?
        }
    ) => {
        define_msg_kind!(
            single_variant,
            [$($derive),*],
            $vis,
            $enum_ident,
            $lifetime,
            $variant_ident,
            $($field_ident : $field_ty),+
        );
        impl<'a> LifetimeGeneric for $enum_ident<'a> {
            type Me<'b> = $enum_ident<'b>;
        }
    };
    (
        #[derive($($derive:path),*)]
        $vis:vis enum $enum_ident:ident<$lifetime:lifetime> $($tokens:tt)*
    ) => {
        define_msg_kind!(
            mutliple_variants,
            [$($derive),*],
            $vis,
            $enum_ident,
            $lifetime,
            $($tokens)*
        );
        impl<'a> LifetimeGeneric for $enum_ident<'a> {
            type Me<'b> = $enum_ident<'b>;
        }
    };
    (
        #[derive($($derive:path),*)]
        $vis:vis enum $enum_ident:ident {
            $variant_ident:ident {
                $($field_ident:ident : $field_ty:ty),+ $(,)?
            }$(,)?
        }
    ) => {
        define_msg_kind!(
            single_variant,
            [$($derive),*],
            $vis,
            $enum_ident,
            ,
            $variant_ident,
            $($field_ident : $field_ty),+
        );
        impl LifetimeGeneric for $enum_ident {
            type Me<'b> = Self;
        }
    };
    (
        #[derive($($derive:path),*)]
        $vis:vis enum $enum_ident:ident $($tokens:tt)*
    ) => {
        define_msg_kind!(
            mutliple_variants,
            [$($derive),*],
            $vis,
            $enum_ident,
            ,
            $($tokens)*
        );
        impl LifetimeGeneric for $enum_ident {
            type Me<'b> = Self;
        }
    };
    (
        single_variant,
        [$($derive:path),*],
        $vis:vis,
        $enum_ident:ident,
        $($lifetime:lifetime)?,
        $variant_ident:ident,
        $($field_ident:ident : $field_ty:ty),+
    ) => {
        #[derive($($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        $vis enum $enum_ident <$($lifetime)?> {
            $variant_ident($variant_ident<$($lifetime)?>),
        }

        #[derive($($derive),*)]
        $vis struct $variant_ident <$($lifetime)?> {
            $($vis $field_ident : $field_ty,)+
        }

        impl<$($lifetime)?> $enum_ident<$($lifetime)?> {
            $vis fn as_inner(&self) -> &$variant_ident {
                match self {
                    $enum_ident::$variant_ident(inner) => inner
                }
            }
            $vis fn into_inner(self) -> $variant_ident<$($lifetime)?> {
                match self {
                    $enum_ident::$variant_ident(inner) => inner
                }
            }
        }
    };
    (
        mutliple_variants,
        [$($derive:path),*],
        $vis:vis,
        $enum_ident:ident,
        $($lifetime:lifetime)?,
        $($tokens:tt)*
    ) => {
        #[derive($($derive),*)]
        #[serde(tag = "type")]
        #[serde(rename_all = "snake_case")]
        $vis enum $enum_ident <$($lifetime)?> $($tokens)*
    };
}
pub use define_msg_kind;

impl LifetimeGeneric for () {
    type Me<'a> = Self;
}
