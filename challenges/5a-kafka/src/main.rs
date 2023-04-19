use std::{
    collections::hash_map::IntoIter,
    iter::{Enumerate, FilterMap, Skip},
    slice::Iter,
    sync::Arc,
};

use common::{
    respond, FxHashMap, HealthyMutex, Message, MessageWithLifetime, Never, NodeBuilder,
    PushGetIndex, Request, Response, SerializableIterator,
};
use serde::{Deserialize, Serialize};

struct NodeState {
    logs: HealthyMutex<FxHashMap<String, Vec<u64>>>,
}

type Node = common::Node<NodeState, InboundRequest, OutboundResponse<'static>, Never, Never>;

fn main() {
    NodeBuilder::init()
        .with_state(|_, _| NodeState {
            logs: Default::default(),
        })
        .build()
        .run(request_handler);
}

async fn request_handler(node: Arc<Node>, request_msg: Message<Request<InboundRequest>>) {
    match request_msg.body.kind {
        InboundRequest::Send { key, msg } => {
            let offset = node
                .state
                .logs
                .lock()
                .entry(key)
                .or_insert(Default::default())
                .push_get_index(msg);
            respond!(node, request_msg, OutboundResponse::SendOk { offset });
        }
        InboundRequest::Poll { offsets } => {
            let guard = node.state.logs.lock();
            let poll_ok = offsets.into_iter().filter_map(|(key, offset)| {
                guard.get(&key).map(move |a| {
                    (
                        key,
                        SerializableIterator::new(a.iter().enumerate().skip(offset - 1)),
                    )
                })
            });
            serde_json::to_writer(String::new(), value)
            respond!(
                node,
                request_msg,
                OutboundResponse::PollOk {
                    msgs: SerializableIterator::new(poll_ok)
                }
            );
        }
        InboundRequest::CommitOffsets { offsets } => todo!(),
        InboundRequest::ListCommittedOffsets { keys } => todo!(),
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InboundRequest {
    Send { key: String, msg: u64 },
    Poll { offsets: FxHashMap<String, usize> },
    CommitOffsets { offsets: FxHashMap<String, usize> },
    ListCommittedOffsets { keys: Vec<String> },
}

impl MessageWithLifetime for InboundRequest {
    type Msg<'a> = InboundRequest;
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum OutboundResponse<'a> {
    SendOk { offset: usize },
    PollOk { msgs: PollOk<'a> },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk { offsets: FxHashMap<String, usize> },
}

impl<'a> MessageWithLifetime for OutboundResponse<'a> {
    type Msg<'b> = OutboundResponse<'b>;
}

type PollOk<'a> = SerializableIterator<
    FilterMap<
        IntoIter<String, usize>,
        fn(
            (String, usize),
        ) -> Option<(String, SerializableIterator<Skip<Enumerate<Iter<'a, u64>>>>)>,
    >,
>;
