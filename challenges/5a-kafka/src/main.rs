use std::{
    iter::{Copied, Enumerate, Skip},
    slice::Iter,
    sync::Arc,
    vec::IntoIter,
};

use common::{
    define_msg_kind, respond, FxHashMap, HealthyMutex, Message, Never, NodeBuilder, PushGetIndex,
    Request, Response, SerializableIterator,
};
use serde::{Deserialize, Serialize};

struct NodeState {
    logs: HealthyMutex<FxHashMap<String, Vec<u64>>>,
}

type Node = common::Node<NodeState, InboundRequest, OutboundResponse, Never, Never>;

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
            let mut poll_ok = FxHashMap::default();
            let guard = node.state.logs.lock();
            for (key, offset) in offsets {
                let values = match guard.get(&key) {
                    Some(a) => a.iter().copied().enumerate().skip(offset - 1),
                    None => continue,
                };
                poll_ok.insert(key, SerializableIterator::new(values));
            }
            // node.serialize_response(Message {
            //     src: request_msg.dest,
            //     dest: request_msg.src,
            //     body: Response {
            //         in_reply_to: request_msg.body.msg_id,
            //         kind: OutboundResponse::PollOk { msgs: poll_ok },
            //     },
            // });
            respond!(
                node,
                request_msg,
                OutboundResponse::PollOk { msgs: poll_ok }
            );
        }
        InboundRequest::CommitOffsets { offsets } => todo!(),
        InboundRequest::ListCommittedOffsets { keys } => todo!(),
    }
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    pub enum InboundRequest {
        Send { key: String, msg: u64 },
        Poll { offsets: FxHashMap<String, usize> },
        CommitOffsets { offsets: FxHashMap<String, usize> },
        ListCommittedOffsets { keys: Vec<String> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum OutboundResponse {
        SendOk {
            offset: usize,
        },
        PollOk {
            msgs: FxHashMap<
                String,
                SerializableIterator<
                    std::iter::Skip<
                        std::iter::Enumerate<std::iter::Copied<std::slice::Iter<'static, u64>>>,
                    >,
                >,
            >,
        },
        CommitOffsetsOk {},
        ListCommittedOffsetsOk {
            offsets: FxHashMap<String, usize>,
        },
    }
);
