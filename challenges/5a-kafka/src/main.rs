use std::sync::Arc;

use common::{
    respond, FxHashMap, HealthyMutex, Message, Never, NodeBuilder, PushGetIndex, Request, Response,
    SerIterMapJson, SerIterSeqJson, SerializeIteratorMap, SerializeIteratorSeq,
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
            let poll_ok = {
                let logs_guard = node.state.logs.lock();
                offsets
                    .into_iter()
                    .filter_map(|(key, offset)| {
                        logs_guard.get(&key).map(|a| {
                            let logs = a.iter().copied().enumerate().skip(offset - 1).to_json_seq();
                            (key, logs)
                        })
                    })
                    .to_json_map()
            };
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

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InboundRequest {
    Send { key: String, msg: u64 },
    Poll { offsets: FxHashMap<String, usize> },
    CommitOffsets { offsets: FxHashMap<String, usize> },
    ListCommittedOffsets { keys: Vec<String> },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum OutboundResponse {
    SendOk {
        offset: usize,
    },
    PollOk {
        msgs: SerIterMapJson<String, SerIterSeqJson<(usize, u64)>>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: FxHashMap<String, usize>,
    },
}
