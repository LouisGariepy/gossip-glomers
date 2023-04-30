use std::sync::Arc;

use serde::{Deserialize, Serialize};

use common::{
    message::{Message, Request},
    node::{self, respond, NodeBuilder, NodeTrait},
    FxHashMap, HealthyMutex, PushGetIndex,
};

#[derive(Default)]
struct Log {
    commited: usize,
    messages: Vec<u64>,
}

#[derive(Default)]
struct NodeState {
    logs: HealthyMutex<FxHashMap<String, Log>>,
}

type Node = node::SimpleNode<InboundRequest, OutboundResponse, NodeState>;

#[tokio::main]
async fn main() {
    let builder = NodeBuilder::init().with_state(|_, _| NodeState::default());
    Node::build(builder).run(|node, request| async { request_handler(node, request) });
}

fn request_handler(node: Arc<Node>, request: Message<Request<InboundRequest>>) {
    match request.body.kind {
        InboundRequest::Send { key, msg } => {
            // Insert a new message in the log entry associated with the given key.
            // Record the offset of the inserted message.
            let offset = node
                .state
                .logs
                .lock()
                .entry(key)
                .or_default()
                .messages
                .push_get_index(msg);
            respond!(node, request, OutboundResponse::SendOk { offset });
        }
        InboundRequest::Poll { offsets } => {
            let poll_ok = {
                let logs_guard = node.state.logs.lock();
                offsets
                    .into_iter()
                    // Filter to keep only log entries that exist
                    .filter_map(|(key, offset)| {
                        logs_guard.get(&key).map(|log| {
                            // Get all the message from the given offset onwards.
                            let log_messages = log
                                .messages
                                .iter()
                                .copied()
                                .enumerate()
                                .skip(offset)
                                .collect();
                            (key, log_messages)
                        })
                    })
                    .collect()
            };
            respond!(node, request, OutboundResponse::PollOk { msgs: poll_ok });
        }
        InboundRequest::CommitOffsets { offsets } => {
            {
                let mut logs_guard = node.state.logs.lock();
                // Set the commited offset for all the given logs.
                for (key, offset) in offsets {
                    logs_guard.get_mut(&key).unwrap().commited = offset;
                }
            }
            respond!(node, request, OutboundResponse::CommitOffsetsOk {});
        }
        InboundRequest::ListCommittedOffsets { keys } => {
            let offsets = {
                let logs_guard = node.state.logs.lock();
                // Get the commited offset of all the given keys that exist on this node.
                keys.into_iter()
                    .filter_map(|key| logs_guard.get(&key).map(|log| (key, log.commited)))
                    .collect()
            };
            respond!(
                node,
                request,
                OutboundResponse::ListCommittedOffsetsOk { offsets }
            );
        }
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
        msgs: FxHashMap<String, Vec<(usize, u64)>>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: FxHashMap<String, usize>,
    },
}
