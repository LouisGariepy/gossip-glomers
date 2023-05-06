use std::sync::Arc;

use messages::{InboundRequest, OutboundResponse};

use common::{
    message::{Message, Request},
    node::{self, respond, NodeBuilder, NodeTrait},
    FxHashMap, HealthyMutex, PushGetIndex,
};

mod messages;

#[derive(Default)]
struct Log {
    commited: usize,
    messages: Vec<u64>,
}

#[derive(Default)]
struct NodeState {
    logs: HealthyMutex<FxHashMap<String, Log>>,
}

type Node = node::SimpleNode<InboundRequest, NodeState>;

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
            let logs_guard = node.state.logs.lock();

            let poll_ok = offsets
                .into_iter()
                // Filter to keep only log entries that exist
                .filter_map(|(key, offset)| {
                    logs_guard.get(&key).map(|log| {
                        // Get all the message from the given offset onwards.
                        let log_messages =
                            log.messages.iter().copied().enumerate().skip(offset).into();
                        (key, log_messages)
                    })
                })
                .into();

            respond!(
                [logs_guard],
                node,
                request,
                OutboundResponse::PollOk { msgs: poll_ok }
            );
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
            let logs_guard = node.state.logs.lock();

            // Get the commited offset of all the given keys that exist on this node.
            let offsets = keys
                .into_iter()
                .filter_map(|key| logs_guard.get(&key).map(|log| (key, log.commited)))
                .into();

            respond!(
                [logs_guard],
                node,
                request,
                OutboundResponse::ListCommittedOffsetsOk { offsets },
            );
        }
    }
}
