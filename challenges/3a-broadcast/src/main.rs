use std::sync::{Arc, Mutex, MutexGuard};

use common::{
    define_msg_kind,
    id::NodeId,
    message::{Message, Response, TopologyRequest, TopologyResponse},
    node::{NodeBuilder, NodeChannel},
    FxIndexSet,
};
use serde::{Deserialize, Serialize};

type Node = common::node::Node<NodeState, BroadcastRequest, BroadcastResponse<'static>, (), ()>;

#[derive(Debug, Default)]
struct NodeState {
    /// The set of all messages held by this node.
    messages: Mutex<FxIndexSet<u64>>,
}

#[tokio::main]
async fn main() {
    NodeBuilder::init().with_state(initialize_node).build().run(
        |node: Arc<Node>, msg| async move {
            match msg.body.kind {
                BroadcastRequest::Read {} => {
                    // Send this node's recorded messages
                    node.send_response(Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: BroadcastResponse::ReadOk {
                                messages: node.state.messages.lock().unwrap(),
                            },
                        },
                    });
                }
                BroadcastRequest::Broadcast { message } => {
                    // Insert the new message in the node's message set
                    node.state.messages.lock().unwrap().insert(message);
                    // Send OK response
                    node.send_response(Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: BroadcastResponse::BroadcastOk {},
                        },
                    });
                }
            }
        },
    );
}

fn initialize_node(_: &NodeId, channel: &mut NodeChannel) -> NodeState {
    // Receive and respond to initial topology request
    let topology_request = channel.receive_msg::<TopologyRequest>();
    channel.send_msg(&Message {
        src: topology_request.dest,
        dest: topology_request.src,
        body: Response {
            in_reply_to: topology_request.body.msg_id,
            kind: TopologyResponse::TopologyOk {},
        },
    });

    NodeState {
        messages: Default::default(),
    }
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    pub enum BroadcastRequest {
        Read {},
        Broadcast { message: u64 },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum BroadcastResponse<'a> {
        ReadOk {
            #[serde(serialize_with = "common::serialize_guard")]
            messages: MutexGuard<'a, FxIndexSet<u64>>,
        },
        BroadcastOk {},
    }
);
