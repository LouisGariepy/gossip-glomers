use std::sync::{Mutex, MutexGuard};

use common::{
    define_msg_kind,
    id::NodeId,
    message::{Message, Response, TopologyRequest, TopologyResponse},
    node::{LifetimeGeneric, NodeBuilder, NodeChannel},
    FxIndexSet,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default)]
struct NodeState {
    messages: Mutex<FxIndexSet<u64>>,
}

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(initialize_node)
        .build::<BroadcastRequest, BroadcastResponse, (), ()>()
        .run(|node, msg| async move {
            match msg.body.kind {
                BroadcastRequest::Broadcast { message } => {
                    node.state.messages.lock().unwrap().insert(message);
                    node.send_response(Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: BroadcastResponse::BroadcastOk {},
                        },
                    });
                }
                BroadcastRequest::Read {} => {
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
            }
        });
}

fn initialize_node(_: &NodeId, channel: &mut NodeChannel) -> NodeState {
    let topology_msg = channel.receive_msg::<TopologyRequest>();
    channel.send_msg(&Message {
        src: topology_msg.dest,
        dest: topology_msg.src,
        body: Response {
            in_reply_to: topology_msg.body.msg_id,
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
