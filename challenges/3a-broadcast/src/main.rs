use std::sync::Mutex;

use common::{
    message::{
        InboundBroadcastRequest, Message, OutboundBroadcastResponse, Response, TopologyRequest,
        TopologyResponse,
    },
    node::{NodeBuilder, NodeChannel},
    FxIndexSet,
};

#[derive(Debug, Default)]
struct NodeState {
    messages: Mutex<FxIndexSet<u64>>,
}

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(initialize_node)
        .build::<InboundBroadcastRequest, ()>()
        .run(|node, msg| async move {
            match msg.body.kind {
                InboundBroadcastRequest::Broadcast { message } => {
                    node.state.messages.lock().unwrap().insert(message);
                    node.send_msg(&Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundBroadcastResponse::BroadcastOk {},
                        },
                    });
                }
                InboundBroadcastRequest::Read {} => {
                    let messages = &node.state.messages.lock().unwrap();
                    let ser_msg = Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundBroadcastResponse::ReadOk { messages },
                        },
                    }
                    .to_json();
                    node.send_msg(ser_msg);
                }
                InboundBroadcastRequest::Healing { .. } => unreachable!(),
            }
        });
}

fn initialize_node(channel: &mut NodeChannel) -> NodeState {
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
