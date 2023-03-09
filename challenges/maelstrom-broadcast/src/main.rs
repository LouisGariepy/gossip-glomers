use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

use common::{
    id::{MessageId, SiteId},
    message::{BroadcastRequest, BroadcastResponse, Message, TopologyRequest, TopologyResponse},
    node::NodeBuilder,
};
use rustc_hash::FxHashSet;

#[derive(Debug, Default)]
struct NodeState {
    next_msg_id: AtomicU64,
    messages: Mutex<FxHashSet<u64>>,
}

#[tokio::main]
async fn main() {
    NodeBuilder::init_with(|channel| {
        let topology_msg = channel.receive_msg::<TopologyRequest>();
        channel.send_msg(&Message {
            src: topology_msg.dest,
            dest: topology_msg.src,
            body: TopologyResponse {
                in_reply_to: topology_msg.body.msg_id,
            },
        });
        topology_msg.body.topology
    })
    .with_state(NodeState::default())
    .run(|node, msg| match msg.body {
        BroadcastRequest::Broadcast { msg_id, message } => {
            let inserted = node.state.messages.lock().unwrap().insert(message);
            if inserted {
                let neighbours = node
                    .initial_data
                    .get(&node.node_id)
                    .unwrap()
                    .iter()
                    .filter(|&&n| SiteId::Node(n) != msg.src);

                for neighbour in neighbours {
                    let neighbour = *neighbour;
                    let node = node.clone();
                    let msg_id = node.state.next_msg_id.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(async move {
                        node.send_msg(&Message {
                            src: msg.dest,
                            dest: SiteId::Node(neighbour),
                            body: BroadcastRequest::Broadcast {
                                msg_id: MessageId(msg_id),
                                message,
                            },
                        })
                    });
                }
            }
            node.send_msg(&Message {
                src: msg.dest,
                dest: msg.src,
                body: BroadcastResponse::BroadcastOk {
                    in_reply_to: msg_id,
                },
            });
        }
        BroadcastRequest::Read { msg_id } => {
            let ser_msg = Message {
                src: msg.dest,
                dest: msg.src,
                body: BroadcastResponse::ReadOk {
                    messages: &node.state.messages.lock().unwrap(),
                    in_reply_to: msg_id,
                },
            }
            .to_json();
            node.send_json_msg(&ser_msg);
        }
    });
}
