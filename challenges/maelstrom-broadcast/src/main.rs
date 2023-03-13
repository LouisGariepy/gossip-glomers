use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

use common::{
    id::{MessageId, SiteId},
    message::{
        BroadcastRequest, InboundBroadcastResponse, Message, OutboundBroadcastResponse, Request,
        Response, TopologyRequest, TopologyResponse,
    },
    node::NodeBuilder,
};
use futures::{stream::FuturesUnordered, StreamExt};
use rustc_hash::FxHashSet;

#[derive(Debug, Default)]
struct NodeState {
    next_msg_id: AtomicU64,
    messages: Mutex<FxHashSet<u64>>,
}

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_initial_data(|channel| {
            let topology_msg = channel.receive_msg::<TopologyRequest>();
            channel.send_msg(&Message {
                src: topology_msg.dest,
                dest: topology_msg.src,
                body: Response {
                    in_reply_to: topology_msg.body.msg_id,
                    kind: TopologyResponse {},
                },
            });
            topology_msg.body.kind.topology
        })
        .with_state(NodeState::default())
        .run::<BroadcastRequest, InboundBroadcastResponse, _>(|node, msg| async move {
            match msg.body.kind {
                BroadcastRequest::Broadcast { message } => {
                    let inserted = node.state.messages.lock().unwrap().insert(message);
                    if inserted {
                        let neighbours = node
                            .initial_data
                            .get(&node.node_id)
                            .unwrap()
                            .iter()
                            .filter(|&&n| SiteId::Node(n) != msg.src);

                        neighbours
                            .map(|neighbour| {
                                let neighbour = *neighbour;
                                let msg_id = MessageId(
                                    node.state.next_msg_id.fetch_add(1, Ordering::SeqCst),
                                );
                                let rpc_msg = Message {
                                    src: msg.dest,
                                    dest: SiteId::Node(neighbour),
                                    body: Request {
                                        msg_id,
                                        kind: BroadcastRequest::Broadcast { message },
                                    },
                                };
                                node.rpc(msg_id, rpc_msg)
                            })
                            .collect::<FuturesUnordered<_>>()
                            .collect::<Vec<_>>()
                            .await;
                    }
                    node.send_msg(&Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundBroadcastResponse::BroadcastOk {},
                        },
                    });
                }
                BroadcastRequest::Read {} => {
                    let ser_msg = Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundBroadcastResponse::ReadOk {
                                messages: &node.state.messages.lock().unwrap(),
                            },
                        },
                    }
                    .to_json();
                    node.send_json_msg(&ser_msg);
                }
            }
        });
}
