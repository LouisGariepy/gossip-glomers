use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

use common::{
    id::{MessageId, SiteId},
    message::{
        InboundBroadcastRequest, InboundBroadcastResponse, Message, OutboundBroadcastRequest,
        OutboundBroadcastResponse, Request, Response, TopologyRequest, TopologyResponse,
    },
    node::{NodeBuilder, NodeChannel},
    FxIndexSet,
};

#[derive(Debug, Default)]
struct NodeState {
    next_msg_id: AtomicU64,
    messages: Mutex<FxIndexSet<u64>>,
}

impl NodeState {
    fn next_msg_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(initialize_node)
        .build::<InboundBroadcastRequest, InboundBroadcastResponse>()
        .run(|node, msg| async move {
            match msg.body.kind {
                InboundBroadcastRequest::Broadcast { message } => {
                    node.state.messages.lock().unwrap().insert(message);
                    if let SiteId::Client(_) = msg.src {
                        let neighbours = node
                            .node_ids
                            .iter()
                            .filter(|&&n| SiteId::Node(n) != msg.src);

                        for neighbour in neighbours {
                            let node = node.clone();
                            let neighbour = *neighbour;
                            tokio::spawn(async move {
                                let msg_id = MessageId(node.state.next_msg_id());
                                let rpc_msg = Message {
                                    src: msg.dest,
                                    dest: SiteId::Node(neighbour),
                                    body: Request {
                                        msg_id,
                                        kind: OutboundBroadcastRequest::Broadcast { message },
                                    },
                                };
                                node.rpc(msg_id, &rpc_msg.to_json()).await;
                            });
                        }
                    }
                    node.send_msg(
                        &Message {
                            src: msg.dest,
                            dest: msg.src,
                            body: Response {
                                in_reply_to: msg.body.msg_id,
                                kind: OutboundBroadcastResponse::BroadcastOk {},
                            },
                        }
                        .to_json(),
                    );
                }
                InboundBroadcastRequest::Read {} => {
                    node.send_msg(
                        &Message {
                            src: msg.dest,
                            dest: msg.src,
                            body: Response {
                                in_reply_to: msg.body.msg_id,
                                kind: OutboundBroadcastResponse::ReadOk {
                                    messages: &node.state.messages.lock().unwrap(),
                                },
                            },
                        }
                        .to_json(),
                    );
                }
                InboundBroadcastRequest::BroadcastMany { .. } => unreachable!(),
            }
        });
}

fn initialize_node(channel: &mut NodeChannel) -> NodeState {
    let topology_request = channel.receive_msg::<TopologyRequest>();
    let topology_response = Message {
        src: topology_request.dest,
        dest: topology_request.src,
        body: Response {
            in_reply_to: topology_request.body.msg_id,
            kind: TopologyResponse::TopologyOk {},
        },
    };
    channel.send_msg(&topology_response);
    NodeState {
        next_msg_id: Default::default(),
        messages: Default::default(),
    }
}
