use std::sync::{Arc, Mutex, MutexGuard};

use common::{
    define_msg_kind, FxIndexSet, Message, NodeBuilder, NodeChannel, NodeId, Request, Response,
    TopologyRequest, TopologyResponse,
};
use serde::{Deserialize, Serialize};

type Node = common::Node<
    NodeState,
    InboundRequest,
    OutboundResponse<'static>,
    OutboundRequest,
    InboundResponse,
>;

#[derive(Debug, Default)]
struct NodeState {
    /// The set of all messages held by this node.
    messages: Mutex<FxIndexSet<u64>>,
    /// The direct neighbours of this node.
    neighbours: Vec<NodeId>,
}

#[tokio::main]
async fn main() {
    NodeBuilder::init().with_state(initialize_node).build().run(
        |node: Arc<Node>, msg| async move {
            match msg.body.kind {
                InboundRequest::Read {} => {
                    // Send this node's recorded messages
                    node.send_response(Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundResponse::ReadOk {
                                messages: node.state.messages.lock().unwrap(),
                            },
                        },
                    });
                }
                InboundRequest::Broadcast { message } => {
                    // Try to insert the new message in the node's message set
                    let inserted = node.state.messages.lock().unwrap().insert(message);

                    // If the message was newly inserted
                    // then start the broadcasting procedure
                    if inserted {
                        // Broadcast to all susceptible neighbours
                        for neighbour in &node.state.neighbours {
                            // Spawn a task for this RPC request
                            tokio::spawn({
                                let node = Arc::clone(&node);
                                let neighbour = *neighbour;
                                async move {
                                    // Send broadcast request and await response

                                    // Create the RPC request
                                    // Send RPC request and await response
                                    let response = node
                                        .rpc(Message {
                                            src: msg.dest,
                                            dest: neighbour.into(),
                                            body: Request {
                                                msg_id: node.next_msg_id(),
                                                kind: OutboundRequest::Broadcast(Broadcast {
                                                    message,
                                                }),
                                            },
                                        })
                                        .await
                                        .expect("RPC request did not time out");
                                    // Assert that the response is OK
                                    assert!(
                                        matches!(
                                            response.body.kind,
                                            InboundResponse::BroadcastOk { .. }
                                        ),
                                        "expected a `BroadcastOk` response"
                                    );
                                }
                            });
                        }
                    }

                    // After broadcasting procedure is
                    // complete, send OK response
                    node.send_response(Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundResponse::BroadcastOk {},
                        },
                    });
                }
            }
        },
    );
}

fn initialize_node(node_id: NodeId, channel: &mut NodeChannel) -> NodeState {
    // Receive and respond to initial topology request
    let topology_request = channel.receive_msg::<TopologyRequest>();
    channel.send_msg(Message {
        src: topology_request.dest,
        dest: topology_request.src,
        body: Response {
            in_reply_to: topology_request.body.msg_id,
            kind: TopologyResponse::TopologyOk {},
        },
    });

    // Extract neighbours from topology request
    let mut topology = topology_request.body.kind.into_inner().topology;
    let neighbours = topology
        .remove(&node_id)
        .expect("the topology should include this node's neighbours");

    NodeState {
        messages: Mutex::default(),
        neighbours,
    }
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    enum InboundRequest {
        Read {},
        Broadcast { message: u64 },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    enum OutboundResponse<'a> {
        ReadOk {
            #[serde(serialize_with = "common::serialize_guard")]
            messages: MutexGuard<'a, FxIndexSet<u64>>,
        },
        BroadcastOk {},
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    enum OutboundRequest {
        Broadcast { message: u64 },
    }
);

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    enum InboundResponse {
        BroadcastOk {},
    }
);
