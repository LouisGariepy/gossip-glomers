use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, MutexGuard,
};

use common::{
    define_msg_kind,
    id::{MessageId, NodeId, SiteId},
    message::{Message, Request, Response, TopologyRequest, TopologyResponse},
    node::{NodeBuilder, NodeChannel},
    FxIndexSet,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default)]
struct NodeState {
    next_msg_id: AtomicU64,
    messages: Mutex<FxIndexSet<u64>>,
    neighbours: Vec<NodeId>,
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
        .build::<InboundBroadcastRequest, OutboundBroadcastResponse, OutboundBroadcastRequest, InboundBroadcastResponse>()
        .run(|node, msg| async move {
            match msg.body.kind {
                InboundBroadcastRequest::Broadcast { message } => {
                    // Insert the broadcasted message
                    let inserted = node.state.messages.lock().unwrap().insert(message);

                    // If the message was inserted (i.e. it was new to this node), then
                    // start broadcasting
                    if inserted {
                        // All neighbours are susceptible except the
                        // one who just broadcasted the message to us
                        let susceptible_neighbours = node
                            .state
                            .neighbours
                            .iter()
                            .filter(|&&neighbour_id| neighbour_id != msg.src);

                        // Broadcast to all susceptible neighbours
                        for neighbour in susceptible_neighbours {
                            let node = node.clone();
                            let neighbour = *neighbour;
                            // Spawn a task for this RPC request
                            tokio::spawn(async move {
                                // Create the RPC request
                                let msg_id = MessageId(node.state.next_msg_id());
                                let rpc_msg = Message {
                                    src: msg.dest,
                                    dest: SiteId::Node(neighbour),
                                    body: Request {
                                        msg_id,
                                        kind: OutboundBroadcastRequest::Broadcast { message },
                                    },
                                };
                                // Send RPC request and await response
                                let response = node
                                    .rpc(rpc_msg)
                                    .await
                                    .expect("RPC request did not time out");
                                // Assert that the response is a `BroadcastOk`
                                assert!(
                                    matches!(
                                        response.body.kind,
                                        InboundBroadcastResponse::BroadcastOk { .. }
                                    ),
                                    "expected a `BroadcastOk` response"
                                );
                            });
                        }
                    }
                    // Respond to the inbound broadcast request with a `BroadcastOk`
                    node.send_response(Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundBroadcastResponse::BroadcastOk {},
                        },
                    });
                }
                InboundBroadcastRequest::Read {} => {
                    // Respond to the read request with a response containing the list of all this
                    // node's current messages.
                    node.send_response(Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundBroadcastResponse::ReadOk {
                                messages: node.state.messages.lock().unwrap(),
                            },
                        },
                    });
                }
            }
        });
}

fn initialize_node(node_id: &NodeId, channel: &mut NodeChannel) -> NodeState {
    // Receive and respond to initial topology request
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

    // Extract neighbours from topology request
    let mut topology = topology_request.body.kind.into_inner().topology;
    let neighbours = topology
        .remove(node_id)
        .expect("the topology should include this node's neighbours");

    NodeState {
        next_msg_id: Default::default(),
        messages: Default::default(),
        neighbours,
    }
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    pub enum InboundBroadcastRequest {
        Read {},
        Broadcast { message: u64 },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum OutboundBroadcastRequest {
        Read {},
        Broadcast { message: u64 },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum OutboundBroadcastResponse<'a> {
        ReadOk {
            #[serde(serialize_with = "common::serialize_guard")]
            messages: MutexGuard<'a, FxIndexSet<u64>>,
        },
        BroadcastOk {},
    }
);

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    pub enum InboundBroadcastResponse {
        ReadOk { messages: FxIndexSet<u64> },
        BroadcastOk {},
    }
);
