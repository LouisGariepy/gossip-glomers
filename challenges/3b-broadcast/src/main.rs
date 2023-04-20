use std::sync::Arc;

use common::{
    define_msg_kind, respond, rpc, FxIndexSet, HealthyMutex, Message, NodeBuilder, NodeChannel,
    NodeId, Request, Response, TopologyRequest, TopologyResponse,
};
use serde::{Deserialize, Serialize};

type Node<'a> =
    common::Node<NodeState, InboundRequest, OutboundResponse<'a>, OutboundRequest, InboundResponse>;

#[derive(Debug, Default)]
struct NodeState {
    /// The set of all messages held by this node.
    messages: HealthyMutex<FxIndexSet<u64>>,
    /// The direct neighbours of this node.
    neighbours: Vec<NodeId>,
}

#[tokio::main]
async fn main() {
    NodeBuilder::init().with_state(initialize_node).build().run(
        |node: Arc<Node>, request| async move {
            match request.body.kind {
                InboundRequest::Read {} => {
                    // Send this node's recorded messages
                    respond!(
                        node,
                        request,
                        OutboundResponse::ReadOk {
                            messages: &node.state.messages.lock(),
                        }
                    );
                }
                InboundRequest::Broadcast { message } => {
                    // Try to insert the new message in the node's message set
                    let inserted = node.state.messages.lock().insert(message);

                    // If the message was newly inserted
                    // then start the broadcasting procedure
                    if inserted {
                        // Broadcast to all susceptible neighbours
                        for neighbour in node.state.neighbours.iter().copied() {
                            // Spawn a task for this RPC request
                            tokio::spawn({
                                let node = Arc::clone(&node);
                                async move {
                                    // Create the RPC request
                                    // Send RPC request and await response
                                    let response = rpc!(
                                        node,
                                        neighbour,
                                        OutboundRequest::Broadcast(Broadcast { message })
                                    )
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
                    respond!(node, request, OutboundResponse::BroadcastOk {});
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
        messages: HealthyMutex::default(),
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
        ReadOk { messages: &'a FxIndexSet<u64> },
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
