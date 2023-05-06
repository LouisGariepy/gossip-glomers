use std::sync::Arc;

use messages::{InboundRequest, InboundResponse, OutboundRequest, OutboundResponse};

use common::{
    id::NodeId,
    message::{Message, Response, TopologyRequest, TopologyResponse},
    node::{self, respond, rpc, NodeBuilder, NodeBuilderData, NodeChannel, NodeTrait},
    FxIndexSet, HealthyMutex,
};

mod messages;

type Node = node::Node<InboundRequest, InboundResponse, NodeState>;

#[derive(Debug, Default)]
struct NodeState {
    /// The set of all messages held by this node.
    messages: HealthyMutex<FxIndexSet<u64>>,
    /// The direct neighbours of this node.
    neighbours: Vec<NodeId>,
}

#[tokio::main]
async fn main() {
    let builder = NodeBuilder::init().with_state(initialize_node);
    Node::build(builder).run(|node: Arc<Node>, request| async move {
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
                                let response =
                                    rpc!(node, neighbour, OutboundRequest::Broadcast { message })
                                        .await
                                        .expect("RPC request should not time out");
                                // Assert that the response is OK
                                assert!(
                                    matches!(
                                        response.body.kind,
                                        InboundResponse::BroadcastOk { .. }
                                    ),
                                    "expected a `BroadcastOk` response, got {:?} instead",
                                    response.body.kind,
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
    });
}

fn initialize_node(builder_data: &NodeBuilderData, channel: &mut NodeChannel) -> NodeState {
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
    let mut topology = topology_request.body.kind.topology();
    let neighbours = topology
        .remove(&builder_data.id)
        .expect("the topology should include this node's neighbours");

    NodeState {
        messages: HealthyMutex::default(),
        neighbours,
    }
}
