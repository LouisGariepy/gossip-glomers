use std::sync::Arc;

use common::{
    define_msg_kind, respond, FxIndexSet, HealthyMutex, Message, Never, NodeBuilder, NodeChannel,
    NodeId, Response, TopologyRequest, TopologyResponse,
};
use serde::{Deserialize, Serialize};

type Node = common::Node<NodeState, InboundRequest, OutboundResponse<'static>, Never, Never>;

#[derive(Debug, Default)]
struct NodeState {
    /// The set of all messages held by this node.
    messages: HealthyMutex<FxIndexSet<u64>>,
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
                    // Insert the new message in the node's message set
                    node.state.messages.lock().insert(message);
                    // Send OK response
                    respond!(node, request, OutboundResponse::BroadcastOk {});
                }
            }
        },
    );
}

fn initialize_node(_: NodeId, channel: &mut NodeChannel) -> NodeState {
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

    NodeState {
        messages: HealthyMutex::default(),
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
