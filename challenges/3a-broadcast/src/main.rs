use messages::{InboundRequest, OutboundResponse};

use common::{
    message::{Message, Response, TopologyRequest, TopologyResponse},
    node::{respond, NodeBuilder, NodeBuilderData, NodeChannel, NodeTrait, SimpleNode},
    FxIndexSet, HealthyMutex,
};

mod messages;

#[tokio::main]
async fn main() {
    let builder = NodeBuilder::init().with_state(initialize_node);
    SimpleNode::<InboundRequest, _>::build(builder).run(|node, request| async move {
        match request.body.kind {
            InboundRequest::Read {} => {
                // Send this node's recorded messages
                respond!(
                    node,
                    request,
                    OutboundResponse::ReadOk {
                        messages: &node.state.lock(),
                    }
                );
            }
            InboundRequest::Broadcast { message } => {
                // Insert the new message in the node's message set
                node.state.lock().insert(message);
                // Send OK response
                respond!(node, request, OutboundResponse::BroadcastOk {});
            }
        }
    });
}

fn initialize_node(
    _: &NodeBuilderData,
    channel: &mut NodeChannel,
) -> HealthyMutex<FxIndexSet<u64>> {
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

    HealthyMutex::default()
}
