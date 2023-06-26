mod messages;

use common::{
    id::MsgIdGenerator,
    node::{BuildNode, NodeBuilder, SimpleNode},
    respond,
};

use messages::{InboundRequest, OutboundResponse};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_default_state::<MsgIdGenerator>()
        .build::<SimpleNode<InboundRequest, _>>()
        .run(|node, request| async move {
            // Send a unique id made up of the node's id and
            // an atomically incremented msg id.
            respond!(
                node,
                request,
                OutboundResponse::GenerateOk {
                    id: (node.id, node.state.next()),
                }
            );
        });
}
