use messages::{InboundRequest, OutboundResponse};

use common::{
    id::MsgIdGenerator,
    node::{NodeBuilder, NodeTrait, SimpleNode},
    respond,
};

mod messages;

#[tokio::main]
async fn main() {
    let builder = NodeBuilder::init().with_state(|_, _| MsgIdGenerator::default());
    SimpleNode::<InboundRequest, _>::build(builder).run(|node, request| async move {
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
