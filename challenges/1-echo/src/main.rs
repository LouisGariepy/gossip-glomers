use common::node::{respond, NodeBuilder, NodeTrait, SimpleNode};
use messages::{InboundRequest, OutboundResponse};

mod messages;

#[tokio::main]
async fn main() {
    let builder = NodeBuilder::init();
    SimpleNode::<InboundRequest, _>::build(builder).run(|node, request| async move {
        // Send echo back as response
        respond!(
            node,
            request,
            OutboundResponse::EchoOk {
                echo: request.body.kind.echo(),
            }
        );
    });
}
