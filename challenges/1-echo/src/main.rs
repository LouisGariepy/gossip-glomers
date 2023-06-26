mod messages;

use common::node::{respond, BuildNode, NodeBuilder, SimpleNode};

use messages::{InboundRequest, OutboundResponse};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .build::<SimpleNode<InboundRequest>>()
        .run(|node, request| async move {
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
