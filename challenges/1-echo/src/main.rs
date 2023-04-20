use std::sync::Arc;

use serde::{Deserialize, Serialize};

use common::{respond, Message, NodeBuilder, Response};

type Node = common::SimpleNode<(), InboundRequest, OutboundResponse>;

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .build_simple()
        .run(|node: Arc<Node>, request| async move {
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

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InboundRequest {
    Echo { echo: String },
}

impl InboundRequest {
    fn echo(self) -> String {
        match self {
            InboundRequest::Echo { echo } => echo,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutboundResponse {
    EchoOk { echo: String },
}
