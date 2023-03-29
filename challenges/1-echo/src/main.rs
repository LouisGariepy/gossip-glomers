use std::sync::Arc;

use serde::{Deserialize, Serialize};

use common::{define_msg_kind, respond, Message, Never, NodeBuilder, Response};

type Node = common::Node<(), InboundRequest, OutboundResponse, Never, Never>;

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .build()
        .run(|node: Arc<Node>, request| async move {
            // Send echo back as response
            respond!(
                node,
                request,
                OutboundResponse::EchoOk(EchoOk {
                    echo: request.body.kind.into_inner().echo,
                })
            );
        });
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    enum InboundRequest {
        Echo { echo: String },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    enum OutboundResponse {
        EchoOk { echo: String },
    }
);
