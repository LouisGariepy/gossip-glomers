use std::sync::Arc;

use serde::{Deserialize, Serialize};

use common::{define_msg_kind, Message, Never, NodeBuilder, Response};

type Node = common::Node<(), InboundRequest, OutboundResponse, Never, Never>;

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .build()
        .run(|node: Arc<Node>, msg| async move {
            // Send echo back as response
            node.send_response(Message {
                src: msg.dest,
                dest: msg.src,
                body: Response {
                    in_reply_to: msg.body.msg_id,
                    kind: OutboundResponse::EchoOk(EchoOk {
                        echo: msg.body.kind.into_inner().echo,
                    }),
                },
            });
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
