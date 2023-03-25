use std::sync::Arc;

use serde::{Deserialize, Serialize};

use common::{define_msg_kind, Message, MessageId, Never, NodeBuilder, NodeId, Response};

type Node = common::Node<(), InboundRequest, OutboutResponse, Never, Never>;

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .build()
        .run(|node: Arc<Node>, msg| async move {
            // Send a unique id made up of the node's id and
            // an atomically incremented msg id.
            node.send_response(Message {
                src: msg.dest,
                dest: msg.src,
                body: Response {
                    in_reply_to: msg.body.msg_id,
                    kind: OutboutResponse::GenerateOk(GenerateOk {
                        id: (node.node_id, node.next_msg_id()),
                    }),
                },
            });
        });
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    enum InboundRequest {
        Generate {},
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    enum OutboutResponse {
        GenerateOk { id: (NodeId, MessageId) },
    }
);
