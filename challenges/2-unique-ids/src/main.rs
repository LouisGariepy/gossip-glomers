use std::sync::Arc;

use serde::{Deserialize, Serialize};

use common::{
    define_msg_kind, respond, Message, MsgId, MsgIdGenerator, NodeBuilder, NodeId, Response,
};

type Node = common::SimpleNode<MsgIdGenerator, InboundRequest, OutboundResponse>;

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(|_, _| MsgIdGenerator::default())
        .build_simple()
        .run(|node: Arc<Node>, request| async move {
            // Send a unique id made up of the node's id and
            // an atomically incremented msg id.
            respond!(
                node,
                request,
                OutboundResponse::GenerateOk(GenerateOk {
                    id: (node.node_id, node.state.next()),
                })
            );
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
    enum OutboundResponse {
        GenerateOk { id: (NodeId, MsgId) },
    }
);
