use serde::{Deserialize, Serialize};

use common::{
    id::{MsgId, MsgIdGenerator, NodeId},
    node::{NodeBuilder, NodeTrait, SimpleNode},
    respond,
};

#[tokio::main]
async fn main() {
    let builder = NodeBuilder::init().with_state(|_, _| MsgIdGenerator::default());
    SimpleNode::<InboundRequest, OutboundResponse, _>::build(builder).run(
        |node, request| async move {
            // Send a unique id made up of the node's id and
            // an atomically incremented msg id.
            respond!(
                node,
                request,
                OutboundResponse::GenerateOk {
                    id: (node.id, node.state.next()),
                }
            );
        },
    );
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InboundRequest {
    Generate {},
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutboundResponse {
    GenerateOk { id: (NodeId, MsgId) },
}
