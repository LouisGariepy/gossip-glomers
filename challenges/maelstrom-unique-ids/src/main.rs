use std::sync::atomic::{AtomicU64, Ordering};

use common::{
    message::{GenerateRequest, GenerateResponse, Message},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(AtomicU64::new(0))
        .run::<GenerateRequest, GenerateResponse>(|node, msg| Message {
            src: msg.dest,
            dest: msg.src,
            body: GenerateResponse {
                in_reply_to: msg.body.msg_id,
                id: [
                    node.info.node_id.0,
                    node.state.fetch_add(1, Ordering::SeqCst),
                ],
            },
        })
        .await;
}
