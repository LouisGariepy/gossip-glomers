use std::sync::atomic::{AtomicU64, Ordering};

use common::{
    message::{GenerateRequest, GenerateResponse, Message, Response},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(AtomicU64::new(0))
        .run::<GenerateRequest, GenerateResponse, _>(|node, msg| async move {
            node.send_msg(&Message {
                src: msg.dest,
                dest: msg.src,
                body: Response {
                    in_reply_to: msg.body.msg_id,
                    kind: GenerateResponse {
                        id: [node.node_id.0, node.state.fetch_add(1, Ordering::SeqCst)],
                    },
                },
            })
        });
}
