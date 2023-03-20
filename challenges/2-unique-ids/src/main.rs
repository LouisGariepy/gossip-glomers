use std::sync::atomic::{AtomicU64, Ordering};

use common::{
    message::{GenerateOk, GenerateRequest, GenerateResponse, Message, Response},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(|_| AtomicU64::new(0))
        .build::<GenerateRequest, ()>()
        .run(|node, msg| async move {
            node.send_msg(
                &Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Response {
                        in_reply_to: msg.body.msg_id,
                        kind: GenerateResponse::GenerateOk(GenerateOk {
                            id: [node.node_id.0, node.state.fetch_add(1, Ordering::SeqCst)],
                        }),
                    },
                }
                .to_json(),
            );
        });
}
