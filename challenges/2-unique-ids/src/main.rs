use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use common::{
    define_msg_kind,
    message::{Message, Response},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(|_, _| AtomicU64::new(0))
        .build::<GenerateRequest, GenerateResponse, (), ()>()
        .run(|node, msg| async move {
            node.send_response(Message {
                src: msg.dest,
                dest: msg.src,
                body: Response {
                    in_reply_to: msg.body.msg_id,
                    kind: GenerateResponse::GenerateOk(GenerateOk {
                        id: [node.node_id.0, node.state.fetch_add(1, Ordering::SeqCst)],
                    }),
                },
            });
        });
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    pub enum GenerateRequest {
        Generate {},
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum GenerateResponse {
        GenerateOk { id: [u64; 2] },
    }
);
