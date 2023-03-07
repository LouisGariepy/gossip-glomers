use std::sync::Mutex;

use common::{
    message::{BroadcastRequest, BroadcastResponse, Message},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(Mutex::new(Vec::<u64>::new()))
        .run(|node, msg| match msg.body {
            BroadcastRequest::Topology { .. } => {
                todo!()
            }
            BroadcastRequest::Broadcast { .. } => todo!(),
            BroadcastRequest::Read { msg_id } => {
                let lock = node.state().lock().unwrap();
                node.send_msg(&Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: BroadcastResponse::ReadOk {
                        messages: &lock,
                        in_reply_to: msg_id,
                    },
                });
            }
        })
        .await;
}
