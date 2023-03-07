use std::sync::Mutex;

use common::{
    message::{BroadcastRequest, BroadcastResponse, Message},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_state(Mutex::new(Vec::<u64>::new()))
        .run::<BroadcastRequest, BroadcastResponse>(|node, msg| {
            let response_body = match msg.body {
                BroadcastRequest::Topology { msg_id, topology } => {
                    todo!()
                }
                BroadcastRequest::Broadcast { msg_id, message } => todo!(),
                BroadcastRequest::Read { msg_id } => BroadcastResponse::ReadOk {
                    messages: &node.state.lock().unwrap(),
                    in_reply_to: msg_id,
                },
            };
            Message {
                src: msg.dest,
                dest: msg.src,
                body: response_body,
            }
        })
        .await;
}
