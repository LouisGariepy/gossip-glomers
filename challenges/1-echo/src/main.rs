use serde::{Deserialize, Serialize};

use common::{
    define_msg_kind,
    message::{Message, Response},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .build::<EchoRequest, EchoResponse, (), ()>()
        .run(|node, msg| async move {
            node.send_response(Message {
                src: msg.dest,
                dest: msg.src,
                body: Response {
                    in_reply_to: msg.body.msg_id,
                    kind: EchoResponse::EchoOk(EchoOk {
                        echo: msg.body.kind.into_inner().echo,
                    }),
                },
            })
        });
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    pub enum EchoRequest {
        Echo { echo: String },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum EchoResponse {
        EchoOk { echo: String },
    }
);
