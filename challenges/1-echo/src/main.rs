use common::{
    message::{EchoOk, EchoRequest, EchoResponse, Message, Response},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .build::<EchoRequest, ()>()
        .run(|node, msg| async move {
            node.send_msg(&Message {
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
