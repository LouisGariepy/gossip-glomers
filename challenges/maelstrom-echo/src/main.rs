use common::{
    message::{EchoRequest, EchoResponse, Message, Response},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init().run::<EchoRequest, EchoResponse, _>(|node, msg| async move {
        node.send_msg(&Message {
            src: msg.dest,
            dest: msg.src,
            body: Response {
                in_reply_to: msg.body.msg_id,
                kind: EchoResponse {
                    echo: msg.body.kind.echo,
                },
            },
        })
    });
}
