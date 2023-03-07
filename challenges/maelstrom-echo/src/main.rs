use common::{
    message::{EchoRequest, EchoResponse, Message},
    node::NodeBuilder,
};

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .run::<EchoRequest, EchoResponse>(|_, msg| Message {
            src: msg.dest,
            dest: msg.src,
            body: EchoResponse {
                in_reply_to: msg.body.msg_id,
                echo: msg.body.echo,
            },
        })
        .await;
}
