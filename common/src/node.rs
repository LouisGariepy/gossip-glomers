use std::{
    io::{stdin, stdout, Stdin, Stdout, Write},
    sync::Arc,
};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    id::NodeId,
    message::MaybeBorrowed,
    message::{InitRequest, InitResponse, Message},
};

pub struct Node<T> {
    pub info: NodeInfo,
    pub state: T,
}

pub struct NodeInfo {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
}

pub struct NodeBuilder {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
    stdin: Stdin,
    stdout: Stdout,
}

impl NodeBuilder {
    #[must_use]
    pub fn init() -> Self {
        let stdin = stdin();
        let stdout = stdout();
        let mut buf = String::new();

        eprintln!("Waiting for init request...");
        let msg = receive_msg::<InitRequest>(&mut buf, &stdin);
        eprintln!("Init request received!");

        eprintln!("Sending init response...");
        let init_reponse = Message {
            src: msg.dest,
            dest: msg.src,
            body: InitResponse {
                in_reply_to: msg.body.msg_id,
            },
        };
        send_msg(&stdout, &init_reponse);
        eprintln!("Init response sent!");

        eprintln!("Node initialization complete!");
        NodeBuilder {
            node_id: msg.body.node_id,
            node_ids: msg.body.node_ids,
            stdin,
            stdout,
        }
    }

    pub fn with_state<T: Send + Sync + 'static>(self, state: T) -> NodeRunBuilder<Node<T>> {
        NodeRunBuilder {
            state: Node {
                info: NodeInfo {
                    node_id: self.node_id,
                    node_ids: self.node_ids,
                },
                state,
            },
            stdin: self.stdin,
            stdout: self.stdout,
        }
    }

    pub async fn run<Req: DeserializeOwned + 'static, Res: MaybeBorrowed + Serialize + 'static>(
        self,
        msg_handler: fn(&NodeInfo, Message<Req>) -> Message<Res::T<'_>>,
    ) {
        NodeRunBuilder {
            state: NodeInfo {
                node_id: self.node_id,
                node_ids: self.node_ids,
            },
            stdin: self.stdin,
            stdout: self.stdout,
        }
        .run(msg_handler)
        .await;
    }
}

pub struct NodeRunBuilder<T: Send + Sync + 'static> {
    state: T,
    stdin: Stdin,
    stdout: Stdout,
}

impl<T: Send + Sync + 'static> NodeRunBuilder<T> {
    pub async fn run<Req: DeserializeOwned + 'static, Res: MaybeBorrowed + Serialize + 'static>(
        self,
        msg_handler: fn(&T, Message<Req>) -> Message<Res::T<'_>>,
    ) {
        let node_state = Arc::new(self.state);
        let mut lines = self.stdin.lines();
        let stdout = Arc::new(self.stdout);
        while let Some(Ok(line)) = lines.next() {
            let stdout = stdout.clone();
            let node_state = node_state.clone();
            let _handle = tokio::spawn(async move {
                let request_msg: Message<Req> = serde_json::from_str(&line).unwrap();
                let response_msg = msg_handler(&node_state, request_msg);
                send_msg(&stdout, &response_msg);
            });
        }
    }
}

fn send_msg<T: Serialize>(stdout: &Stdout, msg: &Message<T>) {
    let response_ser = serde_json::to_string(&msg).unwrap();
    writeln!(stdout.lock(), "{response_ser}").unwrap();
}

fn receive_msg<T: DeserializeOwned>(buf: &mut String, stdin: &Stdin) -> Message<T> {
    stdin.read_line(buf).unwrap();
    serde_json::from_str(buf).unwrap()
}
