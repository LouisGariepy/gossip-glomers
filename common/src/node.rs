use std::{
    io::{stdin, stdout, Stdin, Stdout, Write},
    sync::Arc,
};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    id::NodeId,
    message::{InitRequest, InitResponse, Message},
};

struct InnerNode<T> {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
    state: T,
}

pub struct Node<'a, T> {
    inner_node: &'a InnerNode<T>,
    stdout: &'a Stdout,
}

impl<'a, T> Node<'a, T> {
    pub fn send_msg<B: Serialize>(&self, msg: &Message<B>) {
        let response_ser = serde_json::to_string(&msg).unwrap();
        writeln!(self.stdout.lock(), "{response_ser}").unwrap();
    }

    pub fn node_id(&self) -> NodeId {
        self.inner_node.node_id
    }
    pub fn node_ids(&self) -> &[NodeId] {
        &self.inner_node.node_ids
    }

    pub fn state(&self) -> &T {
        &self.inner_node.state
    }
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

    pub fn with_state<T: Send + Sync + 'static>(self, state: T) -> NodeRunBuilder<T> {
        NodeRunBuilder {
            data: InnerNode {
                node_id: self.node_id,
                node_ids: self.node_ids,
                state,
            },
            stdin: self.stdin,
            stdout: self.stdout,
        }
    }

    pub async fn run<Req: DeserializeOwned + 'static>(
        self,
        msg_handler: fn(&Node<()>, Message<Req>),
    ) {
        NodeRunBuilder {
            data: InnerNode {
                node_id: self.node_id,
                node_ids: self.node_ids,
                state: (),
            },
            stdin: self.stdin,
            stdout: self.stdout,
        }
        .run(msg_handler)
        .await;
    }
}

pub struct NodeRunBuilder<T: Send + Sync + 'static> {
    data: InnerNode<T>,
    stdin: Stdin,
    stdout: Stdout,
}

impl<T: Send + Sync + 'static> NodeRunBuilder<T> {
    pub async fn run<Req: DeserializeOwned + 'static>(
        self,
        msg_handler: fn(&Node<T>, Message<Req>),
    ) {
        let node_state = Arc::new(self.data);
        let stdout = Arc::new(self.stdout);
        let mut lines = self.stdin.lines();
        while let Some(Ok(line)) = lines.next() {
            let stdout = stdout.clone();
            let inner_node = node_state.clone();
            let _handle = tokio::spawn(async move {
                let node = Node {
                    inner_node: &inner_node,
                    stdout: &stdout,
                };
                let request_msg: Message<Req> = serde_json::from_str(&line).unwrap();
                msg_handler(&node, request_msg);
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
