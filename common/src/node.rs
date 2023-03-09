use std::{
    io::{stdin, stdout, Lines, Stdin, StdinLock, Stdout, Write},
    sync::Arc,
};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    id::NodeId,
    message::{InitRequest, InitResponse, JsonString, Message},
};

pub struct Node<InitialData, State> {
    pub initial_data: InitialData,
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
    pub state: State,
    stdout: Stdout,
}

impl<InitialData, State> Node<InitialData, State> {
    pub fn send_msg<Body: Serialize>(&self, msg: &Message<Body>) {
        let response_ser = serde_json::to_string(&msg).unwrap();
        writeln!(self.stdout.lock(), "{response_ser}").unwrap();
    }

    pub fn send_json_msg(&self, ser_msg: &JsonString) {
        let msg_str = ser_msg.as_str();
        writeln!(self.stdout.lock(), "{msg_str}").unwrap();
    }
}

pub struct NodeChannel<'a> {
    stdin_lines: Lines<StdinLock<'a>>,
    stdout: Stdout,
}

impl<'a> NodeChannel<'a> {
    pub fn send_msg<Body: Serialize>(&self, msg: &Message<Body>) {
        let response_ser = serde_json::to_string(&msg).unwrap();
        writeln!(self.stdout.lock(), "{response_ser}").unwrap();
    }

    pub fn receive_msg<Body: DeserializeOwned>(&mut self) -> Message<Body> {
        let line = self.stdin_lines.next().unwrap().unwrap();
        serde_json::from_str(&line).unwrap()
    }
}

pub struct NodeBuilder<InitialData> {
    initial_data: InitialData,
    node_id: NodeId,
    node_ids: Vec<NodeId>,
    stdin: Stdin,
    stdout: Stdout,
}

impl NodeBuilder<()> {
    #[must_use]
    pub fn init() -> Self {
        NodeBuilder::init_with(|_| ())
    }
}

impl<InitialData> NodeBuilder<InitialData>
where
    InitialData: Send + Sync + 'static,
{
    #[must_use]
    pub fn init_with(init: fn(&mut NodeChannel) -> InitialData) -> Self {
        let mut channel = NodeChannel {
            stdin_lines: stdin().lines(),
            stdout: stdout(),
        };

        let msg = channel.receive_msg::<InitRequest>();
        let init_reponse = Message {
            src: msg.dest,
            dest: msg.src,
            body: InitResponse {
                in_reply_to: msg.body.msg_id,
            },
        };
        channel.send_msg(&init_reponse);

        let init_state = init(&mut channel);

        NodeBuilder {
            initial_data: init_state,
            node_id: msg.body.node_id,
            node_ids: msg.body.node_ids,
            stdin: stdin(),
            stdout: channel.stdout,
        }
    }

    pub fn with_state<State>(self, state: State) -> NodeRunner<InitialData, State>
    where
        State: Send + Sync + 'static,
    {
        NodeRunner {
            node: Node {
                initial_data: self.initial_data,
                node_id: self.node_id,
                node_ids: self.node_ids,
                state,
                stdout: self.stdout,
            },
            stdin: self.stdin,
        }
    }

    pub async fn run<Req>(self, msg_handler: fn(Arc<Node<InitialData, ()>>, Message<Req>))
    where
        Req: DeserializeOwned + 'static,
    {
        NodeRunner {
            node: Node {
                initial_data: self.initial_data,
                node_id: self.node_id,
                node_ids: self.node_ids,
                state: (),
                stdout: self.stdout,
            },
            stdin: self.stdin,
        }
        .run(msg_handler);
    }
}

pub struct NodeRunner<InitData, State>
where
    InitData: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    stdin: Stdin,
    node: Node<InitData, State>,
}

impl<InitData, State> NodeRunner<InitData, State>
where
    InitData: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    pub fn run<Req: DeserializeOwned + 'static>(
        self,
        msg_handler: fn(Arc<Node<InitData, State>>, Message<Req>),
    ) {
        let node = Arc::new(self.node);
        let mut lines = self.stdin.lines();
        while let Some(Ok(line)) = lines.next() {
            let node_asd = node.clone();
            let _handle = tokio::spawn(async move {
                let request_msg: Message<Req> = serde_json::from_str(&line).unwrap();
                msg_handler(node_asd, request_msg);
            });
        }
    }
}
