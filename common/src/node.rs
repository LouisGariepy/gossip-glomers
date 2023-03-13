use std::{
    future::Future,
    io::{stdin, stdout, Stdin, Stdout, Write},
    sync::{Arc, Mutex},
};

use rustc_hash::FxHashMap;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::oneshot;

use crate::{
    id::{MessageId, NodeId},
    message::{InitRequest, InitResponse, JsonString, Message, MessageType, Request, Response},
};

pub struct Node<InitialData, State, InboundResponseBody> {
    pub initial_data: InitialData,
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
    pub state: State,
    pub callbacks:
        Mutex<FxHashMap<MessageId, oneshot::Sender<Message<Response<InboundResponseBody>>>>>,
    stdout: Stdout,
}

impl<InitialData, State, InboundResponseBody: Serialize>
    Node<InitialData, State, InboundResponseBody>
{
    pub fn send_msg<OutBoundResponseBody: Serialize>(
        &self,
        msg: &Message<Response<OutBoundResponseBody>>,
    ) {
        writeln!(self.stdout.lock(), "{}", msg.to_json().as_str()).unwrap();
    }

    pub async fn rpc<OutboundRequestBody: Serialize>(
        &self,
        msg_id: MessageId,
        msg: Message<Request<OutboundRequestBody>>,
    ) -> Message<Response<InboundResponseBody>> {
        let (sender, receiver) = oneshot::channel();
        self.callbacks.lock().unwrap().insert(msg_id, sender);
        writeln!(self.stdout.lock(), "{}", msg.to_json().as_str()).unwrap();
        let response = receiver.await.unwrap();
        eprintln!("Received response!");
        response
    }

    pub fn send_json_msg(&self, ser_msg: &JsonString) {
        let msg_str = ser_msg.as_str();
        writeln!(self.stdout.lock(), "{msg_str}").unwrap();
    }
}

pub struct NodeChannel {
    buf: String,
    stdin: Stdin,
    stdout: Stdout,
}

impl NodeChannel {
    pub fn send_msg<Body: Serialize>(&self, msg: &Message<Response<Body>>) {
        writeln!(self.stdout.lock(), "{}", msg.to_json().as_str()).unwrap();
    }

    pub fn receive_msg<Body: DeserializeOwned>(&mut self) -> Message<Request<Body>> {
        self.stdin.read_line(&mut self.buf).unwrap();
        let msg = Message::from_json_str(&self.buf);
        self.buf.clear();
        msg
    }
}
type InboundRequestHandler<InitialData, State, RequestBody, ResponseBody, F> =
    fn(Arc<Node<InitialData, State, ResponseBody>>, Message<Request<RequestBody>>) -> F;

pub struct NodeBuilder<InitialData, State> {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
    initial_data: InitialData,
    state: State,
    channel: NodeChannel,
}

impl NodeBuilder<(), ()> {
    #[must_use]
    pub fn init() -> NodeBuilder<(), ()> {
        let mut channel = NodeChannel {
            buf: String::new(),
            stdin: stdin(),
            stdout: stdout(),
        };

        let msg = channel.receive_msg::<InitRequest>();
        let init_reponse = Message {
            src: msg.dest,
            dest: msg.src,
            body: Response {
                in_reply_to: msg.body.msg_id,
                kind: InitResponse {},
            },
        };
        channel.send_msg(&init_reponse);

        NodeBuilder {
            node_id: msg.body.kind.node_id,
            node_ids: msg.body.kind.node_ids,
            initial_data: (),
            state: (),
            channel,
        }
    }
}

impl<InitialData, State> NodeBuilder<InitialData, State>
where
    InitialData: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    #[must_use]
    pub fn with_initial_data<NewInitialData>(
        mut self,
        init: fn(&mut NodeChannel) -> NewInitialData,
    ) -> NodeBuilder<NewInitialData, State>
    where
        NewInitialData: Send + Sync + 'static,
    {
        NodeBuilder {
            node_id: self.node_id,
            node_ids: self.node_ids,
            initial_data: init(&mut self.channel),
            state: self.state,
            channel: self.channel,
        }
    }

    pub fn with_state<NewState>(self, state: NewState) -> NodeBuilder<InitialData, NewState>
    where
        NewState: Send + Sync + 'static,
    {
        NodeBuilder {
            initial_data: self.initial_data,
            node_id: self.node_id,
            node_ids: self.node_ids,
            state,
            channel: self.channel,
        }
    }

    pub fn run<InboundRequestBody, InboundResponseBody, F>(
        self,
        request_handler: InboundRequestHandler<
            InitialData,
            State,
            InboundRequestBody,
            InboundResponseBody,
            F,
        >,
    ) where
        InboundRequestBody: DeserializeOwned + Send + Sync + 'static,
        InboundResponseBody: std::fmt::Debug + DeserializeOwned + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        let node = Arc::new(Node {
            initial_data: self.initial_data,
            node_id: self.node_id,
            node_ids: self.node_ids,
            state: self.state,
            stdout: self.channel.stdout,
            callbacks: Default::default(),
        });
        let mut lines = self.channel.stdin.lines();
        while let Some(Ok(line)) = lines.next() {
            let node = node.clone();
            let msg =
                Message::<MessageType<InboundRequestBody, InboundResponseBody>>::from_json_str(
                    &line,
                );
            match msg.body {
                MessageType::Request(request) => {
                    let request_msg = Message {
                        src: msg.src,
                        dest: msg.dest,
                        body: request,
                    };
                    tokio::spawn(async move {
                        request_handler(node, request_msg).await;
                    });
                }
                MessageType::Response(response) => {
                    node.callbacks
                        .lock()
                        .unwrap()
                        .remove(&response.in_reply_to)
                        .unwrap()
                        .send(Message {
                            src: msg.src,
                            dest: msg.dest,
                            body: response,
                        })
                        .unwrap();
                }
            }
        }
    }
}
