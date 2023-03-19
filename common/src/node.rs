use std::{
    future::Future,
    io::{stdin, stdout, Stdin, Stdout, Write},
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use rustc_hash::FxHashMap;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::oneshot;

use crate::{
    id::{MessageId, NodeId},
    json::Json,
    message::{InitRequest, InitResponse, Message, MessageType, Request, Response},
};

type Callback<InboundResponseBody> = oneshot::Sender<Message<Response<InboundResponseBody>>>;
type InboundRequestHandler<State, RequestBody, ResponseBody, F> =
    fn(Arc<Node<State, RequestBody, ResponseBody>>, Message<Request<RequestBody>>) -> F;

pub struct Node<State, InboundRequestBody, InboundResponseBody> {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
    pub state: State,
    pub callbacks: Mutex<FxHashMap<MessageId, Callback<InboundResponseBody>>>,
    stdout: Stdout,
    marker: PhantomData<InboundRequestBody>,
}

impl<State, InboundRequestBody, InboundResponseBody>
    Node<State, InboundRequestBody, InboundResponseBody>
where
    InboundRequestBody: DeserializeOwned + Send + Sync + 'static,
    InboundResponseBody: std::fmt::Debug + DeserializeOwned + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    pub fn send_msg(&self, msg: impl Into<Json>) {
        writeln!(self.stdout.lock(), "{}", Into::<Json>::into(msg).as_str()).unwrap();
    }

    pub async fn rpc(
        &self,
        msg_id: MessageId,
        msg: &Json,
    ) -> Option<Message<Response<InboundResponseBody>>> {
        let (sender, receiver) = oneshot::channel();
        // Insert a new callback
        self.callbacks.lock().unwrap().insert(msg_id, sender);
        // Send RPC request
        writeln!(self.stdout.lock(), "{}", msg.as_str()).unwrap();
        // Wait to receive response
        let result = match tokio::time::timeout(Duration::from_secs(1), receiver).await {
            // Response received
            Ok(msg) => Some(msg.unwrap()),
            // Timeout
            Err(_) => None,
        };
        // Remove callback and return result
        self.callbacks.lock().unwrap().remove(&msg_id);
        result
    }

    pub fn run<F>(
        self: Arc<Self>,
        request_handler: InboundRequestHandler<State, InboundRequestBody, InboundResponseBody, F>,
    ) where
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        let stdin = stdin();
        let mut lines = stdin.lines();
        while let Some(Ok(line)) = lines.next() {
            let node = self.clone();
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
                    let callback = node.callbacks.lock().unwrap().remove(&response.in_reply_to);
                    if let Some(callback) = callback {
                        callback
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

pub struct NodeBuilder<State> {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
    state: State,
    channel: NodeChannel,
}

impl NodeBuilder<()> {
    #[must_use]
    pub fn init() -> Self {
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
                kind: InitResponse::InitOk {},
            },
        };
        channel.send_msg(&init_reponse);
        let request_body = msg.body.kind.into_inner();
        Self {
            node_id: request_body.node_id,
            node_ids: request_body.node_ids,
            state: (),
            channel,
        }
    }

    #[must_use]
    pub fn with_state<State>(mut self, init: fn(&mut NodeChannel) -> State) -> NodeBuilder<State>
    where
        State: Send + Sync + 'static,
    {
        NodeBuilder {
            node_id: self.node_id,
            node_ids: self.node_ids,
            state: init(&mut self.channel),
            channel: self.channel,
        }
    }
}

impl<State> NodeBuilder<State> {
    #[must_use]
    pub fn build<InboundRequestBody, InboundResponseBody>(
        self,
    ) -> Arc<Node<State, InboundRequestBody, InboundResponseBody>> {
        Arc::new(Node {
            node_id: self.node_id,
            node_ids: self.node_ids,
            state: self.state,
            stdout: self.channel.stdout,
            callbacks: Default::default(),
            marker: Default::default(),
        })
    }
}
