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

/// Type that allows RPC tasks to be awoken.
type RpcCallback<InboundResponseBody> = oneshot::Sender<Message<Response<InboundResponseBody>>>;

/// Type for the function that handles a [`Node`]'s inbound requests.
type InboundRequestHandler<State, RequestBody, ResponseBody, F> =
    fn(Arc<Node<State, RequestBody, ResponseBody>>, Message<Request<RequestBody>>) -> F;

/// A node abstraction. The main main item provided by this module. Nodes can hold
/// state, receive and send messages
pub struct Node<State, InboundRequestBody, InboundResponseBody> {
    /// This [`Node`]'s ID.
    pub node_id: NodeId,
    /// The list of all participating [`Node`] IDs.
    pub node_ids: Vec<NodeId>,
    /// This [`Node`]'s state.
    pub state: State,
    /// A map containing [`RpcCallback`]s, identified by the
    /// [`MessageId`] of the corresponding RPC request.
    pub rpc_callbacks: Mutex<FxHashMap<MessageId, RpcCallback<InboundResponseBody>>>,
    /// Standard output. Allows nodes to send messages.
    stdout: Stdout,
    /// Zero-sized marker used to make the generic bounds more ergonomic for users.
    marker: PhantomData<InboundRequestBody>,
}

impl<State, InboundRequestBody, InboundResponseBody>
    Node<State, InboundRequestBody, InboundResponseBody>
where
    InboundRequestBody: DeserializeOwned + Send + Sync + 'static,
    InboundResponseBody: std::fmt::Debug + DeserializeOwned + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    /// Writes a JSON message on a single line over STDOUT.
    pub fn send_msg(&self, msg: &Json) {
        writeln!(self.stdout.lock(), "{}", msg.as_str()).unwrap();
    }

    pub async fn rpc(
        &self,
        msg_id: MessageId,
        msg: &Json,
    ) -> Option<Message<Response<InboundResponseBody>>> {
        let (sender, receiver) = oneshot::channel();
        // Insert a new callback
        self.rpc_callbacks.lock().unwrap().insert(msg_id, sender);
        // Send RPC request
        writeln!(self.stdout.lock(), "{}", msg.as_str()).unwrap();
        // Wait to receive response
        let result = match tokio::time::timeout(Duration::from_secs(3), receiver).await {
            // Response received
            Ok(msg) => Some(msg.unwrap()),
            // Timeout
            Err(_) => None,
        };
        // Remove callback and return result
        self.rpc_callbacks.lock().unwrap().remove(&msg_id);
        result
    }

    /// Inbound message handler. It deserializes inbound messages and dispatches them.
    ///
    /// This function runs the provided request handler upon receiving a request.
    /// On the other hand, when receiving a response (to a previously made RPC request), it
    /// wakes the RPC task by sending the response over the registered callback.
    pub fn run<F>(
        self: Arc<Self>,
        request_handler: InboundRequestHandler<State, InboundRequestBody, InboundResponseBody, F>,
    ) where
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        // Read lines from STDIN
        let mut lines = stdin().lines();
        while let Some(Ok(line)) = lines.next() {
            eprintln!("{line}");
            // Deserialize line into message
            let msg =
                Message::<MessageType<InboundRequestBody, InboundResponseBody>>::from_json_str(
                    &line,
                );
            // Check if message was a request or a response
            match msg.body {
                // Request are handled by the request handler in a separate task
                MessageType::Request(request) => {
                    let node = self.clone();
                    tokio::spawn(async move {
                        request_handler(
                            node,
                            Message {
                                src: msg.src,
                                dest: msg.dest,
                                body: request,
                            },
                        )
                        .await;
                    });
                }
                // Response are sent to RPC tasks via a callback
                MessageType::Response(response) => {
                    // Get the callback corresponding to the RPC request's id
                    let callback = self
                        .rpc_callbacks
                        .lock()
                        .unwrap()
                        .remove(&response.in_reply_to);
                    // If this callback still exists (i.e. it hasn't timed
                    // out yet), send the response message over the callback
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

/// A simple abstraction that can read
/// and write messages over STDIN/STDOUT.
pub struct NodeChannel {
    /// An internal buffer to read lines from STDIN.
    buf: String,
    /// Standard input.
    stdin: Stdin,
    /// Standard output.
    stdout: Stdout,
}

impl NodeChannel {
    /// Serializes a message to JSON and writes on a single line over STDOUT
    pub fn send_msg<Body: Serialize>(&self, msg: &Message<Response<Body>>) {
        writeln!(self.stdout.lock(), "{}", msg.to_json().as_str()).unwrap();
    }

    /// Reads a single line from STDIN and deserializes it from JSON into a message.
    pub fn receive_msg<Body: DeserializeOwned>(&mut self) -> Message<Request<Body>> {
        self.stdin.read_line(&mut self.buf).unwrap();
        let msg = Message::from_json_str(&self.buf);
        self.buf.clear();
        msg
    }
}

/// A convenient builder for [`Node`]s. Notably, the [`NodeBuilder`]
/// is responsible of handling all initial message exchanges and to
/// set the node's state.
pub struct NodeBuilder<State> {
    /// A communication channel to STDIN/STDOUT held by
    /// this builder to send and receive initial messages.
    channel: NodeChannel,
    /// The [`Node`] ID received from the
    /// [`crate::message::InitRequest`] message.
    node_id: NodeId,
    /// The list of all participating [`Node`] IDs
    /// received from the [`crate::message::InitRequest`] message.
    node_ids: Vec<NodeId>,
    /// A user-defined state.
    state: State,
}

impl NodeBuilder<()> {
    /// Handles the [`crate::message::InitRequest`] message
    /// exchange and creates a new instance of [`NodeBuilder`].
    #[must_use]
    pub fn init() -> Self {
        // Initialize the builder's communication channel.
        let mut channel = NodeChannel {
            buf: String::new(),
            stdin: stdin(),
            stdout: stdout(),
        };

        // Receive init request and send response
        let init_request = channel.receive_msg::<InitRequest>();
        let init_reponse = Message {
            src: init_request.dest,
            dest: init_request.src,
            body: Response {
                in_reply_to: init_request.body.msg_id,
                kind: InitResponse::InitOk {},
            },
        };
        channel.send_msg(&init_reponse);

        // Create the builder from the information in the init request.
        let init = init_request.body.kind.into_inner();
        Self {
            node_id: init.node_id,
            node_ids: init.node_ids,
            state: (),
            channel,
        }
    }

    /// Sets the [`NodeBuilder`]'s state. This state will be inherited by the [`Node`]
    /// this builder will create. This method can be used to handle initial message exchanges
    /// that are required to set the state.
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
    /// Consumes this builder and creates a [`Node`].
    /// The [`Node`] will inherit this builder's state.
    #[must_use]
    pub fn build<InboundRequestBody, InboundResponseBody>(
        self,
    ) -> Arc<Node<State, InboundRequestBody, InboundResponseBody>> {
        Arc::new(Node {
            node_id: self.node_id,
            node_ids: self.node_ids,
            state: self.state,
            stdout: self.channel.stdout,
            rpc_callbacks: Default::default(),
            marker: Default::default(),
        })
    }
}