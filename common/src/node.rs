use std::{
    future::Future,
    io::{stdin, stdout, Stdin, Stdout, Write},
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use rustc_hash::FxHashMap;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::oneshot;

use crate::{
    id::{MessageId, NodeId, SiteId},
    message::{InitRequest, InitResponse, Message, MessageType, Request, Response},
    Json,
};

/// Type that allows RPC tasks to be awoken.
type RpcCallback<IRes> = oneshot::Sender<Message<Response<IRes>>>;

/// Type for the function that handles a [`Node`]'s inbound requests.
type InboundRequestHandler<State, IReq, ORes, OReq, IRes, F> =
    fn(Arc<Node<State, IReq, ORes, OReq, IRes>>, Message<Request<IReq>>) -> F;

/// A node abstraction. The main main item provided by this module. Nodes can hold
/// state, receive and send messages
pub struct Node<State, IReq, ORes, OReq, IRes> {
    /// This [`Node`]'s ID.
    pub node_id: NodeId,
    /// The list of all participating [`Node`] IDs.
    pub node_ids: Vec<NodeId>,
    /// This [`Node`]'s state.
    pub state: State,
    /// A map containing [`RpcCallback`]s, identified by the
    /// [`MessageId`] of the corresponding RPC request.
    pub rpc_callbacks: Mutex<FxHashMap<MessageId, RpcCallback<IRes>>>,
    /// Standard output. Allows nodes to send messages.
    stdout: Stdout,
    /// Atomic counter for message IDs.
    next_msg_id: AtomicU64,
    /// Zero-sized marker used to make the generic bounds more ergonomic for users.
    phantom_ireq: PhantomData<fn(IReq)>,
    /// Zero-sized marker to enforce the correct outbound response type.
    phantom_ores: PhantomData<fn() -> ORes>,
    /// Zero-sized marker to enforce the correct outbound request type.
    phantom_oreq: PhantomData<fn() -> OReq>,
}

impl<State, IReq, ORes, OReq, IRes> Node<State, IReq, ORes, OReq, IRes>
where
    IReq: DeserializeOwned,
    ORes: Serialize,
    OReq: Serialize,
    IRes: std::fmt::Debug + DeserializeOwned + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    /// Atomically increment the node's message counter and return previous value.
    pub fn next_msg_id(&self) -> MessageId {
        MessageId(self.next_msg_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Serializes a request message to JSON and returns it along with it's message id.
    pub fn serialize(&self, req: Message<Request<OReq>>) -> (MessageId, Json) {
        (req.body.msg_id, req.into_json())
    }

    /// Convenience function to create a request message.
    pub fn new_request<T: Into<SiteId>>(&self, dest: T, kind: OReq) -> Message<Request<OReq>> {
        Message {
            src: self.node_id.into(),
            dest: dest.into(),
            body: Request {
                msg_id: self.next_msg_id(),
                kind,
            },
        }
    }

    /// Convenience function to create a request message and serialize it to
    /// JSON, returning the serialized value along with it's message id.
    pub fn serialize_new_request<T: Into<SiteId>>(&self, dest: T, kind: OReq) -> (MessageId, Json) {
        self.serialize(Message {
            src: self.node_id.into(),
            dest: dest.into(),
            body: Request {
                msg_id: self.next_msg_id(),
                kind,
            },
        })
    }

    /// Sends a response message.
    pub fn send_response(&self, msg: Message<Response<ORes>>) {
        let msg_json = msg.into_json();
        writeln!(self.stdout.lock(), "{}", msg_json.as_str()).unwrap();
    }

    /// Performs a RPC call by sending the request message and registering a callback.
    /// When the node receives the corresponding response, it will send back the response
    /// via the callback.
    ///
    /// If the RPC call times out, this function return `None`. Otherwise it returns the response.
    pub async fn rpc(&self, msg: Message<Request<OReq>>) -> Option<Message<Response<IRes>>> {
        self.rpc_json(msg.body.msg_id, msg.into_json()).await
    }

    /// Performs a RPC call by sending the serialized request message and registering a callback.
    /// When the node receives the corresponding response, it will send back the response
    /// via the callback.
    ///
    /// If the RPC call times out, this function return `None`. Otherwise it returns the response.
    pub async fn rpc_json(
        &self,
        msg_id: MessageId,
        ser_req: Json,
    ) -> Option<Message<Response<IRes>>> {
        let (sender, receiver) = oneshot::channel();
        // Insert a new callback
        self.rpc_callbacks.lock().unwrap().insert(msg_id, sender);
        // Send RPC request
        writeln!(self.stdout.lock(), "{}", ser_req.as_str()).unwrap();
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
        request_handler: InboundRequestHandler<State, IReq, ORes, OReq, IRes, F>,
    ) where
        F: Future<Output = ()> + Send + 'static,
    {
        // Read lines from STDIN
        let mut lines = stdin().lines();
        while let Some(Ok(line)) = lines.next() {
            eprintln!("{line}");
            // Deserialize line into message
            let msg = Message::<MessageType<IReq, IRes>>::from_json_str(&line);
            // Check if message was a request or a response
            match msg.body {
                // Request are handled by the request handler in a separate task
                MessageType::Request(request) => {
                    tokio::spawn({
                        let node = Arc::clone(&self);
                        request_handler(
                            node,
                            Message {
                                src: msg.src,
                                dest: msg.dest,
                                body: request,
                            },
                        )
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

/// A convenience macro to handle sending RPC requests that contain non-`Send` data.
#[macro_export]
macro_rules! rpc {
    ($node:ident, $dest:expr, $kind:expr $(,)?) => {{
        let (msg_id, req_json) = $node.serialize_new_request($dest, $kind);
        $node.rpc_json(msg_id, req_json)
    }};
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
    pub fn with_state<State>(
        mut self,
        init: fn(node_id: NodeId, &mut NodeChannel) -> State,
    ) -> NodeBuilder<State>
    where
        State: Send + Sync + 'static,
    {
        NodeBuilder {
            node_ids: self.node_ids,
            state: init(self.node_id, &mut self.channel),
            node_id: self.node_id,
            channel: self.channel,
        }
    }
}

impl<State> NodeBuilder<State> {
    /// Consumes this builder and creates a [`Node`].
    /// The [`Node`] will inherit this builder's state.
    #[must_use]
    pub fn build<IReq, ORes, OReq, IRes>(self) -> Arc<Node<State, IReq, ORes, OReq, IRes>> {
        Arc::new(Node {
            node_id: self.node_id,
            node_ids: self.node_ids,
            state: self.state,
            stdout: self.channel.stdout,
            rpc_callbacks: Mutex::default(),
            phantom_ireq: PhantomData,
            phantom_ores: PhantomData,
            phantom_oreq: PhantomData,
            next_msg_id: AtomicU64::default(),
        })
    }
}
