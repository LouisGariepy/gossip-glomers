use std::{
    fmt::Debug,
    future::Future,
    io::{stdin, stdout, Stdin, Stdout, Write},
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};

use rustc_hash::FxHashMap;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::oneshot;

use crate::{
    id::{MsgId, MsgIdGenerator, NodeId},
    json::Json,
    message::{InitRequest, InitResponse, Message, MessageType, Request, Response},
    utils::SameType,
    HealthyMutex,
};

/// Type that allows RPC tasks to be awoken.
type RpcWaker<IRes> = oneshot::Sender<Message<Response<IRes>>>;

/// Type for the function that handles a [`Node`]'s inbound requests.
type InboundRequestHandler<IReq, IRes, State, F> =
    fn(Arc<Node<IReq, IRes, State>>, Message<Request<IReq>>) -> F;

/// Type for the function that handles a [`SimpleNode`]'s inbound requests.
type SimpleInboundRequestHandler<IReq, State, F> =
    fn(Arc<SimpleNode<IReq, State>>, Message<Request<IReq>>) -> F;

/// A node abstraction. The main main item provided by this module. Nodes can hold
/// state, receive and send both requests and responses.
///
/// In contrast to [`SimpleNode`]s, nodes can send RPC requests.
pub struct Node<IReq, IRes, State = ()> {
    /// This node's ID.
    pub id: NodeId,
    /// The list of all participating node IDs.
    pub network: Vec<NodeId>,
    /// Atomic counter for message IDs.
    pub msg_id_gen: MsgIdGenerator,
    /// This node's state.
    pub state: State,
    /// A map containing [`RpcWaker`]s, identified by the
    /// [`MsgId`] of the corresponding RPC request.
    rpc_wakers: HealthyMutex<FxHashMap<MsgId, RpcWaker<IRes>>>,
    /// Standard output. Allows nodes to send messages.
    stdout: Stdout,
    /// Zero-sized marker to enforce the correct inbound request type.
    phantom_ireq: PhantomData<fn(IReq)>,
}

impl<IReq, IRes, State> Node<IReq, IRes, State>
where
    IReq: DeserializeOwned,
    IRes: Debug + DeserializeOwned,
{
    /// Serializes a request message to JSON and returns it along with it's message id.
    pub fn serialize_response<T: Serialize>(&self, res: Message<Response<T>>) -> Json {
        res.into_json()
    }

    /// Sends a response message.
    pub fn send_response(&self, res: &Json) {
        writeln!(self.stdout.lock(), "{}", res.as_str()).unwrap();
    }

    /// Serializes a request message to JSON and returns it along with it's message id.
    pub fn serialize_request<T: Serialize>(&self, req: Message<Request<T>>) -> Json {
        req.into_json()
    }

    /// Performs a RPC call by sending the serialized request message and registering a waker.
    /// When the node receives the corresponding response, it will send back the response
    /// via the waker and resume the task that sent the request.
    ///
    /// If the RPC call times out, this function return `None`. Otherwise it returns the response.
    ///
    /// # Panics
    /// Panics if the sender half of the RPC waker has been dropped before sending anything.
    pub async fn rpc(&self, msg_id: MsgId, ser_req: Json) -> Option<Message<Response<IRes>>> {
        let (sender, receiver) = oneshot::channel();
        // Insert a new waker
        self.rpc_wakers.lock().insert(msg_id, sender);
        // Send RPC request
        writeln!(self.stdout.lock(), "{}", ser_req.as_str()).unwrap();
        // Wait to receive response
        let result = match tokio::time::timeout(Duration::from_secs(3), receiver).await {
            // Response received
            Ok(msg) => Some(msg.unwrap()),
            // Timeout
            Err(_) => None,
        };
        // Remove the corresponding waker and return the result
        self.rpc_wakers.lock().remove(&msg_id);
        result
    }

    /// Inbound message handler. It deserializes inbound messages and dispatches them.
    ///
    /// This function runs the provided request handler upon receiving a request.
    /// On the other hand, when receiving a response (to a previously made RPC request), it
    /// wakes the RPC task by sending the response over the registered waker.
    ///
    /// # Panics
    /// Panics when receiving a RPC response if the receiver half of the waker has already
    /// been dropped.
    pub fn run<F>(self: Arc<Self>, request_handler: InboundRequestHandler<IReq, IRes, State, F>)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Read lines from STDIN
        let mut lines = stdin().lines();
        while let Some(Ok(line)) = lines.next() {
            // Deserialize line into message
            let msg = Message::<MessageType<IReq, IRes>>::from_json_str(&line);
            // Check if message was a request or a response
            match msg.body {
                // Request are handled by the request handler in a separate task
                MessageType::Request(request) => {
                    tokio::spawn({
                        request_handler(
                            Arc::clone(&self),
                            Message {
                                src: msg.src,
                                dest: msg.dest,
                                body: request,
                            },
                        )
                    });
                }
                // Response are sent to RPC tasks via a waker
                MessageType::Response(response) => {
                    // Get the waker corresponding to the RPC request's id
                    let waker = self.rpc_wakers.lock().remove(&response.in_reply_to);
                    // If this waker still exists (i.e. it hasn't timed
                    // out yet), send the response message over the waker
                    if let Some(waker) = waker {
                        waker
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

/// A simple node abstraction. The main main item provided by this module. Simple nodes can hold
/// state, receive requests and send responses.
///
/// In contrast to [`Node`]s, simple nodes cannot perform RPC requests.
pub struct SimpleNode<IReq, State = ()> {
    /// This node's ID.
    pub id: NodeId,
    /// The list of all participating node IDs.
    pub network: Vec<NodeId>,
    /// This node's state.
    pub state: State,
    /// Standard output. Allows nodes to send messages.
    stdout: Stdout,
    /// Zero-sized marker to enforce the correct inbound request type.
    phantom_ireq: PhantomData<fn(IReq)>,
}

impl<IReq, State> SimpleNode<IReq, State>
where
    IReq: DeserializeOwned,
{
    /// Serializes a request message to JSON.
    pub fn serialize_response<T: Serialize>(&self, res: Message<Response<T>>) -> Json {
        res.into_json()
    }

    /// Sends a response message.
    pub fn send_response(&self, res: &Json) {
        writeln!(self.stdout.lock(), "{}", res.as_str()).unwrap();
    }

    /// Inbound message handler. It deserializes inbound request messages and dispatches
    /// them to the request handler on a separate async task.
    pub fn run<F>(self: Arc<Self>, request_handler: SimpleInboundRequestHandler<IReq, State, F>)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Read lines from STDIN
        let mut lines = stdin().lines();
        while let Some(Ok(line)) = lines.next() {
            // Deserialize line into message
            let request = Message::<Request<IReq>>::from_json_str(&line);
            // Request are handled by the request handler in a separate task
            tokio::spawn(request_handler(Arc::clone(&self), request));
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
    pub fn send_msg<Body: Serialize>(&self, msg: Message<Response<Body>>) {
        writeln!(self.stdout.lock(), "{}", msg.into_json().as_str()).unwrap();
    }

    /// Reads a single line from STDIN and deserializes it from JSON into a message.
    ///
    /// # Panics
    /// Panics if unable to read a line from STDIN.
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
    /// Data that was received by the builder during
    /// the initial message exchange.
    pub initial_data: NodeBuilderData,
    /// A communication channel to STDIN/STDOUT held by
    /// this builder to send and receive initial messages.
    channel: NodeChannel,
    /// A user-defined state.
    state: State,
}

/// Data that was received by the builder during
/// the initial message exchange.
pub struct NodeBuilderData {
    /// The [`Node`] ID received from the
    /// [`crate::message::InitRequest`] message.
    pub id: NodeId,
    /// The list of all participating [`Node`] IDs
    /// received from the [`crate::message::InitRequest`] message.
    pub network: Vec<NodeId>,
}

impl NodeBuilder<()> {
    /// Handles the initial Maelstrom message
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
        channel.send_msg(Message {
            src: init_request.dest,
            dest: init_request.src,
            body: Response {
                in_reply_to: init_request.body.msg_id,
                kind: InitResponse::InitOk {},
            },
        });

        // Create the builder from the information in the init request.
        let init = init_request.body.kind.into_inner();
        Self {
            state: (),
            channel,
            initial_data: NodeBuilderData {
                id: init.node_id,
                network: init.node_ids,
            },
        }
    }

    /// Sets the [`NodeBuilder`]'s state. This state will be inherited by the [`Node`]
    /// this builder will create. This method can be used to handle initial message exchanges
    /// that are required to set the state.
    #[must_use]
    pub fn with_state<State>(
        mut self,
        init: fn(initial_data: &NodeBuilderData, &mut NodeChannel) -> State,
    ) -> NodeBuilder<State> {
        NodeBuilder {
            state: init(&self.initial_data, &mut self.channel),
            channel: self.channel,
            initial_data: self.initial_data,
        }
    }

    /// Sets the [`NodeBuilder`]'s state. This state will be inherited by the [`Node`]
    /// this builder will create. This method can be used to handle initial message exchanges
    /// that are required to set the state.
    #[must_use]
    pub fn with_default_state<State: Default>(self) -> NodeBuilder<State> {
        self.with_state(|_, _| State::default())
    }
}

/// A trait used to build nodes.
pub trait BuildNode<Node> {
    /// Builds an instance out of a [`NodeBuilder`].
    #[must_use]
    fn build<N>(self) -> Arc<N::As>
    where
        N: SameType<As = Node>;
}

/// [`NodeBuilder`]s can build [`Node`]s.
impl<IReq, IRes, State> BuildNode<Node<IReq, IRes, State>> for NodeBuilder<State> {
    fn build<N>(self) -> Arc<Node<IReq, IRes, State>> {
        Arc::new(Node {
            id: self.initial_data.id,
            network: self.initial_data.network,
            msg_id_gen: MsgIdGenerator::default(),
            state: self.state,
            stdout: self.channel.stdout,
            rpc_wakers: HealthyMutex::default(),
            phantom_ireq: PhantomData,
        })
    }
}

/// [`NodeBuilder`]s can build [`SimpleNode`]s.
impl<IReq, State> BuildNode<SimpleNode<IReq, State>> for NodeBuilder<State> {
    fn build<N>(self) -> Arc<SimpleNode<IReq, State>> {
        Arc::new(SimpleNode {
            id: self.initial_data.id,
            network: self.initial_data.network,
            state: self.state,
            stdout: self.channel.stdout,
            phantom_ireq: PhantomData,
        })
    }
}

/// Convenience macro to send RPC requests.
/// This macro takes four parameters:
/// 1. (Optional): A bracket-delimited list of identifiers that should be dropped after
/// the request has been serialized. This is useful for example to drop mutex locks as
/// soon as possible.
/// 2. The node's identifier.
/// 3. The destination of this request. This can be anything that implements <code>[Into]&lt;[`SiteId`](crate::id::SiteId)&gt;</code>
/// 4. The request kind.
#[doc(hidden)]
#[macro_export]
macro_rules! rpc {
    (
        $(
            [$($drop_ident:ident),*],
        )?
        $node:ident,
        $dest:expr,
        $kind:expr
        $(,)?
    ) => {{
        let msg_id = $node.msg_id_gen.next();
        let msg_ser = $node.serialize_request(common::message::Message {
            src: $node.id.into(),
            dest: $dest.into(),
            body: common::message::Request {
                msg_id,
                kind: $kind,
            },
        });
        $(
            // Variables to drop
            $(
                drop( $drop_ident );
            )*
        )?
        $node.rpc(msg_id, msg_ser)
    }};
}

/// Convenience macro to send responses to inbound requests.
/// This macro takes four parameters:
/// 1. (Optional): A bracket-delimited list of identifiers that should be dropped after
/// the response has been serialized. This is useful for example to drop mutex locks as
/// soon as possible.
/// 2. The node's identifier.
/// 3. The identifier of the request to which this response it targeted.
/// 4. The response kind.
#[doc(hidden)]
#[macro_export]
macro_rules! respond {
    (
        $(
            [$($drop_ident:ident),*],
        )?
        $node:ident,
        $request:ident,
        $kind:expr
        $(,)?
    ) => {{
        let msg_ser = $node.serialize_response(common::message::Message {
            src: $request.dest,
            dest: $request.src,
            body: common::message::Response {
                in_reply_to: $request.body.msg_id,
                kind: $kind,
            },
        });
        $(
            // Variables to drop
            $(
                drop( $drop_ident );
            )*
        )?
        $node.send_response(&msg_ser);
    }};
}

#[doc(inline)]
pub use respond;
#[doc(inline)]
pub use rpc;
