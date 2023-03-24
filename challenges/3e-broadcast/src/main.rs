use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use common::{
    define_msg_kind,
    id::NodeId,
    message::{Message, Request, Response, TopologyRequest, TopologyResponse},
    node::{NodeBuilder, NodeChannel},
    rpc, FxHashMap, FxIndexSet, IndexSetSlice,
};
use serde::{Deserialize, Serialize};

type Node = common::node::Node<
    NodeState,
    InboundRequest,
    OutboundResponse<'static>,
    OutboundRequest<'static>,
    InboundResponse,
>;

/// Interval between healing broadcasts
const HEALING_INTERVAL: Duration = Duration::from_millis(1000);
/// Interval between regular broadcasts
const BROADCAST_INTERVAL: Duration = Duration::from_millis(200);

/// The state held by this node
#[derive(Debug, Default)]
struct NodeState {
    /// The set of all messages held by this node.
    messages: Mutex<FxIndexSet<u64>>,
    /// The direct neighbours of this node.
    neighbours: Vec<NodeId>,
    /// The starting timestamp of the next broadcast. All messages
    /// with greater or equal timestamps will be broadcasted
    /// during the next regular broadcast.
    broadcast_timestamp: AtomicUsize,
    /// Map recording the timeout timestamp of each neighbour.
    /// All messages with greater or equal timestamps will be
    /// broadcasted to the corresponding neighbour during the
    /// next healing broadcast.
    timeout_timestamps: Mutex<FxHashMap<NodeId, Option<usize>>>,
}

#[tokio::main]
async fn main() {
    // Build node
    let node = NodeBuilder::init().with_state(initialize_node).build();
    // Spawn background healing task
    healing_task(Arc::clone(&node));
    // Spawn background batch broadcasting task
    broadcasting_task(Arc::clone(&node));
    // Run request handler
    node.run(request_handler);
}

fn initialize_node(node_id: NodeId, channel: &mut NodeChannel) -> NodeState {
    // Receive and respond to initial topology message
    let topology_request = channel.receive_msg::<TopologyRequest>();
    channel.send_msg(&Message {
        src: topology_request.dest,
        dest: topology_request.src,
        body: Response {
            in_reply_to: topology_request.body.msg_id,
            kind: TopologyResponse::TopologyOk {},
        },
    });

    // Obtain node neighbours from topology
    let mut topology = topology_request.body.kind.into_inner().topology;
    let neighbours = topology
        .remove(&node_id)
        .expect("the topology should include this node's neighbours");

    // Create empty map for failed broadcasts
    let failed_broadcasts = Mutex::new(
        neighbours
            .iter()
            .map(|&neighbour| (neighbour, None))
            .collect(),
    );
    NodeState {
        messages: Mutex::default(),
        neighbours,
        broadcast_timestamp: AtomicUsize::default(),
        timeout_timestamps: failed_broadcasts,
    }
}

fn healing_task(node: Arc<Node>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(HEALING_INTERVAL);
        loop {
            // Execute at every interval...
            interval.tick().await;
            // Filter to keep neighbours who have a timeout timestamp
            let suceptible_neighbours = node
                .state
                .timeout_timestamps
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(neighbour_id, timeout_timestamp)| {
                    timeout_timestamp.map(|timeout_timestamp| (*neighbour_id, timeout_timestamp))
                })
                .collect::<Vec<_>>();

            // Send outstanding messages to each susceptible neighbour
            for (neighbour_id, timeout_timestamp) in suceptible_neighbours {
                let node = Arc::clone(&node);
                tokio::spawn(async move {
                    // Send broadcast request and await response
                    let response = rpc!(
                        node,
                        neighbour_id,
                        OutboundRequest::BroadcastMany {
                            messages: &node.state.messages.lock().unwrap()[timeout_timestamp..]
                        }
                    )
                    .await;

                    // If the response is OK, remove the timeout timestamp
                    // This signifies that this neighbour has now received all
                    // outstanding messages
                    if let Some(response) = response {
                        if let InboundResponse::BroadcastManyOk {} = response.body.kind {
                            node.state
                                .timeout_timestamps
                                .lock()
                                .unwrap()
                                .get_mut(&neighbour_id)
                                .unwrap()
                                .take();
                        }
                    }
                });
            }
        }
    });
}

fn broadcasting_task(node: Arc<Node>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(BROADCAST_INTERVAL);
        loop {
            // Execute at every interval...
            interval.tick().await;

            let (broadcast_timestamp, messages) = {
                let messages_guard = &node.state.messages.lock().unwrap();

                // If there were no new messages since last broadcast
                // then skip this broadcast
                let broadcast_timestamp = node
                    .state
                    .broadcast_timestamp
                    .swap(messages_guard.len(), Ordering::SeqCst);
                if broadcast_timestamp == messages_guard.len() {
                    continue;
                }

                // Get messages since last broadcast message
                // These messages are serialized right away to
                // avoid holding the message lock for too long
                let messages = node
                    .state
                    .neighbours
                    .iter()
                    .copied()
                    .map(|neighbour_id| {
                        let msg_ser = node.serialize_new_request(
                            neighbour_id,
                            OutboundRequest::BroadcastMany {
                                messages: &messages_guard[broadcast_timestamp..],
                            },
                        );
                        (neighbour_id, msg_ser.0, msg_ser.1)
                    })
                    .collect::<Vec<_>>();
                (broadcast_timestamp, messages)
            };

            // Send new message batch to each susceptible neighbour
            for (neighbour_id, msg_id, msg_ser) in messages {
                tokio::spawn({
                    let node = Arc::clone(&node);
                    async move {
                        // Send RPC request and get response
                        let response = node.rpc_json(msg_id, msg_ser).await;

                        // If the RPC request timed out, record the timestamp
                        if response.is_none() {
                            node.state
                                .timeout_timestamps
                                .lock()
                                .unwrap()
                                .get_mut(&neighbour_id)
                                .unwrap()
                                .get_or_insert(broadcast_timestamp);
                        }
                    }
                });
            }
        }
    });
}

async fn request_handler(node: Arc<Node>, msg: Message<Request<InboundRequest>>) {
    match msg.body.kind {
        InboundRequest::Read {} => {
            // Send this node's recorded messages
            node.send_response(Message {
                src: msg.dest,
                dest: msg.src,
                body: Response {
                    in_reply_to: msg.body.msg_id,
                    kind: OutboundResponse::ReadOk {
                        messages: node.state.messages.lock().unwrap(),
                    },
                },
            });
        }
        InboundRequest::Broadcast { message } => {
            // Insert the new message in the node's message set
            node.state.messages.lock().unwrap().insert(message);
            // Send OK response
            node.send_response(Message {
                src: msg.dest,
                dest: msg.src,
                body: Response {
                    in_reply_to: msg.body.msg_id,
                    kind: OutboundResponse::BroadcastOk {},
                },
            });
        }
        InboundRequest::BroadcastMany {
            messages: healing_messages,
        } => {
            // Insert the new messages in the node's message set
            node.state.messages.lock().unwrap().extend(healing_messages);
            // Send OK response
            node.send_response(Message {
                src: msg.dest,
                dest: msg.src,
                body: Response {
                    in_reply_to: msg.body.msg_id,
                    kind: OutboundResponse::BroadcastManyOk {},
                },
            });
        }
    }
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    pub enum InboundRequest {
        Read {},
        Broadcast { message: u64 },
        BroadcastMany { messages: FxIndexSet<u64> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum OutboundResponse<'a> {
        ReadOk {
            #[serde(serialize_with = "common::serialize_guard")]
            messages: MutexGuard<'a, FxIndexSet<u64>>,
        },
        BroadcastOk {},
        BroadcastManyOk {},
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum OutboundRequest<'a> {
        Read {},
        BroadcastMany { messages: &'a IndexSetSlice<u64> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum InboundResponse {
        ReadOk { messages: FxIndexSet<u64> },
        BroadcastManyOk {},
    }
);
