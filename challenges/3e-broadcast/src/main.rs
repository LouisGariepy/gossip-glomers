use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use common::{
    define_msg_kind, respond, rpc, FxHashMap, FxIndexSet, HealthyMutex, IndexSetSlice, Message,
    NodeBuilder, NodeChannel, NodeId, Request, Response, TopologyRequest, TopologyResponse,
};
use serde::{Deserialize, Serialize};

type Node = common::Node<
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
    messages: HealthyMutex<FxIndexSet<u64>>,
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
    timeout_timestamps: HealthyMutex<FxHashMap<NodeId, Option<usize>>>,
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
    channel.send_msg(Message {
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
    let failed_broadcasts = HealthyMutex::new(
        neighbours
            .iter()
            .map(|&neighbour| (neighbour, None))
            .collect(),
    );
    NodeState {
        messages: HealthyMutex::default(),
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
                .iter()
                .filter_map(|(neighbour, timeout_timestamp)| {
                    timeout_timestamp.map(|timeout_timestamp| (*neighbour, timeout_timestamp))
                })
                .collect::<Vec<_>>();

            // Send outstanding messages to each susceptible neighbour
            for (neighbour, timeout_timestamp) in suceptible_neighbours {
                let node = Arc::clone(&node);
                tokio::spawn(async move {
                    // Send broadcast request and await response
                    let response = rpc!(
                        node,
                        neighbour,
                        OutboundRequest::BroadcastMany {
                            messages: &node.state.messages.lock()[timeout_timestamp..]
                        }
                    )
                    .await;

                    // If the response is OK, remove the timeout timestamp
                    // This signifies that this neighbour has now received all
                    // outstanding messages
                    if response.is_some() {
                        node.state
                            .timeout_timestamps
                            .lock()
                            .get_mut(&neighbour)
                            .unwrap()
                            .take();
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
                let messages_guard = &node.state.messages.lock();

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
                    .map(|neighbour| {
                        let msg_id = node.next_msg_id();
                        let msg_ser = node.serialize_request(Message {
                            src: node.node_id.into(),
                            dest: neighbour.into(),
                            body: Request {
                                msg_id,
                                kind: OutboundRequest::BroadcastMany {
                                    messages: &messages_guard[broadcast_timestamp..],
                                },
                            },
                        });
                        (neighbour, msg_id, msg_ser)
                    })
                    .collect::<Vec<_>>();
                (broadcast_timestamp, messages)
            };

            // Send new message batch to each susceptible neighbour
            for (neighbour, msg_id, msg_ser) in messages {
                tokio::spawn({
                    let node = Arc::clone(&node);
                    async move {
                        // Send RPC request and get response
                        let response = node.rpc(msg_id, msg_ser).await;

                        // If the RPC request timed out, record the timestamp
                        if response.is_none() {
                            node.state
                                .timeout_timestamps
                                .lock()
                                .get_mut(&neighbour)
                                .unwrap()
                                .get_or_insert(broadcast_timestamp);
                        }
                    }
                });
            }
        }
    });
}

#[allow(clippy::unused_async)]
async fn request_handler(node: Arc<Node>, request: Message<Request<InboundRequest>>) {
    match request.body.kind {
        InboundRequest::Read {} => {
            // Send this node's recorded messages
            respond!(
                node,
                request,
                OutboundResponse::ReadOk {
                    messages: &node.state.messages.lock(),
                }
            );
        }
        InboundRequest::Broadcast { message } => {
            // Insert the new message in the node's message set
            node.state.messages.lock().insert(message);
            // Send OK response
            respond!(node, request, OutboundResponse::BroadcastOk {});
        }
        InboundRequest::BroadcastMany {
            messages: healing_messages,
        } => {
            // Insert the new messages in the node's message set
            node.state.messages.lock().extend(healing_messages);
            // Send OK response
            respond!(node, request, OutboundResponse::BroadcastManyOk {});
        }
    }
}

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    enum InboundRequest {
        Read {},
        Broadcast { message: u64 },
        BroadcastMany { messages: FxIndexSet<u64> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    enum OutboundResponse<'a> {
        ReadOk { messages: &'a FxIndexSet<u64> },
        BroadcastOk {},
        BroadcastManyOk {},
    }
);

define_msg_kind!(
    inbound,
    #[derive(Debug, Serialize)]
    enum OutboundRequest<'a> {
        BroadcastMany { messages: &'a IndexSetSlice<u64> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    enum InboundResponse {
        BroadcastManyOk {},
    }
);
