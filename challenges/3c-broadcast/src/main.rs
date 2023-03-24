use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use common::{
    define_msg_kind,
    id::NodeId,
    message::{Message, Request, Response, TopologyRequest, TopologyResponse},
    node::{NodeBuilder, NodeChannel},
    rpc, FxHashMap, FxIndexSet, IndexSetSlice, TupleMap,
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

#[derive(Debug, Default)]
struct NodeState {
    /// The set of all messages held by this node.
    messages: Mutex<FxIndexSet<u64>>,
    /// The direct neighbours of this node.
    neighbours: Vec<NodeId>,
    /// Map recording the timeout timestamp of each neighbour.
    /// All messages with greater or equal timestamps will be
    /// broadcasted to the corresponding neighbour during the
    /// next healing broadcast.
    timeout_timestamps: Mutex<FxHashMap<NodeId, Option<usize>>>,
}

#[tokio::main]
async fn main() {
    // Build node
    let node = NodeBuilder::init()
        .with_state(initialize_node)
        .build::<InboundRequest, OutboundResponse, OutboundRequest, InboundResponse>();
    // Spawn background healing task
    tokio::spawn(spawn_healing(Arc::clone(&node)));
    // Run request handler
    node.run(request_handler);
}

fn initialize_node(node_id: NodeId, channel: &mut NodeChannel) -> NodeState {
    // Receive and respond to initial topology message
    let topology_request = channel.receive_msg::<TopologyRequest>();
    let topology_response = Message {
        src: topology_request.dest,
        dest: topology_request.src,
        body: Response {
            in_reply_to: topology_request.body.msg_id,
            kind: TopologyResponse::TopologyOk {},
        },
    };
    channel.send_msg(&topology_response);

    // Obtain node neighbours from topology
    let mut topology = topology_request.body.kind.into_inner().topology;
    let neighbours = topology
        .remove(&node_id)
        .expect("the topology should include this node's neighbours");

    // Create empty list for failed broadcasts
    let failed_broadcasts = Mutex::new(
        neighbours
            .iter()
            .map(|&neighbour| (neighbour, None))
            .collect(),
    );
    NodeState {
        messages: Mutex::default(),
        neighbours,
        timeout_timestamps: failed_broadcasts,
    }
}

async fn spawn_healing(node: Arc<Node>) {
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
            .filter_map(|(neighbour, timeout_timestamp)| {
                timeout_timestamp.map(|timeout_timestamp| (*neighbour, timeout_timestamp))
            })
            .collect::<Vec<_>>();

        // Send outstanding messages to each susceptible neighbour
        for (neighbour_id, timeout_timestamp) in suceptible_neighbours {
            tokio::spawn({
                let node = Arc::clone(&node);
                async move {
                    // Send broadcast request and await response
                    let response = rpc!(
                        node,
                        neighbour_id,
                        OutboundRequest::BroadcastMany {
                            messages: &node.state.messages.lock().unwrap().as_slice()
                                [timeout_timestamp..],
                        }
                    )
                    .await;

                    // If the response is OK, remove the timeout timestamp
                    // This signifies that this neighbour has now received all
                    // outstanding messages
                    if let Some(response) = response {
                        if matches!(response.body.kind, InboundResponse::BroadcastManyOk {}) {
                            node.state
                                .timeout_timestamps
                                .lock()
                                .unwrap()
                                .get_mut(&neighbour_id)
                                .unwrap()
                                .take();
                        }
                    }
                }
            });
        }
    }
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
            // Try to insert new message in the node's message set
            let inserted = node
                .state
                .messages
                .lock()
                .unwrap()
                .insert_full(message)
                .map(|(index, inserted)| inserted.then_some(index));

            // If the message was newly inserted
            // then start the broadcasting procedure
            if let Some(message_index) = inserted {
                // Broadcast to all neighbouring nodes
                for neighbour in &node.state.neighbours {
                    tokio::spawn({
                        let node = Arc::clone(&node);
                        let neighbour = *neighbour;
                        async move {
                            // Send broadcast request and await response
                            let response =
                                rpc!(node, neighbour, OutboundRequest::Broadcast { message }).await;

                            // If the RPC request timed out, record the latest message's index
                            // in the map entry for this neighbour. This means that this
                            // neighbour has outstanding messages to pick up on.
                            if response.is_none() {
                                node.state
                                    .timeout_timestamps
                                    .lock()
                                    .unwrap()
                                    .get_mut(&neighbour)
                                    .unwrap()
                                    .get_or_insert(message_index);
                            }
                        }
                    });
                }
            }

            // After broadcasting procedure is
            // complete, send OK response
            node.send_response(Message {
                src: msg.dest,
                dest: msg.src,
                body: Response {
                    in_reply_to: msg.body.msg_id,
                    kind: OutboundResponse::BroadcastOk {},
                },
            });
        }
        InboundRequest::BroadcastMany { messages } => {
            // Try to insert the new messages in the node's message set
            let inserted = {
                let mut node_messages = node.state.messages.lock().unwrap();
                let old_len = node_messages.len();
                node_messages.extend(&messages);
                (old_len < node_messages.len()).then_some(old_len)
            };

            // If any of the messages was newly inserted
            // start the broadcasting procedure
            if let Some(broadcast_index) = inserted {
                // Broadcast to all neighbouring nodes
                let healing_messages = Arc::new(messages);
                for neighbour in &node.state.neighbours {
                    tokio::spawn({
                        let node = Arc::clone(&node);
                        let healing_messages = Arc::clone(&healing_messages);
                        let neighbour = *neighbour;
                        async move {
                            // Send RPC request and await response
                            let response = rpc!(
                                node,
                                neighbour,
                                OutboundRequest::BroadcastMany {
                                    messages: healing_messages.as_slice(),
                                }
                            )
                            .await;

                            // If the RPC request timed out, record the timestamp
                            if response.is_none() {
                                node.state
                                    .timeout_timestamps
                                    .lock()
                                    .unwrap()
                                    .get_mut(&neighbour)
                                    .unwrap()
                                    .get_or_insert(broadcast_index);
                            }
                        }
                    });
                }
            }

            // After broadcasting procedure is
            // complete, respond with an OK response
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
        Broadcast { message: u64 },
        BroadcastMany { messages: &'a IndexSetSlice<u64> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum InboundResponse {
        ReadOk { messages: FxIndexSet<u64> },
        BroadcastOk {},
        BroadcastManyOk {},
    }
);
