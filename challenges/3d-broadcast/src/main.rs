use std::{sync::Arc, time::Duration};

use common::{
    define_msg_kind, respond, rpc, FxHashMap, FxIndexSet, HealthyMutex, IndexSetSlice, Message,
    NodeBuilder, NodeChannel, NodeId, Request, Response, TopologyRequest, TopologyResponse,
    TupleMap,
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

#[derive(Debug, Default)]
struct NodeState {
    /// The set of all messages held by this node.
    messages: HealthyMutex<FxIndexSet<u64>>,
    /// The direct neighbours of this node.
    neighbours: Vec<NodeId>,
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
    tokio::spawn(healing_task(Arc::clone(&node)));
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

    // Create empty list for failed broadcasts
    let failed_broadcasts = HealthyMutex::new(
        neighbours
            .iter()
            .map(|&neighbour| (neighbour, None))
            .collect(),
    );
    NodeState {
        messages: HealthyMutex::default(),
        neighbours,
        timeout_timestamps: failed_broadcasts,
    }
}

async fn healing_task(node: Arc<Node>) {
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
            tokio::spawn({
                let node = Arc::clone(&node);
                async move {
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
                    if let Some(response) = response {
                        if let InboundResponse::BroadcastManyOk {} = response.body.kind {
                            node.state
                                .timeout_timestamps
                                .lock()
                                .get_mut(&neighbour)
                                .unwrap()
                                .take();
                        }
                    }
                }
            });
        }
    }
}

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
            // Try to insert the new message in the node's message set
            let inserted = node
                .state
                .messages
                .lock()
                .insert_full(message)
                .map(|(index, inserted)| inserted.then_some(index));

            // If the message was newly inserted
            // then start the broadcasting procedure
            if let Some(message_index) = inserted {
                // Don't broadcast it to the node who just broadcasted it to us.
                let susceptible_neighbours = node
                    .state
                    .neighbours
                    .iter()
                    .filter(|&&neighbour| neighbour != request.src);

                // Broadcast to susceptible neighbours
                for neighbour in susceptible_neighbours {
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
            respond!(node, request, OutboundResponse::BroadcastOk {});
        }
        InboundRequest::BroadcastMany { messages } => {
            // Try to insert the new messages in the node's message set
            let inserted = {
                let mut node_messages = node.state.messages.lock();
                let old_len = node_messages.len();
                node_messages.extend(&messages);
                (old_len < node_messages.len()).then_some(old_len)
            };

            // If any of the messages was newly inserted
            // start the broadcasting procedure
            if let Some(broadcast_index) = inserted {
                // Don't broadcast it to the node who just broadcasted it to us.
                let susceptible_neighbours = node
                    .state
                    .neighbours
                    .iter()
                    .filter(|&&neighbour| neighbour != request.src);
                // Broadcast to susceptible neighbours
                let healing_messages = Arc::new(messages);
                for neighbour in susceptible_neighbours {
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
                                    messages: &healing_messages[..],
                                }
                            )
                            .await;

                            // If the RPC request timed out, record the timestamp
                            if response.is_none() {
                                node.state
                                    .timeout_timestamps
                                    .lock()
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
    #[derive(Debug, Serialize)]
    enum OutboundRequest<'a> {
        Broadcast { message: u64 },
        BroadcastMany { messages: &'a IndexSetSlice<u64> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    enum InboundResponse {
        BroadcastOk {},
        BroadcastManyOk {},
    }
);
