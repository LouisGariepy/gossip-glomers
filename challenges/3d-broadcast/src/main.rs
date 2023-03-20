use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use common::{
    id::{MessageId, NodeId, SiteId},
    message::{
        InboundBroadcastRequest, InboundBroadcastResponse, Message, OutboundBroadcastRequest,
        OutboundBroadcastResponse, Request, Response, TopologyRequest, TopologyResponse,
    },
    node::{NodeBuilder, NodeChannel},
    FxIndexSet,
};
use rustc_hash::FxHashMap;

#[derive(Debug, Default)]
struct NodeState {
    next_msg_id: AtomicU64,
    messages: Mutex<FxIndexSet<u64>>,
    neighbours: Vec<NodeId>,
    timeout_timestamps: Mutex<FxHashMap<NodeId, Option<usize>>>,
}
impl NodeState {
    fn next_msg_id(&self) -> MessageId {
        MessageId(self.next_msg_id.fetch_add(1, Ordering::SeqCst))
    }
}

#[tokio::main]
async fn main() {
    let node = NodeBuilder::init()
        .with_state(initialize_node)
        .build::<InboundBroadcastRequest, InboundBroadcastResponse>();

    // Background healing task
    let node_clone = node.clone();
    tokio::spawn(async move {
        let node = node_clone;
        let mut interval = tokio::time::interval(Duration::from_millis(1000));
        loop {
            // Execute at every interval...
            interval.tick().await;
            // Get neighbour's timeout timestamp
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
                let node = node.clone();
                tokio::spawn(async move {
                    let msg_id = node.state.next_msg_id();
                    let healthcheck_response_ser = {
                        // Get outstanding messages since timeout timestamp
                        let message_guard = &node.state.messages.lock().unwrap();
                        let messages = &message_guard[timeout_timestamp..];
                        // Create healthcheck request
                        Message {
                            src: SiteId::Node(node.node_id),
                            dest: SiteId::Node(neighbour_id),
                            body: Request {
                                msg_id,
                                kind: OutboundBroadcastRequest::BroadcastMany { messages },
                            },
                        }
                        .to_json()
                    };

                    // Send RPC request and get response
                    let response = node.rpc(msg_id, &healthcheck_response_ser).await;

                    // If the response is a HealthCheckOK message, remove the timeout timestamp
                    if let Some(response) = response {
                        if let InboundBroadcastResponse::BroadcastManyOk {} = response.body.kind {
                            *node
                                .state
                                .timeout_timestamps
                                .lock()
                                .unwrap()
                                .get_mut(&neighbour_id)
                                .unwrap() = None;
                        }
                    }
                });
            }
        }
    });

    node.run(|node, msg| async move {
        match msg.body.kind {
            InboundBroadcastRequest::Broadcast { message } => {
                // Try to insert the new message in the node's message list
                let (old_len, inserted) = {
                    let mut node_messages_guard = node.state.messages.lock().unwrap();
                    let old_len = node_messages_guard.len();
                    let inserted = node_messages_guard.insert(message);
                    (old_len, inserted)
                };
                // If the message was inserted (i.e. it was not already known to this node)
                // start broadcasting procedure
                if inserted {
                    // Don't broadcast it to the node who just broadcasted it to us though.
                    let susceptible_neighbours = node
                        .state
                        .neighbours
                        .iter()
                        .filter(|&&neighbour_id| SiteId::Node(neighbour_id) != msg.src);
                    // Broadcast to all susceptible neighbours
                    for neighbour in susceptible_neighbours {
                        let node = node.clone();
                        let neighbour = *neighbour;
                        tokio::spawn(async move {
                            // Build RPC request
                            let neighbour = neighbour;
                            let msg_id = node.state.next_msg_id();
                            let rpc_msg = Message {
                                src: msg.dest,
                                dest: SiteId::Node(neighbour),
                                body: Request {
                                    msg_id,
                                    kind: OutboundBroadcastRequest::Broadcast { message },
                                },
                            }
                            .to_json();

                            // Send RPC request
                            let response = node.rpc(msg_id, &rpc_msg).await;

                            // If the RPC request timed out, record the latest message's index
                            // in the map entry for this neighbour.
                            if response.is_none() {
                                let mut timeout_timestamps_guard =
                                    node.state.timeout_timestamps.lock().unwrap();
                                let timeout_timestamp =
                                    timeout_timestamps_guard.get_mut(&neighbour).unwrap();
                                if timeout_timestamp.is_none() {
                                    *timeout_timestamp = Some(old_len);
                                }
                            }
                        });
                    }
                }
                node.send_msg(
                    &Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundBroadcastResponse::BroadcastOk {},
                        },
                    }
                    .to_json(),
                );
            }
            InboundBroadcastRequest::Read {} => {
                node.send_msg(
                    &Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundBroadcastResponse::ReadOk {
                                messages: &node.state.messages.lock().unwrap(),
                            },
                        },
                    }
                    .to_json(),
                );
            }
            InboundBroadcastRequest::BroadcastMany {
                messages: healing_messages,
            } => {
                let (old_len, inserted) = {
                    let mut inserted = false;
                    let mut node_messages = node.state.messages.lock().unwrap();
                    let old_len = node_messages.len();
                    for message in &healing_messages {
                        let msg_inserted = node_messages.insert(*message);
                        if msg_inserted {
                            inserted = true;
                        }
                    }
                    (old_len, inserted)
                };
                if inserted {
                    let susceptible_neighbours = node
                        .state
                        .neighbours
                        .iter()
                        .filter(|&&neighbour_id| SiteId::Node(neighbour_id) != msg.src);
                    // Broadcast to all neighbouring nodes
                    let healing_messages = Arc::new(healing_messages);
                    for neighbour in susceptible_neighbours {
                        let node = node.clone();
                        let healing_messages = healing_messages.clone();
                        let neighbour = *neighbour;
                        tokio::spawn(async move {
                            // Build RPC request
                            let neighbour = neighbour;
                            let msg_id = node.state.next_msg_id();
                            let rpc_msg = Message {
                                src: msg.dest,
                                dest: SiteId::Node(neighbour),
                                body: Request {
                                    msg_id,
                                    kind: OutboundBroadcastRequest::BroadcastMany {
                                        messages: &healing_messages[..],
                                    },
                                },
                            }
                            .to_json();

                            // Send RPC request
                            let response = node.rpc(msg_id, &rpc_msg).await;

                            // If the RPC request timed out, record the latest message's index
                            // in the map entry for this neighbour.
                            if response.is_none() {
                                let mut timeout_timestamps_guard =
                                    node.state.timeout_timestamps.lock().unwrap();
                                let timeout_timestamp =
                                    timeout_timestamps_guard.get_mut(&neighbour).unwrap();
                                if timeout_timestamp.is_none() {
                                    *timeout_timestamp = Some(old_len);
                                }
                            }
                        });
                    }
                }
                node.send_msg(
                    &Message {
                        src: msg.dest,
                        dest: msg.src,
                        body: Response {
                            in_reply_to: msg.body.msg_id,
                            kind: OutboundBroadcastResponse::BroadcastManyOk {},
                        },
                    }
                    .to_json(),
                )
            }
        }
    });
}

fn initialize_node(channel: &mut NodeChannel) -> NodeState {
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
    let node_id = topology_request
        .dest
        .as_node_id()
        .expect("expected this process to be a node");
    let neighbours = topology
        .remove(node_id)
        .expect("the topology should include this node's neighbours");

    // Create empty list for failed broadcasts
    let failed_broadcasts = Mutex::new(
        neighbours
            .iter()
            .map(|&neighbour| (neighbour, None))
            .collect(),
    );
    NodeState {
        next_msg_id: Default::default(),
        messages: Default::default(),
        neighbours,
        timeout_timestamps: failed_broadcasts,
    }
}
