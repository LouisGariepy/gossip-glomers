use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
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
    node::{Node, NodeBuilder, NodeChannel},
    FxIndexSet,
};
use rustc_hash::FxHashMap;

type BroadcastNode = Node<NodeState, InboundBroadcastRequest, InboundBroadcastResponse>;

#[derive(Debug, Default)]
struct NodeState {
    next_msg_id: AtomicU64,
    messages: Mutex<FxIndexSet<u64>>,
    neighbours: Vec<NodeId>,
    broadcast_timestamp: AtomicUsize,
    timeout_timestamps: Mutex<FxHashMap<NodeId, Option<usize>>>,
}

impl NodeState {
    fn next_msg_id(&self) -> MessageId {
        MessageId(self.next_msg_id.fetch_add(1, Ordering::SeqCst))
    }
}

#[tokio::main]
async fn main() {
    // Build node
    let node = NodeBuilder::init().with_state(initialize_node).build();
    // Spawn background healing task
    healing_task(node.clone());
    // Spawn background batch broadcasting task
    broadcasting_task(node.clone());
    // Start handling inbound messages
    node.run(request_handler);
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
        broadcast_timestamp: Default::default(),
        timeout_timestamps: failed_broadcasts,
    }
}

fn healing_task(node: Arc<BroadcastNode>) {
    tokio::spawn(async move {
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
                    let healthcheck_request_ser = {
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
                    let response = node.rpc(msg_id, &healthcheck_request_ser).await;

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
}

fn broadcasting_task(node: Arc<BroadcastNode>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            // Execute at every interval...
            interval.tick().await;

            let (broadcast_timestamp, messages) = {
                // Get messages since last broadcast_message
                let messages_guard = &node.state.messages.lock().unwrap();
                let broadcast_timestamp = node
                    .state
                    .broadcast_timestamp
                    .swap(messages_guard.len(), Ordering::SeqCst);
                let messages = &messages_guard[broadcast_timestamp..];
                if broadcast_timestamp == messages_guard.len() {
                    continue;
                }

                let messages = node
                    .state
                    .neighbours
                    .iter()
                    .map(|neighbour_id| {
                        (
                            *neighbour_id,
                            Message {
                                src: SiteId::Node(node.node_id),
                                dest: SiteId::Node(*neighbour_id),
                                body: Request {
                                    msg_id: node.state.next_msg_id(),
                                    kind: OutboundBroadcastRequest::BroadcastMany { messages },
                                },
                            }
                            .to_json(),
                        )
                    })
                    .collect::<Vec<_>>();
                (broadcast_timestamp, messages)
            };

            // Send new message batch to each susceptible neighbour
            for (neighbour_id, msg) in messages {
                let node = node.clone();
                tokio::spawn(async move {
                    let msg_id = node.state.next_msg_id();
                    // Send RPC request and get response
                    let response = node.rpc(msg_id, &msg).await;

                    if response.is_none() {
                        let mut timeout_timestamps_guard =
                            node.state.timeout_timestamps.lock().unwrap();
                        let timeout_timestamp =
                            timeout_timestamps_guard.get_mut(&neighbour_id).unwrap();
                        if timeout_timestamp.is_none() {
                            *timeout_timestamp = Some(broadcast_timestamp);
                        }
                    }
                });
            }
        }
    });
}

async fn request_handler(node: Arc<BroadcastNode>, msg: Message<Request<InboundBroadcastRequest>>) {
    match msg.body.kind {
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
        InboundBroadcastRequest::Broadcast { message } => {
            node.state.messages.lock().unwrap().insert(message);
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
            )
        }
        InboundBroadcastRequest::BroadcastMany {
            messages: healing_messages,
        } => {
            node.state.messages.lock().unwrap().extend(healing_messages);
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
}
