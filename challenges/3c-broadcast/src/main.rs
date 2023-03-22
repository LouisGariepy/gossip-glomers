use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use common::{
    define_msg_kind,
    id::{MessageId, NodeId, SiteId},
    message::{Message, Request, Response, TopologyRequest, TopologyResponse},
    node::{LifetimeGeneric, NodeBuilder, NodeChannel},
    FxHashMap, FxIndexSet, IndexSetSlice,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Debug, Default)]
struct NodeState {
    next_msg_id: AtomicU64,
    messages: AsyncMutex<FxIndexSet<u64>>,
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
        .build::<InboundBroadcastRequest, OutboundBroadcastResponse, OutboundBroadcastRequest, InboundBroadcastResponse>();

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

                    // Send RPC request and get response
                    let response = node
                        .rpc2_with(
                            SiteId::Node(node.node_id),
                            SiteId::Node(neighbour_id),
                            msg_id,
                            &node.state.messages,
                            |m| {
                                async move {
                                    OutboundBroadcastRequest::BroadcastMany {
                                        messages: &m.lock().await.as_slice()[timeout_timestamp..],
                                    }
                                }
                                .await
                            },
                        )
                        .await;

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
                // Try to insert new message in the node's message list
                let (old_len, inserted) = {
                    let mut node_messages_guard = node.state.messages.lock().unwrap();
                    let old_len = node_messages_guard.len();
                    let inserted = node_messages_guard.insert(message);
                    (old_len, inserted)
                };
                if inserted {
                    // Broadcast to all neighbouring nodes
                    for neighbour in &node.state.neighbours {
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
                            };

                            // Send RPC request
                            let response = node.rpc2(rpc_msg).await;

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
                node.send_response(Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Response {
                        in_reply_to: msg.body.msg_id,
                        kind: OutboundBroadcastResponse::BroadcastOk {},
                    },
                });
            }
            InboundBroadcastRequest::Read {} => {
                node.send_response(Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Response {
                        in_reply_to: msg.body.msg_id,
                        kind: OutboundBroadcastResponse::ReadOk {
                            messages: node.state.messages.lock().unwrap(),
                        },
                    },
                });
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
                    // Broadcast to all neighbouring nodes
                    let healing_messages = Arc::new(healing_messages);
                    for neighbour in &node.state.neighbours {
                        let node = node.clone();
                        let healing_messages = healing_messages.clone();
                        let neighbour = *neighbour;
                        tokio::spawn(async move {
                            // Build RPC request
                            let neighbour = neighbour;
                            let msg_id = node.state.next_msg_id();

                            // Send RPC request
                            let response = node
                                .rpc2(Message {
                                    src: msg.dest,
                                    dest: SiteId::Node(neighbour),
                                    body: Request {
                                        msg_id,
                                        kind: OutboundBroadcastRequest::BroadcastMany {
                                            messages: healing_messages.as_slice(),
                                        },
                                    },
                                })
                                .await;

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
                node.send_response(Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Response {
                        in_reply_to: msg.body.msg_id,
                        kind: OutboundBroadcastResponse::BroadcastManyOk {},
                    },
                })
            }
        }
    });
}

fn initialize_node(node_id: &NodeId, channel: &mut NodeChannel) -> NodeState {
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

define_msg_kind!(
    #[derive(Debug, Deserialize)]
    pub enum InboundBroadcastRequest {
        Read {},
        Broadcast { message: u64 },
        BroadcastMany { messages: FxIndexSet<u64> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize)]
    pub enum OutboundBroadcastResponse<'a> {
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
    pub enum OutboundBroadcastRequest<'a> {
        Read {},
        Broadcast { message: u64 },
        BroadcastMany { messages: &'a IndexSetSlice<u64> },
    }
);

define_msg_kind!(
    #[derive(Debug, Serialize, Deserialize)]
    pub enum InboundBroadcastResponse {
        ReadOk { messages: FxIndexSet<u64> },
        BroadcastOk {},
        BroadcastManyOk {},
    }
);
