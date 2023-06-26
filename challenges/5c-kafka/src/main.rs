mod messages;

use std::{
    collections::BTreeMap,
    future::ready,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, MutexGuard, RwLockReadGuard,
    },
    time::Duration,
};

use futures::{stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};

use common::{
    id::NodeId,
    message::{Message, Request},
    node::{self, BuildNode, NodeBuilder},
    respond, rpc, FxHashMap, HealthyMutex, HealthyRwLock,
};

use messages::{
    FollowerInboundRequest, FollowerInboundResponse, FollowerOutboundRequest,
    FollowerOutboundResponse, LeaderInboundRequest, LeaderInboundResponse, LeaderOutboundRequest,
    LeaderOutboundResponse,
};

/// Node type for follower nodes.
type FollowerNode = node::Node<FollowerInboundRequest, FollowerInboundResponse, FollowerState>;
/// Node type for the leader node.
type LeaderNode = node::Node<LeaderInboundRequest, LeaderInboundResponse, LeaderState>;
/// A hash map type containing write-ahead logs for log queues.
type Wal = FxHashMap<String, QueueWal>;
/// A hash map type containing the log queues.
type Log = FxHashMap<String, Queue>;

/// The leader node will always be the node with id 0.
const LEADER: NodeId = NodeId(0);
/// The interval at which log queues will be replicated.
/// Increasing this will cause less replication chatter, but
/// it will increase the number of stale polls.
const REPLICATION_INTERVAL: Duration = Duration::from_millis(1000);

/// A write-ahead log for a single log queue.
/// This type has the same structure as the actual (queue)[`Queue`] but
/// it uses a
#[derive(Debug, Default, Serialize, Deserialize)]
struct QueueWal {
    commit: Option<u64>,
    messages: Vec<(u64, u64)>,
}

#[derive(Default)]
struct Queue {
    commit: Option<u64>,
    messages: BTreeMap<u64, u64>,
}

#[derive(Default)]
struct FollowerState {
    log: HealthyRwLock<Log>,
    wal: HealthyMutex<Wal>,
}

#[derive(Default)]
struct LeaderState {
    log: HealthyRwLock<Log>,
    wal: HealthyMutex<Wal>,
    followers: Vec<NodeId>,
    global_offset: AtomicU64,
}

impl LeaderState {
    fn reserve_offset(&self) -> u64 {
        self.global_offset.fetch_add(1, Ordering::SeqCst)
    }
}

#[tokio::main]
async fn main() {
    let builder = NodeBuilder::init();
    if builder.initial_data.id == LEADER {
        let node = builder
            .with_state(|builder_data, _| {
                let followers = builder_data
                    .network
                    .iter()
                    .copied()
                    .filter(|&id| id != builder_data.id)
                    .collect();
                LeaderState {
                    followers,
                    ..Default::default()
                }
            })
            .build::<LeaderNode>();
        spawn_replication_task(Arc::clone(&node));
        node.run(leader_request_handler);
    } else {
        builder
            .with_default_state()
            .build::<FollowerNode>()
            .run(follower_request_handler);
    };
}

async fn follower_request_handler(
    node: Arc<FollowerNode>,
    request: Message<Request<FollowerInboundRequest>>,
) {
    match request.body.kind {
        FollowerInboundRequest::Send { queue_key, msg } => {
            let reserve_offset_response =
                rpc!(node, LEADER, FollowerOutboundRequest::ReserveOffset {})
                    .await
                    .expect("RPC request should not time out");
            let offset = match reserve_offset_response.body.kind {
                FollowerInboundResponse::ReserveOffsetOk { offset } => offset,
            };
            send_handler(&mut node.state.wal.lock(), queue_key, offset, msg);
            respond!(node, request, FollowerOutboundResponse::SendOk { offset });
        }
        FollowerInboundRequest::Poll { offsets } => {
            let log = node.state.log.read();
            let msgs = poll_handler(&log, offsets)
                .map(|(queue_key, msgs)| (queue_key, msgs.into()))
                .into();
            respond!(
                [log],
                node,
                request,
                FollowerOutboundResponse::PollOk { msgs }
            );
        }
        FollowerInboundRequest::CommitOffsets { commits } => {
            commit_offsets_handler(node.state.log.read(), node.state.wal.lock(), commits);
            respond!(node, request, FollowerOutboundResponse::CommitOffsetsOk {});
        }
        FollowerInboundRequest::ListCommittedOffsets { queue_keys } => {
            let commits = list_committed_offsets_handler(node.state.log.read(), queue_keys).into();
            respond!(
                node,
                request,
                FollowerOutboundResponse::ListCommittedOffsetsOk { offsets: commits },
            );
        }
        FollowerInboundRequest::Wal {} => {
            let mut wal = node.state.wal.lock();
            respond!(node, request, FollowerOutboundResponse::WalOk { wal: &wal });
            wal.clear();
        }
        FollowerInboundRequest::Replicate { merged_wal } => {
            commit_wal(&mut node.state.log.write(), merged_wal);
            respond!(node, request, FollowerOutboundResponse::ReplicateOk {});
        }
    }
}

async fn leader_request_handler(
    node: Arc<LeaderNode>,
    request: Message<Request<LeaderInboundRequest>>,
) {
    match request.body.kind {
        LeaderInboundRequest::Send { queue_key, msg } => {
            let offset = node.state.reserve_offset();
            send_handler(&mut node.state.wal.lock(), queue_key, offset, msg);
            respond!(node, request, LeaderOutboundResponse::SendOk { offset });
        }
        LeaderInboundRequest::Poll { offsets } => {
            let log = node.state.log.read();
            let msgs = poll_handler(&log, offsets)
                .map(|(queue_key, msgs)| (queue_key, msgs.into()))
                .into();
            respond!(
                [log],
                node,
                request,
                LeaderOutboundResponse::PollOk { msgs }
            );
        }
        LeaderInboundRequest::CommitOffsets { commits } => {
            commit_offsets_handler(node.state.log.read(), node.state.wal.lock(), commits);
            respond!(node, request, LeaderOutboundResponse::CommitOffsetsOk {});
        }
        LeaderInboundRequest::ListCommittedOffsets { queue_keys } => {
            let commits = list_committed_offsets_handler(node.state.log.read(), queue_keys).into();
            respond!(
                node,
                request,
                LeaderOutboundResponse::ListCommittedOffsetsOk { offsets: commits },
            );
        }
        LeaderInboundRequest::ReserveOffset {} => {
            let offset = node.state.reserve_offset();
            respond!(
                node,
                request,
                LeaderOutboundResponse::ReserveOffsetOk { offset }
            );
        }
    }
}

fn spawn_replication_task(node: Arc<LeaderNode>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(REPLICATION_INTERVAL);
        loop {
            // Execute at every interval...
            interval.tick().await;

            // Create the merged WAL from all the follower WALs
            let mut merged_wal = node
                .state
                .followers
                .iter()
                .map(|&follower| rpc!(node, follower, LeaderOutboundRequest::Wal {}))
                .collect::<FuturesUnordered<_>>()
                .map(|wal_response| wal_response.expect("RPC request should not time out"))
                .map(|wal_response| match wal_response.body.kind {
                    LeaderInboundResponse::WalOk { wal } => wal,
                    _ => panic!(
                        "expected `WalOk`, got `{:?}` instead",
                        wal_response.body.kind
                    ),
                })
                .fold(Wal::default(), |mut merged_wal, mut follower_wal| {
                    merge_wal(&mut merged_wal, follower_wal.drain());
                    ready(merged_wal)
                })
                .await;
            // Merge the leader's WAL too!
            merge_wal(&mut merged_wal, node.state.wal.lock().drain());

            // Send replication requests to all followers
            let mut replication_responses = node
                .state
                .followers
                .iter()
                .map(|&follower| {
                    rpc!(
                        node,
                        follower,
                        LeaderOutboundRequest::Replicate {
                            merged_wal: &merged_wal
                        }
                    )
                })
                .collect::<FuturesUnordered<_>>()
                .map(|replication_response| {
                    replication_response.expect("RPC request should not time out")
                });

            // Ensure that all followers responded an OK
            while let Some(replication_response) = replication_responses.next().await {
                assert!(
                    matches!(
                        replication_response.body.kind,
                        LeaderInboundResponse::ReplicateOk { .. }
                    ),
                    "expected a `ReplicateOk` response, got {:?} instead",
                    replication_response.body.kind,
                );
            }

            // Commit the merged WAL on the leader node too
            commit_wal(&mut node.state.log.write(), merged_wal);
        }
    });
}

fn merge_wal(merged_wal: &mut Wal, follower_wal: impl Iterator<Item = (String, QueueWal)>) {
    // Merge follower WAL into merged WAL.
    for (queue_key, mut follower_queue_wal) in follower_wal {
        // Create queue entry in the WAL if it does not already exist.
        let merged_queue_wal = merged_wal.entry(queue_key).or_default();
        // Select highest commit
        if follower_queue_wal.commit > merged_queue_wal.commit {
            merged_queue_wal.commit = follower_queue_wal.commit;
        }
        // Append follower WAL messages
        merged_queue_wal
            .messages
            .append(&mut follower_queue_wal.messages);
    }
}

fn commit_wal(log: &mut Log, merged_wal: Wal) {
    for (queue_key, queue_wal) in merged_wal {
        // Create queue if it does not exist
        let queue = log.entry(queue_key).or_default();
        // Set the new committed offset if the WAL has some.
        if queue_wal.commit.is_some() {
            queue.commit = queue_wal.commit;
        }
        // Extend the queue's message with the WAL's
        // messages for that queue
        queue.messages.extend(queue_wal.messages);
    }
}

fn send_handler(wal: &mut Wal, queue_key: String, offset: u64, msg: u64) {
    // Adds a message in the WAL
    wal.entry(queue_key)
        .or_default()
        .messages
        .push((offset, msg));
}

fn poll_handler(
    log: &Log,
    offsets: Vec<(String, u64)>,
) -> impl Iterator<Item = (String, impl Iterator<Item = (&u64, &u64)>)> {
    offsets
        .into_iter()
        // Filter to keep only log entries that exist
        .filter_map(|(queue_key, offset)| {
            log.get(&queue_key).map(|queue| {
                // Get all the message from the given offset onwards.
                let poll_messages = queue.messages.iter().skip(offset as usize);
                (queue_key, poll_messages)
            })
        })
}

fn commit_offsets_handler(
    log: RwLockReadGuard<Log>,
    mut wal: MutexGuard<Wal>,
    commits: Vec<(String, u64)>,
) {
    for (queue_key, commit) in commits {
        let queue_commit = log.get(&queue_key).and_then(|queue| queue.commit);
        let wal_commit = &mut wal.entry(queue_key).or_default().commit;

        // Only set WAL commit if the given commit is greater
        // than the current log commit (if WAL commit is none) or
        // the current WAL commit (if WAL commit it some).
        let commit = Some(commit);
        if commit > wal_commit.or(queue_commit) {
            *wal_commit = commit;
        };
    }
}

fn list_committed_offsets_handler(
    log: RwLockReadGuard<Log>,
    queue_keys: Vec<String>,
) -> impl Iterator<Item = (String, u64)> + '_ {
    // Filter out queue keys that don't exist, or
    // queue keys for which there is no commit yet.
    queue_keys.into_iter().filter_map(move |queue_key| {
        log.get(&queue_key)
            .and_then(|queue| queue.commit.map(|commit| (queue_key, commit)))
    })
}
