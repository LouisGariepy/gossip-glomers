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
use messages::{
    FollowerInboundRequest, FollowerInboundResponse, LeaderInboundRequest, LeaderInboundResponse,
    LeaderOutboundRequest,
};

use common::{
    id::NodeId,
    message::{Message, Request},
    node::{self, NodeBuilder, NodeTrait},
    respond, rpc, FxHashMap, HealthyMutex, HealthyRwLock,
};
use serde::{Deserialize, Serialize};

use crate::messages::{FollowerOutboundRequest, FollowerOutboundResponse, LeaderOutboundResponse};

mod messages;

type FollowerNode = node::Node<FollowerInboundRequest, FollowerInboundResponse, FollowerState>;
type LeaderNode = node::Node<LeaderInboundRequest, LeaderInboundResponse, LeaderState>;
type Wal = FxHashMap<String, QueueWal>;
type Log = FxHashMap<String, Queue>;

const LEADER: NodeId = NodeId(0);
const REPLICATION_INTERVAL: Duration = Duration::from_millis(1000);

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
        eprintln!("Node kind: Leader");
        let builder = builder.with_state(|builder_data, _| {
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
        });
        let node = LeaderNode::build(builder);
        spawn_replication_task(Arc::clone(&node));
        node.run(leader_request_handler);
    } else {
        eprintln!("Node kind: Follower");
        let builder = builder.with_state(|_, _| FollowerState::default());
        let node = FollowerNode::build(builder);
        node.run(follower_request_handler);
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

            // Create merged wal from all the follower wals
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

            // Commit the merged wal on the leader node too
            commit_wal(&mut node.state.log.write(), merged_wal);
        }
    });
}

fn merge_wal(merged_wal: &mut Wal, follower_wal: impl Iterator<Item = (String, QueueWal)>) {
    // Merge follower wal into merged wal.
    for (queue_key, mut follower_wal_value) in follower_wal {
        // Create queue entry in the wal if it does not already exist.
        let merged_wal_value = merged_wal.entry(queue_key).or_default();
        // Select highest commit
        if follower_wal_value.commit > merged_wal_value.commit {
            merged_wal_value.commit = follower_wal_value.commit;
        }
        // Append follower wal messages
        merged_wal_value
            .messages
            .append(&mut follower_wal_value.messages);
    }
}

fn commit_wal(log: &mut Log, merged_wal: Wal) {
    for (queue_key, wal_value) in merged_wal {
        let entry = log.entry(queue_key).or_default();
        if let Some(committed) = wal_value.commit {
            entry.commit = Some(committed);
        }
        entry.messages.extend(wal_value.messages);
    }
}

fn send_handler(wal: &mut Wal, queue_key: String, offset: u64, msg: u64) {
    wal.entry(queue_key)
        .or_default()
        .messages
        .push((offset, msg));
}

fn poll_handler(
    log: &Log,
    offsets: FxHashMap<String, u64>,
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
    mut wal_guard: MutexGuard<Wal>,
    commits: FxHashMap<String, u64>,
) {
    for (queue_key, commit) in commits {
        let queue_commit = log.get(&queue_key).and_then(|queue| queue.commit);
        let wal_commit = &mut wal_guard.entry(queue_key).or_default().commit;

        // Only set wal commit if the given commit is greater
        // than the current log commit (if wal commit is none) or
        // the current wal commit (if wal commit it some).
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
