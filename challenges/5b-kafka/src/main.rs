mod messages;

use std::sync::Arc;

use futures::{stream::FuturesUnordered, StreamExt};
use serde::Serialize;

use common::{
    id::{ServiceId, SiteId},
    message::{KvResponse, MaelstromError, Message, Request},
    node::{self, respond, rpc, BuildNode, NodeBuilder},
    FxHashMap, HealthyMutex,
};

use messages::{InboundRequest, OutboundResponse};

#[derive(Default)]
struct NodeState {
    log_commits: HealthyMutex<FxHashMap<String, usize>>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Key<'a> {
    /// Log keys are distinguished by the name of the log.
    /// Log entries contain the next available offset.
    LogKey(&'a str),
    /// Message keys are distringuished by the name of the log and
    /// the offset at which the message is located. Message entries
    /// contain the value of the message.
    MsgKey(&'a str, usize),
}

type Node = node::Node<InboundRequest, KvResponse<u64>, NodeState>;
type KvRequest<'a> = common::message::KvRequest<Key<'a>, u64>;

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_default_state()
        .build::<Node>()
        .run(request_handler);
}

async fn request_handler(node: Arc<Node>, request: Message<Request<InboundRequest>>) {
    match request.body.kind {
        InboundRequest::Send { key, msg } => {
            // Loop until an available offset is reserved.
            let offset = loop {
                if let Some(offset) = try_reserve_offset(&node, &key).await {
                    break offset;
                }
            };

            // Write the message to the reserved offset.
            let write_response = rpc!(
                node,
                SiteId::Service(ServiceId::LinKv),
                KvRequest::Write {
                    key: Key::MsgKey(&key, offset),
                    value: msg
                }
            )
            .await
            .expect("RPC request should not timeout");

            // Assert that the response is OK
            assert!(
                matches!(write_response.body.kind, KvResponse::WriteOk {}),
                "expected a `WriteOk` response, got {:?} instead",
                write_response.body.kind,
            );

            respond!(node, request, OutboundResponse::SendOk { offset });
        }
        InboundRequest::Poll { offsets } => {
            let msgs = offsets
                .into_iter()
                .map(|(key, mut offset)| {
                    // Spawn a separate task for each log
                    tokio::spawn({
                        let node = Arc::clone(&node);
                        async move {
                            let mut msgs = Vec::new();
                            loop {
                                // Read the message in this log at the given offset
                                let read_response = rpc!(
                                    node,
                                    ServiceId::LinKv,
                                    KvRequest::Read {
                                        key: Key::MsgKey(&key, offset)
                                    }
                                )
                                .await
                                .expect("RPC request did not time out");

                                // Add the message to our message list.
                                // If the given offset does not contain a message, it
                                // means the last available messsage was polled.
                                match read_response.body.kind {
                                    KvResponse::ReadOk { value } => msgs.push((offset, value)),
                                    KvResponse::Error {
                                        code: MaelstromError::KeyDoesNotExist,
                                    } => break,
                                    _ => panic!(
                                        "expected `ReadOk`, got `{:?}` instead",
                                        read_response.body.kind
                                    ),
                                }

                                // Increment the offset for the next turn of the loop.
                                offset += 1;
                            }
                            (key, msgs)
                        }
                    })
                })
                .collect::<FuturesUnordered<_>>()
                .map(|future| future.expect("task should be able to be joined"))
                .collect::<FxHashMap<String, Vec<(usize, u64)>>>()
                .await;
            respond!(node, request, OutboundResponse::PollOk { msgs });
        }
        InboundRequest::CommitOffsets { offsets } => {
            {
                let mut logs_guard = node.state.log_commits.lock();
                // Set the commited offset for all the given logs.
                for (key, offset) in offsets {
                    *logs_guard.entry(key).or_insert(offset) = offset;
                }
            }
            respond!(node, request, OutboundResponse::CommitOffsetsOk {});
        }
        InboundRequest::ListCommittedOffsets { keys } => {
            let logs_guard = node.state.log_commits.lock();
            // Get the commited offset of all the given keys that exist on this node.
            let offsets = keys
                .into_iter()
                .filter_map(|key| logs_guard.get(&key).map(|commited| (key, *commited)))
                .into();

            respond!(
                [logs_guard],
                node,
                request,
                OutboundResponse::ListCommittedOffsetsOk { offsets }
            );
        }
    }
}

async fn try_reserve_offset(node: &Node, key: &str) -> Option<usize> {
    // Fetch the first available offset.
    let read_response = rpc!(
        node,
        SiteId::Service(ServiceId::LinKv),
        KvRequest::Read {
            key: Key::LogKey(key)
        }
    )
    .await
    .expect("RPC request should not time out");

    let offset = match read_response.body.kind {
        // If the log entry exists, we get the offset value.
        KvResponse::ReadOk { value } => value,
        // If the log entry doesn't exist, we will assume a next available offset
        // of 0.
        KvResponse::Error {
            code: MaelstromError::KeyDoesNotExist,
        } => 0,
        _ => panic!(
            "expected `ReadOk`, got `{:?}` instead",
            read_response.body.kind
        ),
    };

    // Try to reserve an offset by incrementing the log's next available offset.
    // If the log did not exist, create it.
    let cas_response = rpc!(
        node,
        ServiceId::LinKv,
        KvRequest::Cas {
            key: Key::LogKey(key),
            from: offset,
            to: offset + 1,
            create_if_not_exists: true
        }
    )
    .await
    .expect("RPC request should not time out");

    match cas_response.body.kind {
        // If the log's next available offset was incremented, then an available offset
        // has successfully been found.
        KvResponse::CasOk {} => Some(offset as usize),
        // It is possible that in between the read and cas operation, another node
        // has snatched the offset that this node was trying to reserve.
        // In that case, retry the whole procedure.
        KvResponse::Error {
            code: MaelstromError::PreconditionFailed,
        } => None,
        _ => panic!(
            "expected `CasOk` or `MaelstromError::PreconditionFailed`, got `{:?}` instead",
            cas_response.body.kind
        ),
    }
}
