mod messages;

use std::{future::ready, sync::Arc, time::UNIX_EPOCH};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::Mutex as TokioMutex;

use common::{
    id::ServiceId,
    message::{KvResponse, MaelstromError, Message, Request},
    node::{self, respond, rpc, BuildNode, NodeBuilder},
};

use messages::{InboundRequest, OutboundResponse};

type Node = node::Node<InboundRequest, KvResponse<u64>, TokioMutex<()>>;
type KvRequest = common::message::KvRequest<u64, u64>;

#[tokio::main]
async fn main() {
    NodeBuilder::init()
        .with_default_state()
        .build::<Node>()
        .run(request_handler);
}

async fn request_handler(node: Arc<Node>, request: Message<Request<InboundRequest>>) {
    match request.body.kind {
        InboundRequest::Add { delta } => {
            {
                // Lock the state mutex to prevent multiple threads from interacting with the
                // K-V store at the same time. This prevents data races where multiple threads
                // try to write at the same time.
                //
                // For example, thread A reads the value, then, right after, thread B reads and writes the value.
                // The value that thread A subsequently will write is incorrect since it relies on a stale
                // value, not the one that was recently written by thread B.
                let _guard = node.state.lock();

                // Send the RPC request to read the value of the key associated with
                // this node
                let read_response = rpc!(
                    node,
                    ServiceId::SeqKv,
                    KvRequest::Read {
                        key: node.id.into()
                    }
                )
                .await
                .expect("RPC request should not timeout");

                // If the key-value store responds with an OK, then we add the delta
                // to the read value.
                //
                // If it responds with a `KeyDoesNotExist` error, then the value to write
                // is simply the delta itself.
                let value_to_write = match read_response.body.kind {
                    KvResponse::ReadOk { value } => value + delta,
                    KvResponse::Error {
                        code: MaelstromError::KeyDoesNotExist,
                    } => delta,
                    _ => panic!(
                        "expected `ReadOk`, got `{:?}` instead",
                        read_response.body.kind
                    ),
                };

                // Send the RPC request to write the new value into the entry associated
                // with this node.
                let write_response = rpc!(
                    node,
                    ServiceId::SeqKv,
                    KvRequest::Write {
                        key: node.id.into(),
                        value: value_to_write
                    }
                )
                .await
                .expect("RPC request should not timeout");

                // Assert the write operation was successful
                assert!(
                    matches!(write_response.body.kind, KvResponse::WriteOk {}),
                    "expected a `WriteOk` response, got {:?} instead",
                    write_response.body.kind,
                );
            }

            // Send OK to the client.
            respond!(node, request, OutboundResponse::AddOk {});
        }
        InboundRequest::Read {} => {
            // Write to the dummy key with the current timestamp value.
            // This is a hack (more or less) to force maelstrom to provide
            // fresh reads. [See this comment](https://github.com/jepsen-io/maelstrom/issues/39#issuecomment-1445414521).
            let write_response = rpc!(
                node,
                ServiceId::SeqKv,
                KvRequest::Write {
                    // We use the maximal possible key to ensure we (hopefully) never collide
                    // with a genuine node's key.
                    key: u64::MAX,
                    // The value has to be globally unique to ensure fresh reads.
                    // The current timestamp should be Good Enough™.
                    value: std::time::SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64
                }
            )
            .await
            .expect("RPC request should not timeout");

            // Assert the write operation was successful
            assert!(
                matches!(write_response.body.kind, KvResponse::WriteOk {}),
                "expected a `WriteOk` response, got {:?} instead",
                write_response.body.kind,
            );

            // Get the value of all the nodes' entries.
            let counter = node
                .network
                .iter()
                // Spawn read tasks
                .map(|node_id| {
                    let node = Arc::clone(&node);
                    let node_id = *node_id;
                    tokio::spawn(async move {
                        rpc!(
                            node,
                            ServiceId::SeqKv,
                            KvRequest::Read {
                                key: node_id.into()
                            }
                        )
                        .await
                        .expect("RPC request did not time out")
                    })
                })
                .collect::<FuturesUnordered<_>>()
                // Handle result
                .map(|response| response.expect("task should be able to be joined"))
                // Map responses to values
                .map(|response| match response.body.kind {
                    KvResponse::ReadOk { value } => value,
                    KvResponse::Error {
                        code: MaelstromError::KeyDoesNotExist,
                    } => 0,
                    _ => panic!("expected `ReadOk`, got `{:?}` instead", response.body.kind),
                })
                // Sum all the values to get the whole counter's value.
                // If a node's entry doesn't exist yet, it doesn't contribute
                .fold(0, |acc, value| ready(acc + value))
                .await;

            // Send the counter value to the client.
            respond!(node, request, OutboundResponse::ReadOk { value: counter });
        }
    }
}
