#![allow(dead_code, unused_imports, unused_variables)]

use jsonrpc_pubsub::{
    typed::{Sink, Subscriber},
    SubscriptionId,
};

use crate::rpc::{
    helpers::{errors, Subscribers},
    metadata::Metadata,
    traits::PubSub,
    types::{pubsub, Header as RpcHeader, Log as RpcLog, H256},
};

use jsonrpc_core::{
    futures::{sync::mpsc, Future, IntoFuture, Stream},
    BoxFuture, Error, Result,
};

use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

use cfxcore::{
    BlockDataManager, ConsensusGraph, Notifications, SynchronizationGraph,
};

use futures::future::{FutureExt, TryFutureExt};
use parking_lot::RwLock;
use primitives::{filter::Filter, log_entry::LocalizedLogEntry, BlockHeader};
use runtime::Executor;

type Client = Sink<pubsub::Result>;

/// Cfx PubSub implementation.
pub struct PubSubClient {
    handler: Arc<ChainNotificationHandler>,
    header_subscribers: Arc<RwLock<Subscribers<Client>>>,
    pivot_subscribers: Arc<RwLock<Subscribers<Client>>>,
    log_subscribers: Arc<RwLock<Subscribers<(Client, Filter)>>>,
}

fn receive_new_block_hashes(
    handler: Arc<ChainNotificationHandler>, data_man: Arc<BlockDataManager>,
    notifications: Arc<Notifications>,
)
{
    // subscribe to the `new_block_hashes` channel
    let receiver = notifications.new_block_hashes.subscribe();

    let handler_clone = handler.clone();

    // loop asynchronously
    let fut = receiver.for_each(move |(hash, _)| {
        let header = match data_man.block_header_by_hash(&hash) {
            Some(h) => handler_clone.notify_headers(&[(*h).clone()]),
            None => return error!("Header {:?} not found", hash),
        };
    });

    handler.executor.spawn(fut.unit_error().boxed().compat())
}

fn receive_pivot_updates(
    handler: Arc<ChainNotificationHandler>, data_man: Arc<BlockDataManager>,
    notifications: Arc<Notifications>,
)
{
    // subscribe to the `pivot_updates` channel
    let receiver = notifications.pivot_updates.subscribe();

    let handler_clone = handler.clone();

    // loop asynchronously
    let fut = receiver.for_each(move |hash| {
        let header = match data_man.block_header_by_hash(&hash) {
            Some(h) => handler_clone.notify_pivots(&[(*h).clone()]),
            None => return error!("Header {:?} not found", hash),
        };
    });

    handler.executor.spawn(fut.unit_error().boxed().compat())
}

fn receive_logs_executed(
    handler: Arc<ChainNotificationHandler>, notifications: Arc<Notifications>,
) {
    // subscribe to the `logs_executed` channel
    let receiver = notifications.logs_executed.subscribe();

    let handler_clone = handler.clone();

    // loop asynchronously
    let fut = receiver.for_each(move |logs| {
        handler_clone.notify_logs(&logs[..]);
    });

    handler.executor.spawn(fut.unit_error().boxed().compat())
}

impl PubSubClient {
    /// Creates new `PubSubClient`.
    pub fn new(
        executor: Executor, consensus: Arc<ConsensusGraph>,
        notifications: Arc<Notifications>,
    ) -> Self
    {
        let header_subscribers = Arc::new(RwLock::new(Subscribers::default()));
        let pivot_subscribers = Arc::new(RwLock::new(Subscribers::default()));
        let log_subscribers = Arc::new(RwLock::new(Subscribers::default()));

        let handler = Arc::new(ChainNotificationHandler {
            executor,
            consensus: consensus.clone(),
            header_subscribers: header_subscribers.clone(),
            pivot_subscribers: pivot_subscribers.clone(),
            log_subscribers: log_subscribers.clone(),
        });

        receive_new_block_hashes(
            handler.clone(),
            consensus.data_man.clone(),
            notifications.clone(),
        );

        receive_pivot_updates(
            handler.clone(),
            consensus.data_man.clone(),
            notifications.clone(),
        );

        receive_logs_executed(handler.clone(), notifications.clone());

        PubSubClient {
            handler,
            header_subscribers,
            pivot_subscribers,
            log_subscribers,
        }
    }

    /// Returns a chain notification handler.
    pub fn handler(&self) -> Weak<ChainNotificationHandler> {
        Arc::downgrade(&self.handler)
    }
}

/// PubSub notification handler.
pub struct ChainNotificationHandler {
    pub executor: Executor,

    consensus: Arc<ConsensusGraph>,

    header_subscribers: Arc<RwLock<Subscribers<Client>>>,
    pivot_subscribers: Arc<RwLock<Subscribers<Client>>>,
    log_subscribers: Arc<RwLock<Subscribers<(Client, Filter)>>>,
}

impl ChainNotificationHandler {
    fn notify(
        executor: &Executor, subscriber: &Client, result: pubsub::Result,
    ) {
        executor.spawn(subscriber.notify(Ok(result)).map(|_| ()).map_err(
            |e| warn!(target: "rpc", "Unable to send notification: {}", e),
        ));
    }

    fn notify_headers(&self, headers: &[BlockHeader]) {
        for subscriber in self.header_subscribers.read().values() {
            let convert = |h| RpcHeader::new(h, &self.consensus);

            for h in headers.iter().map(convert) {
                Self::notify(
                    &self.executor,
                    subscriber,
                    pubsub::Result::Header(h),
                );
            }
        }
    }

    fn notify_pivots(&self, headers: &[BlockHeader]) {
        for subscriber in self.pivot_subscribers.read().values() {
            let convert = |h| RpcHeader::new(h, &self.consensus);

            for h in headers.iter().map(convert) {
                Self::notify(
                    &self.executor,
                    subscriber,
                    pubsub::Result::Header(h),
                );
            }
        }
    }

    fn notify_logs(&self, logs: &[LocalizedLogEntry]) {
        for (subscriber, filter) in self.log_subscribers.read().values() {
            let logs_filtered = logs
                .iter()
                .filter(|l| filter.matches(&l.entry))
                .cloned()
                .map(RpcLog::from);

            for log in logs_filtered {
                Self::notify(
                    &self.executor,
                    subscriber,
                    pubsub::Result::Log(log),
                );
            }
        }
    }
}

impl PubSub for PubSubClient {
    type Metadata = Metadata;

    fn subscribe(
        &self, _meta: Metadata, subscriber: Subscriber<pubsub::Result>,
        kind: pubsub::Kind, params: Option<pubsub::Params>,
    )
    {
        let error = match (kind, params) {
            // newHeads
            (pubsub::Kind::NewHeads, None) => {
                self.header_subscribers.write().push(subscriber);
                return;
            }
            (pubsub::Kind::NewHeads, _) => {
                errors::invalid_params("newHeads", "Expected no parameters.")
            }
            // pivotUpdates
            (pubsub::Kind::PivotUpdates, None) => {
                self.pivot_subscribers.write().push(subscriber);
                return;
            }
            (pubsub::Kind::PivotUpdates, _) => errors::invalid_params(
                "pivotUpdates",
                "Expected no parameters.",
            ),
            // logs
            (pubsub::Kind::Logs, Some(pubsub::Params::Logs(filter))) => {
                self.log_subscribers.write().push(subscriber, filter.into());
                return;
            }
            (pubsub::Kind::Logs, _) => errors::invalid_params(
                "pivotUpdates",
                "Expected filter parameter.",
            ),
            _ => errors::unimplemented(None),
        };

        let _ = subscriber.reject(error);
    }

    fn unsubscribe(
        &self, _: Option<Self::Metadata>, id: SubscriptionId,
    ) -> Result<bool> {
        let res0 = self.header_subscribers.write().remove(&id).is_some();
        let res1 = self.pivot_subscribers.write().remove(&id).is_some();
        let res2 = self.log_subscribers.write().remove(&id).is_some();

        Ok(res0 || res1 || res2)
    }
}
