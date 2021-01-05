// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use super::{HasKey, PriorityQueue};
use crate::{
    light_protocol::{
        common::{FullPeerFilter, FullPeerState, Peers},
        Error, ErrorKind,
    },
    message::{MsgId, RequestId},
};
use network::node_table::NodeId;
use parking_lot::{Mutex, RwLock};
use std::{
    cmp::Ord,
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};
use throttling::token_bucket::ThrottleResult;

// define a "trait aliases"
// see: https://www.worthe-it.co.za/blog/2017-01-15-aliasing-traits-in-rust.html
pub trait KeyT: Clone + Eq + Hash {}
impl<T> KeyT for T where T: Clone + Eq + Hash {}

pub trait ItemT<K: KeyT>: Debug + Clone + HasKey<K> + Ord {}
impl<T, K: KeyT> ItemT<K> for T where T: Debug + Clone + HasKey<K> + Ord {}

pub struct InFlightRequest<T> {
    items: Vec<T>,
    id: RequestId,
    peer: NodeId,
    sent_at: Instant,
}

impl<T> Default for InFlightRequest<T> {
    fn default() -> Self {
        InFlightRequest {
            items: vec![],
            ..Default::default()
        }
    }
}

impl<T> InFlightRequest<T> {
    pub fn new(items: Vec<T>, id: RequestId, peer: NodeId) -> Self {
        InFlightRequest {
            items,
            id,
            peer,
            sent_at: Instant::now(),
        }
    }
}

// note: we need to specify trait bounds here,
// otherwise we cannot implement Drop
pub struct Coordinator<'a, K: KeyT, I: ItemT<K>> {
    sync_manager: &'a SyncManager<K, I>,
    req: InFlightRequest<I>,
    to_process: HashSet<K>,
}

impl<'a, K: KeyT, I: ItemT<K>> Coordinator<'a, K, I> {
    fn new(sm: &'a SyncManager<K, I>, req: InFlightRequest<I>) -> Self {
        let to_process = req.items.iter().map(|item| item.key()).collect();

        Coordinator {
            sync_manager: sm,
            req,
            to_process,
        }
    }

    pub fn should_process_item(&mut self, key: &K) -> bool {
        self.to_process.contains(key)
    }

    pub fn item_processed(&mut self, key: &K) { self.to_process.remove(&key); }
}

impl<'a, K: KeyT, I: ItemT<K>> Drop for Coordinator<'a, K, I> {
    fn drop(&mut self) {
        let mut req = Default::default();
        std::mem::swap(&mut req, &mut self.req);

        let to_rerequest = req
            .items
            .into_iter()
            .filter(|item| self.to_process.contains(&item.key()));

        self.sync_manager.insert_waiting(to_rerequest)
    }
}

struct InFlightRequestManager<Key, Item> {
    requests: HashMap<RequestId, InFlightRequest<Item>>,
    keys: HashSet<Key>,
}

impl<Key, Item> Default for InFlightRequestManager<Key, Item> {
    fn default() -> Self {
        Self {
            requests: HashMap::new(),
            keys: HashSet::new(),
        }
    }
}

impl<Key: KeyT, Item: ItemT<Key>> InFlightRequestManager<Key, Item> {
    fn len(&self) -> usize { self.keys.len() }

    fn contains_key(&self, key: &Key) -> bool { self.keys.contains(key) }

    fn insert(&mut self, request: InFlightRequest<Item>) {
        for item in &request.items {
            if self.keys.contains(&item.key()) {
                // TODO
            }

            self.keys.insert(item.key());
        }

        self.requests.insert(request.id, request);
    }

    fn remove(&mut self, id: &RequestId) -> Option<InFlightRequest<Item>> {
        match self.requests.remove(id) {
            None => None,
            Some(req) => {
                for item in &req.items {
                    if !self.keys.remove(&item.key()) {
                        // TODO
                    }
                }

                Some(req)
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = &InFlightRequest<Item>> {
        self.requests.values()
    }
}

pub struct SyncManager<Key, Item> {
    // headers requested but not received yet
    in_flight: RwLock<InFlightRequestManager<Key, Item>>,

    // collection of all peers available
    peers: Arc<Peers<FullPeerState>>,

    // mutex used to make sure at most one thread drives sync at any given time
    sync_lock: Mutex<()>,

    // priority queue of headers we need excluding the ones in `in_flight`
    waiting: RwLock<PriorityQueue<Key, Item>>,

    // used to filter peer to send request
    request_msg_id: MsgId,
}

impl<Key: KeyT, Item: ItemT<Key>> SyncManager<Key, Item> {
    pub fn new(
        peers: Arc<Peers<FullPeerState>>, request_msg_id: MsgId,
    ) -> Self {
        let sync_lock = Default::default();
        let waiting = RwLock::new(PriorityQueue::new());

        SyncManager {
            in_flight: Default::default(),
            peers,
            sync_lock,
            waiting,
            request_msg_id,
        }
    }

    #[inline]
    pub fn num_waiting(&self) -> usize { self.waiting.read().len() }

    #[inline]
    pub fn num_in_flight(&self) -> usize { self.in_flight.read().len() }

    #[inline]
    #[allow(dead_code)]
    pub fn contains(&self, key: &Key) -> bool {
        self.in_flight.read().contains_key(key)
            || self.waiting.read().contains(&key)
    }

    #[inline]
    fn get_existing_peer_state(
        &self, peer: &NodeId,
    ) -> Result<Arc<RwLock<FullPeerState>>, Error> {
        match self.peers.get(peer) {
            Some(state) => Ok(state),
            None => {
                bail!(ErrorKind::InternalError(format!(
                    "Received message from unknown peer={:?}",
                    peer
                )));
            }
        }
    }

    #[inline]
    pub fn receive(
        &self, peer: &NodeId, request_id: RequestId,
    ) -> Result<Option<Coordinator<'_, Key, Item>>, Error> {
        // TODO: handle peer id
        if let Some(req) = self.in_flight.write().remove(&request_id) {
            return Ok(Some(Coordinator::new(&self, req)));
        }

        // request does not exist => throttle
        let peer = self.get_existing_peer_state(peer)?;
        let bucket_name = self.request_msg_id.to_string();

        let bucket = match peer.read().unexpected_msgs.get(&bucket_name) {
            Some(bucket) => bucket,
            None => return Ok(None),
        };

        let result = bucket.lock().throttle_default();

        match result {
            ThrottleResult::Success => Ok(None),
            ThrottleResult::Throttled(_) => Ok(None),
            ThrottleResult::AlreadyThrottled => todo!(),
        }
    }

    #[inline]
    pub fn remove_in_flight(&self, key: &Key) {
        // self.in_flight_old.write().remove(&key);
        unimplemented!()
    }

    #[inline]
    pub fn remove_in_flight_new(&self, id: &RequestId) {
        self.in_flight.write().remove(&id);
    }

    #[inline]
    pub fn insert_waiting<I>(&self, items: I)
    where I: Iterator<Item = Item> {
        let in_flight = self.in_flight.read();
        let mut waiting = self.waiting.write();
        let missing = items.filter(|item| !in_flight.contains_key(&item.key()));
        waiting.extend(missing);
    }

    pub fn sync(
        &self, max_in_flight: usize, batch_size: usize,
        request: impl Fn(&NodeId, Vec<Key>) -> Result<Option<RequestId>, Error>,
    )
    {
        let _guard = match self.sync_lock.try_lock() {
            None => return,
            Some(g) => g,
        };

        // check if there are any peers available
        if self.peers.is_empty() {
            debug!("No peers available; aborting sync");
            return;
        }

        loop {
            // unlock after each batch so that we do not block other threads
            let mut in_flight = self.in_flight.write();
            let mut waiting = self.waiting.write();

            // collect batch
            let max_to_request = max_in_flight.saturating_sub(in_flight.len());
            let num_to_request = std::cmp::min(max_to_request, batch_size);

            if num_to_request == 0 {
                return;
            }

            let mut batch: Vec<Item> = vec![];

            // NOTE: cannot use iterator on BinaryHeap as
            // it returns elements in arbitrary order!
            while let Some(item) = waiting.pop() {
                // skip occasional items already in flight
                // this can happen if an item is inserted using
                // `insert_waiting`, then inserted again using
                // `request_now`
                if !in_flight.contains_key(&item.key()) {
                    batch.push(item);
                }

                if batch.len() == num_to_request {
                    break;
                }
            }

            // we're finished when there's nothing more to request
            if batch.is_empty() {
                return;
            }

            // select peer for batch
            let peer = match FullPeerFilter::new(self.request_msg_id)
                .select(self.peers.clone())
            {
                Some(peer) => peer,
                None => {
                    warn!("No peers available");
                    waiting.extend(batch.to_owned().into_iter());
                    return;
                }
            };

            let keys = batch.iter().map(|h| h.key()).collect();

            match request(&peer, keys) {
                Ok(None) => {}
                Ok(Some(request_id)) => {
                    in_flight.insert(InFlightRequest::new(
                        batch.to_owned(),
                        request_id,
                        peer,
                    ));
                }
                Err(e) => {
                    warn!(
                        "Failed to request items {:?} from peer {:?}: {:?}",
                        batch, peer, e
                    );

                    waiting.extend(batch.to_owned().into_iter());
                }
            }
        }
    }

    #[inline]
    pub fn remove_timeout_requests(&self, timeout: Duration) -> Vec<Item> {
        // let mut in_flight = self.in_flight_old.write();

        // // collect timed-out requests
        // let items: Vec<_> = in_flight
        //     .iter()
        //     .filter_map(|(_hash, req)| match req.sent_at {
        //         t if t.elapsed() < timeout => None,
        //         _ => Some(req.item.clone()),
        //     })
        //     .collect();

        // // remove requests from `in_flight`
        // for item in &items {
        //     in_flight.remove(&item.key());
        // }

        // items
        unimplemented!()
    }

    #[inline]
    pub fn remove_timeout_requests_new(
        &self, timeout: Duration,
    ) -> Vec<InFlightRequest<Item>> {
        let mut in_flight = self.in_flight.write();

        // collect timed-out requests
        let ids: Vec<_> = in_flight
            .iter()
            .filter_map(|req| match req.sent_at {
                t if t.elapsed() < timeout => None,
                _ => Some(req.id),
            })
            .collect();

        // remove requests from `in_flight`
        ids.iter()
            .map(|id| in_flight.remove(id))
            .map(Option::unwrap) // guaranteed to exist
            .collect()
    }

    #[inline]
    pub fn request_now<I>(
        &self, items: I,
        request: impl Fn(&NodeId, Vec<Key>) -> Result<Option<RequestId>, Error>,
    ) where
        I: Iterator<Item = Item>,
    {
        let peer = match FullPeerFilter::new(self.request_msg_id)
            .select(self.peers.clone())
        {
            Some(peer) => peer,
            None => {
                warn!("No peers available");
                self.insert_waiting(items);
                return;
            }
        };

        self.request_now_from_peer(items, &peer, request);
    }

    #[inline]
    pub fn request_now_from_peer<I>(
        &self, items: I, peer: &NodeId,
        request: impl Fn(&NodeId, Vec<Key>) -> Result<Option<RequestId>, Error>,
    ) where
        I: Iterator<Item = Item>,
    {
        let mut in_flight = self.in_flight.write();

        let missing = items
            .filter(|item| !in_flight.contains_key(&item.key()))
            .collect::<Vec<_>>();

        let keys = missing.iter().map(|h| h.key()).collect();

        match request(peer, keys) {
            Ok(None) => {}
            Ok(Some(request_id)) => {
                in_flight
                    .insert(InFlightRequest::new(missing, request_id, *peer));
            }
            Err(e) => {
                warn!(
                    "Failed to request {:?} from {:?}: {:?}",
                    missing, peer, e
                );

                self.insert_waiting(missing.into_iter());
            }
        }
    }
}
