// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use super::common::{HasKey, SyncManager};
use crate::{
    light_protocol::{
        common::{FullPeerState, Peers},
        message::{msgid, GetBlockHeaders},
        Error, ErrorKind, LightNodeConfiguration,
    },
    message::{Message, RequestId},
    sync::SynchronizationGraph,
    UniqueId,
};
use cfx_parameters::light::{
    HEADER_REQUEST_BATCH_SIZE, HEADER_REQUEST_TIMEOUT, MAX_HEADERS_IN_FLIGHT,
};
use cfx_types::H256;
use network::{node_table::NodeId, NetworkContext};
use primitives::BlockHeader;
use std::{
    cmp,
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

#[derive(Debug)]
struct Statistics {
    in_flight: usize,
    waiting: usize,
    inserted: u64,
    duplicate: u64,
    unexpected: u64,
    timeout: u64,
}

#[derive(Debug)]
struct PeerStats {
    num_requests: usize,
    timeout_requests: usize,
    timeout_items: usize,
    connected: bool,
}

impl Default for PeerStats {
    fn default() -> PeerStats {
        PeerStats {
            num_requests: 0,
            timeout_requests: 0,
            timeout_items: 0,
            connected: true,
        }
    }
}

// NOTE: order defines priority: Epoch < Reference < NewHash
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HashSource {
    Epoch,      // hash received through an epoch request
    Dependency, // hash referenced by a header we received
    NewHash,    // hash received through a new hashes announcement
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MissingHeader {
    pub hash: H256,
    pub since: Instant,
    pub source: HashSource,
}

impl MissingHeader {
    pub fn new(hash: H256, source: HashSource) -> Self {
        MissingHeader {
            hash,
            since: Instant::now(),
            source,
        }
    }
}

// MissingHeader::cmp is used for prioritizing header requests
impl Ord for MissingHeader {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let cmp_source = self.source.cmp(&other.source);
        let cmp_since = self.since.cmp(&other.since).reverse();
        let cmp_hash = self.hash.cmp(&other.hash);
        cmp_source.then(cmp_since).then(cmp_hash)
    }
}

impl PartialOrd for MissingHeader {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl HasKey<H256> for MissingHeader {
    fn key(&self) -> H256 { self.hash }
}

pub struct Headers {
    // light node configuration
    config: LightNodeConfiguration,

    // number of headers received multiple times
    duplicate_count: AtomicU64,

    // shared synchronization graph
    graph: Arc<SynchronizationGraph>,

    // number of headers inserted into the sync graph
    pub inserted_count: AtomicU64,

    // series of unique request ids
    request_id_allocator: Arc<UniqueId>,

    // sync and request manager
    sync_manager: SyncManager<H256, MissingHeader>,

    // number of timeout header requests
    timeout_count: AtomicU64,

    // number of unexpected headers received
    // these are mostly responses for timeout requests
    unexpected_count: AtomicU64,





    peers: Arc<Peers<FullPeerState>>,

    peer_stats: parking_lot::RwLock<std::collections::HashMap<NodeId, PeerStats>>,
}

impl Headers {
    pub fn new(
        graph: Arc<SynchronizationGraph>, peers: Arc<Peers<FullPeerState>>,
        request_id_allocator: Arc<UniqueId>, config: LightNodeConfiguration,
    ) -> Self
    {
        let duplicate_count = AtomicU64::new(0);
        let inserted_count = AtomicU64::new(0);
        let sync_manager =
            SyncManager::new(peers.clone(), msgid::GET_BLOCK_HEADERS);
        let timeout_count = AtomicU64::new(0);
        let unexpected_count = AtomicU64::new(0);

        Headers {
            config,
            duplicate_count,
            graph,
            inserted_count,
            request_id_allocator,
            sync_manager,
            timeout_count,
            unexpected_count,
            peers,
            peer_stats: Default::default(),
        }
    }

    #[inline]
    pub fn num_waiting(&self) -> usize { self.sync_manager.num_waiting() }

    #[inline]
    pub fn print_stats(&self) {
        debug!(
            "header sync statistics: {:?}",
            Statistics {
                in_flight: self.sync_manager.num_in_flight(),
                waiting: self.sync_manager.num_waiting(),
                inserted: self.inserted_count.load(Ordering::Relaxed),
                duplicate: self.duplicate_count.load(Ordering::Relaxed),
                unexpected: self.unexpected_count.load(Ordering::Relaxed),
                timeout: self.timeout_count.load(Ordering::Relaxed),
            }
        );

        debug!("peer statistics: {:?}", self.peer_stats.read());
    }

    #[inline]
    pub fn request<I>(&self, hashes: I, source: HashSource)
    where I: Iterator<Item = H256> {
        let headers = hashes
            .filter(|h| !self.graph.contains_block_header(&h))
            .map(|h| MissingHeader::new(h, source.clone()));

        self.sync_manager.insert_waiting(headers);
    }

    #[inline]
    pub fn request_now_from_peer<I>(
        &self, io: &dyn NetworkContext, peer: &NodeId, hashes: I,
        source: HashSource,
    ) where
        I: Iterator<Item = H256>,
    {
        let hashes: Vec<_> = hashes
            .filter(|h| !self.graph.contains_block_header(&h))
            .collect();

        let headers = hashes
            .iter()
            .cloned()
            .map(|h| MissingHeader::new(h, source.clone()));

        self.sync_manager.request_now_from_peer(
            headers,
            peer,
            |peer, hashes| self.send_request(io, peer, hashes),
        );
    }

    pub fn receive(
        &self, peer: &NodeId, id: RequestId,
        headers: impl Iterator<Item = BlockHeader>,
    ) -> Result<(), Error>
    {
        let mut missing = HashSet::new();
        let mut has_invalid_header = false;

        let mut coordinator = match self.sync_manager.receive(peer, id)? {
            None => return Ok(()),
            Some(c) => c,
        };

        // TODO(thegaram): validate header timestamps
        for header in headers {
            let hash = header.hash();

            if !coordinator.should_process_item(&hash) {
                trace!("Received unexpected header: {:?}", hash);
                self.unexpected_count.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // signal that we do not need to re-request this header
            coordinator.item_processed(&hash);

            // check duplicates
            if self.graph.contains_block_header(&hash) {
                self.duplicate_count.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // insert into graph
            let (insert_result, _) = self.graph.insert_block_header(
                &mut header.clone(),
                true,  /* need_to_verify */
                false, /* bench_mode */
                true,  /* insert_to_consensus */
                true,  /* persistent */
            );

            if insert_result.is_invalid() {
                debug!(
                    "Received invalid header {:?} from peer {:?}",
                    hash, peer
                );
                has_invalid_header = true;
                continue;
            }

            // the header is likely to be new as we checked this before, but we
            // still want to avoid unnecessarily re-requesting its ancestors
            if !insert_result.is_new_valid() {
                continue;
            }

            self.inserted_count.fetch_add(1, Ordering::Relaxed);

            // store missing dependencies
            missing.insert(*header.parent_hash());

            for referee in header.referee_hashes() {
                missing.insert(*referee);
            }
        }

        let missing = missing.into_iter();
        self.request(missing, HashSource::Dependency);

        // disconnect peers who send invalid headers
        if has_invalid_header {
            bail!(ErrorKind::InvalidHeader);
        }

        Ok(())
    }

    #[inline]
    pub fn clean_up(&self, io: &dyn NetworkContext) {
        // remove timeout in-flight requests
        let timeout = self
            .config
            .header_request_timeout
            .unwrap_or(*HEADER_REQUEST_TIMEOUT);

        let reqs = self.sync_manager.remove_timeout_requests(timeout);
        trace!("Timeout header requests ({}): {:?}", reqs.len(), reqs);

        let mut stats = self.peer_stats.write();
        let mut peers_to_disconnect = HashSet::new();

        for req in &reqs {
            let mut s = stats.entry(req.peer).or_default();
            s.timeout_requests += 1;
            s.timeout_items += req.items.len();

            if let Some(peer_state) = self.peers.get(&req.peer) {
                if peer_state.write().on_timeout_should_disconnect() {
                    peers_to_disconnect.insert(req.peer);
                    s.connected = false;
                }
            }
        }

        for peer_id in peers_to_disconnect {
            // Note `self.peers` will be used in `disconnect_peer`, so we must
            // call it without locking `self.peers`.
            info!("!!!!!!!!! disconnecting peer: {:?}", peer_id);

            io.disconnect_peer(
                &peer_id,
                Some(network::UpdateNodeOperation::Failure),
                "too many timeout requests", /* reason */
            );
        }

        drop(stats);

        let items = reqs
            .into_iter()
            .map(|r| r.items.into_iter())
            .flatten()
            .collect::<Vec<_>>();

        self.timeout_count
            .fetch_add(items.len() as u64, Ordering::Relaxed);

        // re-request
        self.sync_manager.insert_waiting(items.into_iter());
    }

    #[inline]
    fn send_request(
        &self, io: &dyn NetworkContext, peer: &NodeId, hashes: Vec<H256>,
    ) -> Result<Option<RequestId>, Error> {
        if hashes.is_empty() {
            return Ok(None);
        }

        let request_id = self.request_id_allocator.next();

        trace!(
            "send_request GetBlockHeaders peer={:?} id={:?} hashes={:?}",
            peer,
            request_id,
            hashes
        );

        let msg: Box<dyn Message> =
            Box::new(GetBlockHeaders { request_id, hashes });

        msg.send(io, peer)?;

        self.peer_stats.write().entry(*peer).or_default().num_requests += 1;

        Ok(Some(request_id))
    }

    #[inline]
    pub fn sync(&self, io: &dyn NetworkContext) {
        let max_in_flight = self
            .config
            .max_headers_in_flight
            .unwrap_or(MAX_HEADERS_IN_FLIGHT);

        let batch_size = self
            .config
            .header_request_batch_size
            .unwrap_or(HEADER_REQUEST_BATCH_SIZE);

        self.sync_manager
            .sync(max_in_flight, batch_size, |peer, hashes| {
                self.send_request(io, peer, hashes)
            });
    }
}

#[cfg(test)]
mod tests {
    use super::{super::common::PriorityQueue, HashSource, MissingHeader};
    use cfx_types::H256;
    use rand::prelude::SliceRandom;
    use std::{
        ops::Sub,
        time::{Duration, Instant},
    };

    #[test]
    fn test_ordering() {
        assert!(HashSource::Epoch < HashSource::Dependency);
        assert!(HashSource::Dependency < HashSource::NewHash);

        let now = Instant::now();
        let one_ms_ago = now.sub(Duration::from_millis(1));

        let h0 = MissingHeader {
            hash: H256::from_low_u64_be(0),
            since: now,
            source: HashSource::Epoch,
        };

        let h1 = MissingHeader {
            hash: H256::from_low_u64_be(1),
            since: one_ms_ago,
            source: HashSource::Epoch,
        };

        assert!(h0 < h1); // longer waiting time

        let h2 = MissingHeader {
            hash: H256::from_low_u64_be(2),
            since: now,
            source: HashSource::Dependency,
        };

        assert!(h1 < h2); // higher source priority

        let h3 = MissingHeader {
            hash: H256::from_low_u64_be(3),
            since: one_ms_ago,
            source: HashSource::Dependency,
        };

        assert!(h2 < h3); // longer waiting time

        let h4 = MissingHeader {
            hash: H256::from_low_u64_be(4),
            since: now,
            source: HashSource::NewHash,
        };

        assert!(h3 < h4); // higher source priority

        let h5 = MissingHeader {
            hash: H256::from_low_u64_be(5),
            since: one_ms_ago,
            source: HashSource::NewHash,
        };

        assert!(h4 < h5); // longer waiting time

        let h6 = MissingHeader {
            hash: H256::from_low_u64_be(6),
            since: now,
            source: HashSource::NewHash,
        };

        assert!(h4 < h6); // hash order
    }

    fn assert_deep_equal(h1: Option<MissingHeader>, h2: Option<MissingHeader>) {
        // MissingHeader::eq only considers the hash; here we check all fields
        assert_eq!(h1.clone().map(|h| h.hash), h2.clone().map(|h| h.hash));
        assert_eq!(h1.clone().map(|h| h.since), h2.clone().map(|h| h.since));
        assert_eq!(h1.clone().map(|h| h.source), h2.clone().map(|h| h.source));
    }

    #[test]
    fn test_queue() {
        let now = Instant::now();
        let one_ms_ago = now.sub(Duration::from_millis(1));

        let h0 = MissingHeader {
            hash: H256::from_low_u64_be(0),
            since: now,
            source: HashSource::Epoch,
        };

        let h1 = MissingHeader {
            hash: H256::from_low_u64_be(1),
            since: one_ms_ago,
            source: HashSource::Epoch,
        };

        let h2 = MissingHeader {
            hash: H256::from_low_u64_be(2),
            since: now,
            source: HashSource::Dependency,
        };

        let h3 = MissingHeader {
            hash: H256::from_low_u64_be(3),
            since: one_ms_ago,
            source: HashSource::Dependency,
        };

        let h4 = MissingHeader {
            hash: H256::from_low_u64_be(4),
            since: now,
            source: HashSource::NewHash,
        };

        let h5 = MissingHeader {
            hash: H256::from_low_u64_be(5),
            since: one_ms_ago,
            source: HashSource::NewHash,
        };

        let h6 = MissingHeader {
            hash: H256::from_low_u64_be(5),
            since: one_ms_ago,
            source: HashSource::NewHash,
        };

        let mut headers = vec![];

        headers.push(h0.clone());
        headers.push(h1.clone());
        headers.push(h2.clone());
        headers.push(h3.clone());
        headers.push(h4.clone());
        headers.push(h5.clone());
        headers.push(h6.clone());

        headers.shuffle(&mut rand::thread_rng());
        let mut queue = PriorityQueue::new();
        queue.extend(headers);

        assert_deep_equal(queue.pop(), Some(h5));
        assert_deep_equal(queue.pop(), Some(h4));
        assert_deep_equal(queue.pop(), Some(h3));
        assert_deep_equal(queue.pop(), Some(h2));
        assert_deep_equal(queue.pop(), Some(h1));
        assert_deep_equal(queue.pop(), Some(h0));
        assert_deep_equal(queue.pop(), None);
    }
}
