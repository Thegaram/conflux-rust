// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use parking_lot::RwLock;
use std::{
    cmp,
    collections::{BinaryHeap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::sync::SynchronizationGraph;
use cfx_types::H256;

const MAX_HEADERS_IN_FLIGHT: usize = 50;
const REQUEST_TIMEOUT_MS: u64 = 2000;

// order defined priority: Epoch < Reference < NewHash
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum HashSource {
    Epoch,     // hash received through an epoch request
    Reference, // hash referenced by a header we received
    NewHash,   // hash received through a new hashes announcement
}

#[derive(Clone, Debug, Eq)]
pub(super) struct MissingHeader {
    pub hash: H256,
    pub source: HashSource,
    pub since: Instant,
}

impl PartialEq for MissingHeader {
    fn eq(&self, other: &Self) -> bool { self.hash == other.hash }
}

// MissingHeader::cmp is used for prioritizing header requests
impl Ord for MissingHeader {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        if self.eq(other) {
            return cmp::Ordering::Equal;
        }

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

#[derive(Debug)]
struct HeaderRequest {
    pub header: MissingHeader,
    pub sent_at: Instant,
}

impl HeaderRequest {
    pub fn new(header: MissingHeader) -> Self {
        HeaderRequest {
            header,
            sent_at: Instant::now(),
        }
    }
}

pub(super) struct Requests {
    // shared synchronization graph
    graph: Arc<SynchronizationGraph>,

    // headers requested but not received yet
    in_flight: RwLock<HashMap<H256, HeaderRequest>>,

    // headers we need to request excluding
    // the ones in `in_flight`
    waiting: RwLock<BinaryHeap<MissingHeader>>,
}

impl Requests {
    pub fn new(graph: Arc<SynchronizationGraph>) -> Self {
        Requests {
            graph,
            in_flight: RwLock::new(HashMap::new()),
            waiting: RwLock::new(BinaryHeap::new()),
        }
    }

    pub fn num_waiting(&self) -> usize { self.waiting.read().len() }

    pub fn num_in_flight(&self) -> usize { self.in_flight.read().len() }

    pub fn insert_in_flight(&self, missing: Vec<MissingHeader>) {
        let new = missing
            .into_iter()
            .map(|h| (h.hash.clone(), HeaderRequest::new(h)));
        self.in_flight.write().extend(new);
    }

    pub fn remove_in_flight(&self, hash: &H256) {
        self.in_flight.write().remove(&hash);
    }

    pub fn insert_waiting(&self, hashes: Vec<H256>, source: HashSource) {
        let in_flight = self.in_flight.read();
        let mut waiting = self.waiting.write();

        let ws = hashes
            .into_iter()
            .filter(|h| !in_flight.contains_key(&h))
            .filter(|h| !self.graph.contains_block_header(&h))
            .map(|hash| MissingHeader {
                hash,
                source: source.clone(),
                since: Instant::now(),
            });

        waiting.extend(ws);
    }

    pub fn reinsert_waiting(&self, missing: Vec<MissingHeader>) {
        self.waiting.write().extend(missing);
    }

    pub fn collect_headers_to_request(&self) -> Vec<MissingHeader> {
        let in_flight = self.in_flight.read();
        let mut waiting = self.waiting.write();

        let num_to_request = MAX_HEADERS_IN_FLIGHT - in_flight.len();
        let mut headers = vec![];

        if num_to_request == 0 {
            return headers;
        }

        // NOTE: cannot use iterator on BinaryHeap as
        // it returns elements in arbitrary order!
        while let Some(h) = waiting.pop() {
            if !in_flight.contains_key(&h.hash)
                && !self.graph.contains_block_header(&h.hash)
            {
                headers.push(h);
            }

            if headers.len() == num_to_request {
                break;
            }
        }

        headers
    }

    pub fn clean_up(&self) {
        let mut in_flight = self.in_flight.write();
        let mut waiting = self.waiting.write();

        let now = Instant::now();
        let timeout = Duration::from_millis(REQUEST_TIMEOUT_MS);

        // collect timed-out requests
        let reqs: Vec<_> = in_flight
            .iter()
            .filter_map(|(_hash, req)| match req.sent_at {
                t if now.duration_since(t) < timeout => None,
                _ => Some(req.header.clone()),
            })
            .collect();

        // remove requests from `in_flight`
        for req in &reqs {
            in_flight.remove(&req.hash);
        }

        // reinsert missing requests
        let missing = reqs
            .into_iter()
            .filter(|h| !self.graph.contains_block_header(&h.hash));

        waiting.extend(missing);
    }
}
