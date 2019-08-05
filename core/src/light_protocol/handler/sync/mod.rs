// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

mod requests;

use rlp::Rlp;
use std::{
    cmp,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use cfx_types::H256;

use crate::{
    consensus::ConsensusGraph,
    light_protocol::{
        message::{
            BlockHashes as GetBlockHashesResponse,
            BlockHeaders as GetBlockHeadersResponse, GetBlockHashesByEpoch,
            GetBlockHeaders, NewBlockHashes,
        },
        Error,
    },
    message::{Message, RequestId},
    network::{NetworkContext, PeerId},
    primitives::BlockHeader,
    sync::SynchronizationGraph,
};

use super::peers::Peers;
use requests::{HashSource, Requests};

const CATCH_UP_EPOCH_LAG_THRESHOLD: u64 = 3;
const EPOCH_REQUEST_CHUNK_SIZE: usize = 10;
const HEADER_REQUEST_CHUNK_SIZE: usize = 10;
const NUM_EPOCHS_TO_REQUEST: usize = 100;
const NUM_WAITING_HEADERS_THRESHOLD: usize = 300;

#[derive(Debug)]
struct Statistics {
    catch_up_mode: bool,
    duplicate_count: u64,
    in_flight: usize,
    latest_epoch: u64,
    waiting: usize,
}

pub(super) struct SyncHandler {
    consensus: Arc<ConsensusGraph>,
    duplicate_count: AtomicU64,
    graph: Arc<SynchronizationGraph>,
    latest_epoch_requested: AtomicU64,
    next_request_id: Arc<AtomicU64>,
    peers: Arc<Peers>,
    requests: Requests,
}

impl SyncHandler {
    pub(super) fn new(
        consensus: Arc<ConsensusGraph>, graph: Arc<SynchronizationGraph>,
        next_request_id: Arc<AtomicU64>, peers: Arc<Peers>,
    ) -> Self
    {
        graph.recover_graph_from_db(true /* header_only */);

        let requests = Requests::new(graph.clone());

        SyncHandler {
            consensus,
            duplicate_count: AtomicU64::new(0),
            graph,
            latest_epoch_requested: AtomicU64::new(0),
            next_request_id,
            peers,
            requests,
        }
    }

    #[inline]
    fn next_request_id(&self) -> RequestId {
        self.next_request_id.fetch_add(1, Ordering::Relaxed).into()
    }

    #[inline]
    fn catch_up_mode(&self) -> bool {
        match self.peers.median_epoch() {
            None => true,
            Some(epoch) => {
                let my_epoch = self.consensus.best_epoch_number();
                my_epoch < epoch - CATCH_UP_EPOCH_LAG_THRESHOLD
            }
        }
    }

    #[inline]
    fn get_statistics(&self) -> Statistics {
        Statistics {
            catch_up_mode: self.catch_up_mode(),
            duplicate_count: self.duplicate_count.load(Ordering::Relaxed),
            in_flight: self.requests.num_in_flight(),
            latest_epoch: self.consensus.best_epoch_number(),
            waiting: self.requests.num_waiting(),
        }
    }

    #[inline]
    fn collect_terminals(&self) {
        let terminals = self.peers.collect_all_terminals();
        self.requests.insert_waiting(terminals, HashSource::NewHash);
    }

    #[inline]
    fn request_epochs(
        &self, io: &NetworkContext, peer: PeerId, epochs: Vec<u64>,
    ) -> Result<(), Error> {
        info!("request_epochs peer={:?} epochs={:?}", peer, epochs);

        if epochs.is_empty() {
            return Ok(());
        }

        let msg: Box<dyn Message> = Box::new(GetBlockHashesByEpoch {
            request_id: self.next_request_id(),
            epochs,
        });

        msg.send(io, peer)?;
        Ok(())
    }

    #[inline]
    fn request_headers(
        &self, io: &NetworkContext, peer: PeerId, hashes: Vec<H256>,
    ) -> Result<(), Error> {
        info!("request_headers peer={:?} hashes={:?}", peer, hashes);

        if hashes.is_empty() {
            return Ok(());
        }

        let msg: Box<dyn Message> = Box::new(GetBlockHeaders {
            request_id: self.next_request_id(),
            hashes,
        });

        msg.send(io, peer)?;
        Ok(())
    }

    fn handle_headers(&self, headers: Vec<BlockHeader>) {
        let mut to_request = vec![];

        // TODO(thegaram): validate header timestamps
        for header in headers {
            let hash = header.hash();
            self.requests.remove_in_flight(&hash);

            if self.graph.contains_block_header(&hash) {
                self.duplicate_count.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // insert into graph
            let (valid, _) = self.graph.insert_block_header(
                &mut header.clone(),
                true,  /* need_to_verify */
                false, /* bench_mode */
                true,  /* insert_to_consensus */
                true,  /* persistent */
            );

            if !valid {
                continue;
            }

            // store missing dependencies
            to_request.push(*header.parent_hash());

            for referee in header.referee_hashes() {
                to_request.push(*referee);
            }
        }

        self.requests
            .insert_waiting(to_request, HashSource::Reference);
    }

    fn sync_headers(&self, io: &NetworkContext) -> Result<(), Error> {
        let headers = self.requests.collect_headers_to_request();

        for chunk in headers.chunks(HEADER_REQUEST_CHUNK_SIZE) {
            let peer = match self.peers.random_peer() {
                Some(peer) => peer,
                None => {
                    warn!("No peers available");
                    self.requests.reinsert_waiting(chunk.to_vec());

                    // NOTE: cannot do early return as that way requests
                    // in subsequent chunks would be lost
                    continue;
                }
            };

            let hashes = chunk.iter().map(|h| h.hash.clone()).collect();

            match self.request_headers(io, peer, hashes) {
                Ok(_) => {
                    self.requests.insert_in_flight(chunk.to_vec());
                }
                Err(e) => {
                    warn!(
                        "Failed to request headers {:?} from peer {:?}: {:?}",
                        chunk, peer, e
                    );

                    self.requests.reinsert_waiting(chunk.to_vec());
                }
            }
        }

        Ok(())
    }

    fn choose_epochs_to_request(&self) -> Vec<u64> {
        let my_best = self.consensus.best_epoch_number();
        let requested = self.latest_epoch_requested.load(Ordering::Relaxed);
        let start_from = cmp::max(my_best, requested) + 1;
        let peer_best = self.peers.best_epoch();

        (start_from..peer_best)
            .take(NUM_EPOCHS_TO_REQUEST)
            .collect()
    }

    fn sync_epochs(&self, io: &NetworkContext) -> Result<(), Error> {
        if self.requests.num_waiting() >= NUM_WAITING_HEADERS_THRESHOLD {
            return Ok(());
        }

        let epochs = self.choose_epochs_to_request();

        for chunk in epochs.chunks(EPOCH_REQUEST_CHUNK_SIZE) {
            let last = chunk.last().cloned().expect("chunk is not empty");

            let peer = match self.peers.random_peer_with_epoch(last) {
                Some(peer) => peer,
                None => {
                    warn!("No peers available; aborting sync");
                    break;
                }
            };

            match self.request_epochs(io, peer, chunk.to_vec()) {
                Ok(_) => {
                    self.latest_epoch_requested.store(last, Ordering::Relaxed)
                }
                Err(e) => {
                    warn!(
                        "Failed to request epochs {:?} from peer {:?}: {:?}",
                        chunk, peer, e
                    );
                }
            }
        }

        Ok(())
    }

    pub(super) fn on_block_hashes(
        &self, io: &NetworkContext, _peer: PeerId, rlp: &Rlp,
    ) -> Result<(), Error> {
        let resp: GetBlockHashesResponse = rlp.as_val()?;
        info!("on_block_hashes resp={:?}", resp);

        self.requests.insert_waiting(resp.hashes, HashSource::Epoch);

        self.start_sync(io)?;
        Ok(())
    }

    pub(super) fn on_block_headers(
        &self, io: &NetworkContext, _peer: PeerId, rlp: &Rlp,
    ) -> Result<(), Error> {
        let resp: GetBlockHeadersResponse = rlp.as_val()?;
        info!("on_block_headers resp={:?}", resp);

        self.handle_headers(resp.headers);

        self.start_sync(io)?;
        Ok(())
    }

    pub(super) fn on_new_block_hashes(
        &self, _io: &NetworkContext, peer: PeerId, rlp: &Rlp,
    ) -> Result<(), Error> {
        let msg: NewBlockHashes = rlp.as_val()?;
        info!("on_new_block_hashes msg={:?}", msg);

        if self.catch_up_mode() {
            if let Some(state) = self.peers.get(&peer) {
                let mut state = state.write();
                state.terminals.extend(msg.hashes);
            }
            return Ok(());
        }

        self.requests
            .insert_waiting(msg.hashes, HashSource::NewHash);

        Ok(())
    }

    pub(super) fn start_sync(&self, io: &NetworkContext) -> Result<(), Error> {
        info!("start_sync; statistics: {:?}", self.get_statistics());

        match self.catch_up_mode() {
            true => {
                self.sync_headers(io)?;
                self.sync_epochs(io)?;
            }
            false => {
                self.collect_terminals();
                self.sync_headers(io)?;
            }
        };

        Ok(())
    }

    pub(super) fn clean_up_requests(&self) { self.requests.clean_up(); }
}
