// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use std::{
    cmp,
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use primitives::Receipt;

extern crate futures;
use futures::{Async, Future, Poll};

use crate::{
    consensus::ConsensusGraph,
    light_protocol::{
        common::{Peers, UniqueId, Validate},
        handler::FullPeerState,
        message::{GetReceipts, ReceiptsWithEpoch},
        Error,
    },
    message::Message,
    network::{NetworkContext, PeerId},
    parameters::light::{
        BLOOM_REQUEST_TIMEOUT_MS, MAX_RECEIPTS_IN_FLIGHT,
        RECEIPT_REQUEST_BATCH_SIZE,
    },
};

use super::sync_manager::{HasKey, SyncManager};

#[derive(Debug)]
struct Statistics {
    in_flight: usize,
    verified: usize,
    waiting: usize,
}

#[derive(Clone, Debug, Eq)]
pub(super) struct MissingReceipts {
    pub epoch: u64,
    pub since: Instant,
}

impl MissingReceipts {
    pub fn new(epoch: u64) -> Self {
        MissingReceipts {
            epoch,
            since: Instant::now(),
        }
    }
}

impl PartialEq for MissingReceipts {
    fn eq(&self, other: &Self) -> bool { self.epoch == other.epoch }
}

// MissingReceipts::cmp is used for prioritizing bloom requests
impl Ord for MissingReceipts {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        if self.eq(other) {
            return cmp::Ordering::Equal;
        }

        let cmp_since = self.since.cmp(&other.since).reverse();
        let cmp_epoch = self.epoch.cmp(&other.epoch).reverse();

        cmp_since.then(cmp_epoch)
    }
}

impl PartialOrd for MissingReceipts {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl HasKey<u64> for MissingReceipts {
    fn key(&self) -> u64 { self.epoch }
}

pub struct ReceiptsFuture {
    epoch: u64,
    verified: Arc<RwLock<HashMap<u64, Vec<Vec<Receipt>>>>>,
}

impl ReceiptsFuture {
    pub fn new(
        epoch: u64, verified: Arc<RwLock<HashMap<u64, Vec<Vec<Receipt>>>>>,
    ) -> ReceiptsFuture {
        ReceiptsFuture { epoch, verified }
    }
}

impl Future for ReceiptsFuture {
    type Error = Error;
    type Item = Vec<Vec<Receipt>>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.verified.read().get(&self.epoch) {
            None => Ok(Async::NotReady),
            Some(receipts) => Ok(Async::Ready(receipts.clone())),
        }
    }
}

pub struct Receipts {
    // series of unique request ids
    request_id_allocator: Arc<UniqueId>,

    // sync and request manager
    sync_manager: SyncManager<u64, MissingReceipts>,

    // helper API for validating ledger and state information
    validate: Validate,

    // epoch receipts received from full node
    verified: Arc<RwLock<HashMap<u64, Vec<Vec<Receipt>>>>>,
}

impl Receipts {
    pub(super) fn new(
        consensus: Arc<ConsensusGraph>, peers: Arc<Peers<FullPeerState>>,
        request_id_allocator: Arc<UniqueId>,
    ) -> Self
    {
        let sync_manager = SyncManager::new(peers.clone());
        let validate = Validate::new(consensus.clone());
        let verified = Arc::new(RwLock::new(HashMap::new()));

        verified.write().insert(0, vec![]);

        Receipts {
            request_id_allocator,
            sync_manager,
            validate,
            verified,
        }
    }

    #[inline]
    fn get_statistics(&self) -> Statistics {
        Statistics {
            in_flight: self.sync_manager.num_in_flight(),
            verified: self.verified.read().len(),
            waiting: self.sync_manager.num_waiting(),
        }
    }

    #[inline]
    pub fn request(
        &self, epoch: u64,
    ) -> impl Future<Item = Vec<Vec<Receipt>>, Error = Error> {
        if !self.verified.read().contains_key(&epoch) {
            self.sync_manager
                .insert_waiting(std::iter::once(MissingReceipts::new(epoch)));
        }

        ReceiptsFuture::new(epoch, self.verified.clone())
    }

    #[inline]
    pub(super) fn receive(
        &self, receipts: impl Iterator<Item = ReceiptsWithEpoch>,
    ) -> Result<(), Error> {
        for ReceiptsWithEpoch { epoch, receipts } in receipts {
            info!("Validating receipts {:?} with epoch {}", receipts, epoch);
            self.validate.receipts_with_local_info(epoch, &receipts)?;

            self.verified.write().insert(epoch, receipts);
            self.sync_manager.remove_in_flight(&epoch);
        }

        Ok(())
    }

    #[inline]
    pub(super) fn clean_up(&self) {
        let timeout = Duration::from_millis(BLOOM_REQUEST_TIMEOUT_MS);
        let receiptss = self.sync_manager.remove_timeout_requests(timeout);
        self.sync_manager.insert_waiting(receiptss.into_iter());
    }

    #[inline]
    fn send_request(
        &self, io: &dyn NetworkContext, peer: PeerId, epochs: Vec<u64>,
    ) -> Result<(), Error> {
        info!("send_request peer={:?} epochs={:?}", peer, epochs);

        if epochs.is_empty() {
            return Ok(());
        }

        let msg: Box<dyn Message> = Box::new(GetReceipts {
            request_id: self.request_id_allocator.next(),
            epochs,
        });

        msg.send(io, peer)?;
        Ok(())
    }

    #[inline]
    pub(super) fn sync(&self, io: &dyn NetworkContext) {
        info!("receipt sync statistics: {:?}", self.get_statistics());

        self.sync_manager.sync(
            MAX_RECEIPTS_IN_FLIGHT,
            RECEIPT_REQUEST_BATCH_SIZE,
            |peer, epochs| self.send_request(io, peer, epochs),
        );
    }
}
