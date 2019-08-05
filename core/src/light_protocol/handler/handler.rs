// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use io::TimerToken;
use rlp::Rlp;
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use crate::{
    consensus::ConsensusGraph,
    light_protocol::{
        handle_error,
        message::{msgid, Status},
        Error, ErrorKind,
    },
    message::MsgId,
    network::{NetworkContext, NetworkProtocolHandler, PeerId},
    sync::SynchronizationGraph,
};

use cfx_types::H256;

use super::{peers::Peers, query::QueryHandler, sync::SyncHandler};

const SYNC_TIMER: TimerToken = 0;
const REQUEST_CLEAN_UP_TIMER: TimerToken = 1;

const SYNC_PERIOD_MS: u64 = 5000;
const CLEANUP_PERIOD_MS: u64 = 1000;

/// Handler is responsible for maintaining peer meta-information and
/// dispatching messages to the query and sync sub-handlers.
pub struct Handler {
    consensus: Arc<ConsensusGraph>,
    pub peers: Arc<Peers>,
    pub query: QueryHandler,
    sync: SyncHandler,
}

impl Handler {
    pub fn new(
        consensus: Arc<ConsensusGraph>, graph: Arc<SynchronizationGraph>,
    ) -> Self {
        let peers = Arc::new(Peers::default());
        let next_request_id = Arc::new(AtomicU64::new(0));

        let query =
            QueryHandler::new(consensus.clone(), next_request_id.clone());

        let sync = SyncHandler::new(
            consensus.clone(),
            graph,
            next_request_id,
            peers.clone(),
        );

        Handler {
            consensus,
            peers,
            query,
            sync,
        }
    }

    #[rustfmt::skip]
    fn dispatch_message(
        &self, io: &NetworkContext, peer: PeerId, msg_id: MsgId, rlp: Rlp,
    ) -> Result<(), Error> {
        trace!("Dispatching message: peer={:?}, msg_id={:?}", peer, msg_id);

        match msg_id {
            msgid::STATUS => self.on_status(io, peer, &rlp),
            msgid::STATE_ROOT => self.query.on_state_root(io, peer, &rlp),
            msgid::STATE_ENTRY => self.query.on_state_entry(io, peer, &rlp),
            msgid::BLOCK_HASHES => self.sync.on_block_hashes(io, peer, &rlp),
            msgid::BLOCK_HEADERS => self.sync.on_block_headers(io, peer, &rlp),
            msgid::NEW_BLOCK_HASHES => self.sync.on_new_block_hashes(io, peer, &rlp),
            _ => Err(ErrorKind::UnknownMessage.into()),
        }
    }

    #[inline]
    fn validate_genesis_hash(&self, genesis: H256) -> Result<(), Error> {
        match self.consensus.data_man.true_genesis_block.hash() {
            h if h == genesis => Ok(()),
            h => {
                debug!(
                    "Genesis mismatch (ours: {:?}, theirs: {:?})",
                    h, genesis
                );
                Err(ErrorKind::GenesisMismatch.into())
            }
        }
    }

    fn on_status(
        &self, _io: &NetworkContext, peer: PeerId, rlp: &Rlp,
    ) -> Result<(), Error> {
        let status: Status = rlp.as_val()?;
        info!("on_status peer={:?} status={:?}", peer, status);

        self.validate_genesis_hash(status.genesis_hash)?;
        // TODO(thegaram): check protocol version

        let terminals = status.terminals.into_iter().collect();

        let peer_state = self.peers.insert(peer);
        let mut peer_state = peer_state.write();

        peer_state.protocol_version = status.protocol_version;
        peer_state.genesis_hash = status.genesis_hash;
        peer_state.best_epoch = status.best_epoch;
        peer_state.terminals = terminals;

        Ok(())
    }
}

impl NetworkProtocolHandler for Handler {
    fn initialize(&self, io: &NetworkContext) {
        let period = Duration::from_millis(SYNC_PERIOD_MS);
        io.register_timer(SYNC_TIMER, period)
            .expect("Error registering sync timer");

        let period = Duration::from_millis(CLEANUP_PERIOD_MS);
        io.register_timer(REQUEST_CLEAN_UP_TIMER, period)
            .expect("Error registering request housekeeping timer");
    }

    fn on_message(&self, io: &NetworkContext, peer: PeerId, raw: &[u8]) {
        if raw.len() < 2 {
            return handle_error(
                io,
                peer,
                msgid::INVALID,
                ErrorKind::InvalidMessageFormat.into(),
            );
        }

        let msg_id = raw[0];
        let rlp = Rlp::new(&raw[1..]);
        debug!("on_message: peer={:?}, msgid={:?}", peer, msg_id);

        if let Err(e) = self.dispatch_message(io, peer, msg_id.into(), rlp) {
            handle_error(io, peer, msg_id.into(), e);
        }
    }

    fn on_peer_connected(&self, _io: &NetworkContext, peer: PeerId) {
        info!("on_peer_connected: peer={:?}", peer);
        self.peers.insert(peer);
    }

    fn on_peer_disconnected(&self, _io: &NetworkContext, peer: PeerId) {
        info!("on_peer_disconnected: peer={:?}", peer);
        self.peers.remove(&peer);
    }

    fn on_timeout(&self, io: &NetworkContext, timer: TimerToken) {
        trace!("Timeout: timer={:?}", timer);
        match timer {
            SYNC_TIMER => {
                if let Err(e) = self.sync.start_sync(io) {
                    warn!("Failed to trigger sync: {:?}", e);
                }
            }
            REQUEST_CLEAN_UP_TIMER => {
                self.sync.clean_up_requests();
            }
            // TODO(thegaram): add other timers (e.g. data_man gc)
            _ => warn!("Unknown timer {} triggered.", timer),
        }
    }
}
