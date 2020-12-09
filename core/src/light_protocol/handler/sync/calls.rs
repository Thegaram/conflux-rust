// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

extern crate lru_time_cache;

use lru_time_cache::LruCache;
use parking_lot::{Mutex, RwLock};
use std::{future::Future, sync::Arc};

use super::{
    common::{FutureItem, PendingItem, SyncManager, TimeOrdered},
    state_roots::StateRoots,
};
use crate::{
    consensus::{ConsensusGraph, SharedConsensusGraph},
    executive::ExecutionOutcome,
    light_protocol::{
        common::{FullPeerState, Peers},
        error::*,
        message::{
            msgid, CallContext, CallContextWithKey, CallKey, GetCallContexts,
        },
    },
    message::{Message, RequestId},
    UniqueId,
};
use cfx_parameters::light::{
    BLOOM_REQUEST_BATCH_SIZE, BLOOM_REQUEST_TIMEOUT, CACHE_TIMEOUT,
    MAX_BLOOMS_IN_FLIGHT,
};
use futures::future::FutureExt;
use network::{node_table::NodeId, NetworkContext};
use primitives::{SignedTransaction, StorageKey};

fn calculate_hash<T: std::hash::Hash>(t: &T) -> u64 {
    use std::hash::Hasher;
    let mut s = std::collections::hash_map::DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[derive(Debug)]
struct Statistics {
    cached: usize,
    in_flight: usize,
    waiting: usize,
}

#[derive(Clone)]
struct WrappedExecutionOutcome(Arc<Mutex<Option<ExecutionOutcome>>>);

impl WrappedExecutionOutcome {
    fn new(outcome: ExecutionOutcome) -> Self {
        Self(Arc::new(Mutex::new(Some(outcome))))
    }

    fn unwrap(self) -> ExecutionOutcome {
        self.0.lock().take().unwrap() // TODO
    }
}

type MissingCallResult = TimeOrdered<CallKey>;

type PendingCall = PendingItem<WrappedExecutionOutcome, ClonableError>;

pub struct Calls {
    // shared consensus graph
    consensus: SharedConsensusGraph,

    // series of unique request ids
    request_id_allocator: Arc<UniqueId>,

    // state_root sync manager
    state_roots: Arc<StateRoots>,

    // sync and request manager
    sync_manager: SyncManager<CallKey, MissingCallResult>,

    // bloom filters received from full node
    verified: Arc<RwLock<LruCache<CallKey, PendingCall>>>,
}

impl Calls {
    pub fn new(
        consensus: SharedConsensusGraph, peers: Arc<Peers<FullPeerState>>,
        state_roots: Arc<StateRoots>, request_id_allocator: Arc<UniqueId>,
    ) -> Self
    {
        let sync_manager = SyncManager::new(peers, msgid::GET_CALL_CONTEXTS);

        let cache = LruCache::with_expiry_duration(*CACHE_TIMEOUT);
        let verified = Arc::new(RwLock::new(cache));

        Calls {
            consensus,
            request_id_allocator,
            state_roots,
            sync_manager,
            verified,
        }
    }

    #[inline]
    pub fn print_stats(&self) {
        debug!(
            "call sync statistics: {:?}",
            Statistics {
                cached: self.verified.read().len(), // TODO: this is inaccurate
                in_flight: self.sync_manager.num_in_flight(),
                waiting: self.sync_manager.num_waiting(),
            }
        );
    }

    #[inline]
    pub fn request_now(
        &self, io: &dyn NetworkContext, tx: SignedTransaction, epoch: u64,
    ) -> impl Future<Output = Result<ExecutionOutcome>> {
        let mut verified = self.verified.write();
        let key = CallKey { tx, epoch };

        if !verified.contains_key(&key) {
            trace!(
                "!!!!!!!!! requesting now; tx hash = {:?}, key hash = {:?}!",
                key.tx.hash(),
                calculate_hash(&key)
            );
            let missing = std::iter::once(MissingCallResult::new(key.clone()));

            self.sync_manager.request_now(missing, |peer, keys| {
                self.send_request(io, peer, keys)
            });

            if self.sync_manager.contains(&key) {
                trace!("!!!!!!!!! requesting now; synx manager CONTAINS tx hash = {:?}, key hash = {:?}!", key.tx.hash(), calculate_hash(&key));
            } else {
                trace!("!!!!!!!!! requesting now; synx manager DOES NOT CONTAIN tx hash = {:?}, key hash = {:?}!", key.tx.hash(), calculate_hash(&key));
            }
        }

        verified
            .entry(key.clone())
            .or_insert(PendingItem::pending())
            .clear_error();

        FutureItem::new(key, self.verified.clone())
            .map(|res| res.map_err(|e| e.into()))
            .map(|res| res.map(|outcome| outcome.unwrap()))
    }

    #[inline]
    pub fn receive(
        &self, peer: &NodeId, id: RequestId,
        call_results: impl Iterator<Item = CallContextWithKey>,
    ) -> Result<()>
    {
        for CallContextWithKey { key, context } in call_results {
            trace!(
                "Validating call with key {:?} and context {:?}",
                // result,
                key,
                context
            );

            if self.sync_manager.contains(&key) {
                trace!("!!!!!!!!! receiving; synx manager CONTAINS tx hash = {:?}, key hash = {:?}!", key.tx.hash(), calculate_hash(&key));
            } else {
                trace!("!!!!!!!!! receiving; synx manager DOES NOT CONTAIN tx hash = {:?}, key hash = {:?}!", key.tx.hash(), calculate_hash(&key));
            }

            match self.sync_manager.check_if_requested(peer, id, &key)? {
                None => {
                    trace!("!!!!!!!!! request not found; tx hash = {:?}, key hash = {:?}!", key.tx.hash(), calculate_hash(&key));
                    continue;
                }
                Some(_) => self.validate_and_store(key, context)?,
            };
        }

        Ok(())
    }

    #[inline]
    pub fn validate_and_store(
        &self, key: CallKey, context: CallContext,
    ) -> Result<()> {
        // validate call result
        match self.execute_call(&key.tx, key.epoch, context) {
            Err(e) => {
                // forward error to both rpc caller(s) and sync handler
                // so we need to make it clonable
                let e = ClonableError::from(e);

                self.verified
                    .write()
                    .entry(key.clone())
                    .or_insert(PendingItem::pending())
                    .set_error(e.clone());

                bail!(e);
            }
            Ok(outcome) => {
                // store state entry by state key
                self.verified
                    .write()
                    .entry(key.clone())
                    .or_insert(PendingItem::pending())
                    .set(WrappedExecutionOutcome::new(outcome));

                self.sync_manager.remove_in_flight(&key);

                Ok(())
            }
        }
    }

    #[inline]
    pub fn clean_up(&self) {
        // remove timeout in-flight requests
        let timeout = *BLOOM_REQUEST_TIMEOUT; // TODO
        let calls = self.sync_manager.remove_timeout_requests(timeout);
        trace!("Timeout calls ({}): {:?}", calls.len(), calls);
        self.sync_manager.insert_waiting(calls.into_iter());

        // trigger cache cleanup
        // self.verified.write().get(&Default::default()); // TODO
    }

    #[inline]
    fn send_request(
        &self, io: &dyn NetworkContext, peer: &NodeId, keys: Vec<CallKey>,
    ) -> Result<Option<RequestId>> {
        if keys.is_empty() {
            return Ok(None);
        }

        let request_id = self.request_id_allocator.next();

        trace!(
            "send_request CallTransactions peer={:?} id={:?} keys={:?}",
            peer,
            request_id,
            keys
        );

        let msg: Box<dyn Message> =
            Box::new(GetCallContexts { request_id, keys });

        msg.send(io, peer)?;
        Ok(Some(request_id))
    }

    #[inline]
    pub fn sync(&self, io: &dyn NetworkContext) {
        self.sync_manager.sync(
            MAX_BLOOMS_IN_FLIGHT,     // TODO
            BLOOM_REQUEST_BATCH_SIZE, // TODO
            |peer, epochs| self.send_request(io, peer, epochs),
        );
    }

    #[inline]
    fn execute_call(
        &self, tx: &SignedTransaction, epoch: u64, context: CallContext,
    ) -> Result<ExecutionOutcome> {
        // validate state root
        let state_root = context.state_root;

        trace!("!!!!!!!!! validating state root");

        self.state_roots.validate_state_root(epoch, &state_root)?;
        // .chain_err(|| ErrorKind::InvalidStateProof {
        //     epoch,
        //     key: key.clone(),
        //     value: value.clone(),
        //     reason: "Validation of current state root failed",
        // })?;

        // validate previous state root
        let maybe_prev_root = context.prev_snapshot_state_root;

        trace!("!!!!!!!!! validating previous state root");

        self.state_roots
            .validate_prev_snapshot_state_root(epoch, &maybe_prev_root)?;
        // .chain_err(|| ErrorKind::InvalidStateProof {
        //     epoch,
        //     key: key.clone(),
        //     value: value.clone(),
        //     reason: "Validation of previous state root failed",
        // })?;

        // construct padding
        let maybe_intermediate_padding = maybe_prev_root.map(|root| {
            StorageKey::delta_mpt_padding(
                &root.snapshot_root,
                &root.intermediate_delta_root,
            )
        });

        let consensus = self
            .consensus
            .as_any()
            .downcast_ref::<ConsensusGraph>()
            .expect("downcast should succeed");

        let outcome = consensus
            .call_virtual_verified(
                tx,
                primitives::EpochNumber::Number(epoch),
                context.execution_proof,
                state_root,
                maybe_intermediate_padding,
            )
            .map_err(|e| e.to_string())?; // TODO

        Ok(outcome)
    }
}
