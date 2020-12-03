// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

extern crate lru_time_cache;

use lru_time_cache::LruCache;
use parking_lot::RwLock;
use std::{future::Future, sync::Arc};

use super::{
    common::{FutureItem, TimeOrdered, PendingItem, SyncManager},
    witnesses::Witnesses,
    state_roots::StateRoots,
};
use crate::{
    light_protocol::{
        common::{FullPeerState, Peers},
        error::*,
        message::{msgid, CallTransactions, CallKey, CallResultWithKey, CallResultProof},
    },
    message::{Message, RequestId},
    UniqueId,
    executive::ExecutionOutcome,
};
use cfx_parameters::light::{
    BLOOM_REQUEST_BATCH_SIZE, BLOOM_REQUEST_TIMEOUT, CACHE_TIMEOUT,
    MAX_BLOOMS_IN_FLIGHT,
};
use futures::future::FutureExt;
use network::{node_table::NodeId, NetworkContext};
use primitives::{SignedTransaction};
use primitives::StorageKey;

#[derive(Debug)]
struct Statistics {
    cached: usize,
    in_flight: usize,
    waiting: usize,
}

// TODO
pub type ExecutionOutcomePlaceholder = ();

type MissingCallResult = TimeOrdered<CallKey>;

type PendingCall = PendingItem<ExecutionOutcomePlaceholder, ClonableError>;

pub struct Calls {
    // series of unique request ids
    request_id_allocator: Arc<UniqueId>,

    // state_root sync manager
    state_roots: Arc<StateRoots>,

    // sync and request manager
    sync_manager: SyncManager<CallKey, MissingCallResult>,

    // bloom filters received from full node
    verified: Arc<RwLock<LruCache<CallKey, PendingCall>>>,

    // witness sync manager
    witnesses: Arc<Witnesses>,
}

impl Calls {
    pub fn new(
        peers: Arc<Peers<FullPeerState>>, state_roots: Arc<StateRoots>, request_id_allocator: Arc<UniqueId>,
        witnesses: Arc<Witnesses>,
    ) -> Self
    {
        let sync_manager = SyncManager::new(peers, msgid::CALL_TRANSACTIONS);

        let cache = LruCache::with_expiry_duration(*CACHE_TIMEOUT);
        let verified = Arc::new(RwLock::new(cache));

        Calls {
            request_id_allocator,
            state_roots,
            sync_manager,
            verified,
            witnesses,
        }
    }

    #[inline]
    pub fn print_stats(&self) {
        debug!(
            "call sync statistics: {:?}",
            Statistics {
                cached: self.verified.read().len(),
                in_flight: self.sync_manager.num_in_flight(),
                waiting: self.sync_manager.num_waiting(),
            }
        );
    }

    #[inline]
    pub fn request_now(
        &self, io: &dyn NetworkContext, tx: SignedTransaction, epoch: u64,
    ) -> impl Future<Output = Result<ExecutionOutcomePlaceholder>> {
        let mut verified = self.verified.write();
        let key = CallKey { tx, epoch };

        if !verified.contains_key(&key) {
            let missing = std::iter::once(MissingCallResult::new(key.clone()));

            self.sync_manager.request_now(missing, |peer, keys| {
                self.send_request(io, peer, keys)
            });
        }

        verified
            .entry(key.clone())
            .or_insert(PendingItem::pending())
            .clear_error();

        FutureItem::new(key, self.verified.clone())
            .map(|res| res.map_err(|e| e.into()))
    }

    #[inline]
    pub fn receive(
        &self, peer: &NodeId, id: RequestId,
        call_results: impl Iterator<Item = CallResultWithKey>,
    ) -> Result<()>
    {
        for CallResultWithKey { key, proof } in call_results {
            trace!(
                "Validating call with key {:?} and proof {:?}",
                // result,
                key,
                proof
            );

            match self.sync_manager.check_if_requested(peer, id, &key)? {
                None => continue,
                Some(_) => self.validate_and_store(key, proof)?,
            };
        }

        Ok(())
    }

    #[inline]
    pub fn validate_and_store(&self, key: CallKey, proof: CallResultProof) -> Result<()> {
        // validate call result
        if let Err(e) =
            self.validate_call_result(&key.tx, key.epoch, proof)
        {
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

        // store state entry by state key
        self.verified
            .write()
            .entry(key.clone())
            .or_insert(PendingItem::pending())
            .set(()); // TODO

        self.sync_manager.remove_in_flight(&key);

        Ok(())
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
            Box::new(CallTransactions { request_id, keys });

        msg.send(io, peer)?;
        Ok(Some(request_id))
    }

    #[inline]
    pub fn sync(&self, io: &dyn NetworkContext) {
        self.sync_manager.sync(
            MAX_BLOOMS_IN_FLIGHT, // TODO
            BLOOM_REQUEST_BATCH_SIZE, // TODO
            |peer, epochs| self.send_request(io, peer, epochs),
        );
    }

    #[inline]
    fn validate_call_result(
        &self, tx: &SignedTransaction, epoch: u64,
        proof: CallResultProof,
    ) -> Result<()>
    {
        // validate state root
        let state_root = proof.state_root;

        self.state_roots
            .validate_state_root(epoch, &state_root)?;
            // .chain_err(|| ErrorKind::InvalidStateProof {
            //     epoch,
            //     key: key.clone(),
            //     value: value.clone(),
            //     reason: "Validation of current state root failed",
            // })?;

        // validate previous state root
        let maybe_prev_root = proof.prev_snapshot_state_root;

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

        // TODO: re-execute on state proof
        // ...

        Ok(())
    }
}
