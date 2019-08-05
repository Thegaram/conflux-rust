// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use parking_lot::RwLock;
use rand::Rng;

use cfx_types::H256;

use crate::network::PeerId;

#[derive(Default)]
pub(super) struct PeerState {
    pub protocol_version: u8,
    pub genesis_hash: H256,
    pub best_epoch: u64,
    pub terminals: HashSet<H256>,
}

#[derive(Default)]
pub struct Peers(RwLock<HashMap<PeerId, Arc<RwLock<PeerState>>>>);

impl Peers {
    pub(super) fn insert(&self, peer: PeerId) -> Arc<RwLock<PeerState>> {
        self.0
            .write()
            .entry(peer)
            .or_insert(Arc::new(RwLock::new(PeerState::default())))
            .clone()
    }

    pub(super) fn get(&self, peer: &PeerId) -> Option<Arc<RwLock<PeerState>>> {
        self.0.read().get(&peer).map(Clone::clone)
    }

    pub(super) fn remove(&self, peer: &PeerId) { self.0.write().remove(&peer); }

    pub fn all_peers_shuffled(&self) -> Vec<PeerId> {
        let mut rand = rand::thread_rng();
        let mut peers: Vec<_> = self.0.read().keys().cloned().collect();
        rand.shuffle(&mut peers[..]);
        peers
    }

    pub fn random_peer(&self) -> Option<PeerId> {
        let mut rand = rand::thread_rng();
        let peers: Vec<_> = self.0.read().keys().cloned().collect();
        rand.choose(&peers).cloned()
    }

    pub fn random_peer_with_epoch(&self, epoch: u64) -> Option<PeerId> {
        let mut rand = rand::thread_rng();

        let peers: Vec<_> = self
            .0
            .read()
            .iter()
            .filter_map(|(peer, state)| match state.read().best_epoch {
                e if e >= epoch => Some(peer),
                _ => None,
            })
            .cloned()
            .collect();

        rand.choose(&peers).cloned()
    }

    pub fn best_epoch(&self) -> u64 {
        let mut best = 0;

        for (_, state) in self.0.read().iter() {
            let best_epoch = state.read().best_epoch;
            if best_epoch > best {
                best = best_epoch;
            }
        }

        best
    }

    pub fn median_epoch(&self) -> Option<u64> {
        let mut best_epochs: Vec<_> = self
            .0
            .read()
            .values()
            .map(|s| s.read().best_epoch)
            .collect();

        best_epochs.sort();

        match best_epochs.len() {
            0 => None,
            n => Some(best_epochs[n / 2]),
        }
    }

    pub fn collect_all_terminals(&self) -> Vec<H256> {
        self.0
            .read()
            .values()
            .map(|s| {
                let mut state = s.write();
                let ts = state.terminals.clone();
                state.terminals.clear();
                ts
            })
            .fold(vec![], |mut res, sub| {
                res.extend(sub);
                res
            })
    }
}
